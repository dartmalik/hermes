package mqtt

import (
	"fmt"
	"strings"
	"time"

	"github.com/dartali/hermes"
)

const (
	ClientIDPrefix       = "/clients/"
	ClientRequestTimeout = 1500 * time.Millisecond
)

type Endpoint interface {
	Write(msg interface{})
	WriteAndClose(msg interface{})
	Close()
}

func isClientID(id hermes.ReceiverID) bool {
	return strings.HasPrefix(string(id), ClientIDPrefix)
}

func clientID(id MqttClientId) hermes.ReceiverID {
	return hermes.ReceiverID(ClientIDPrefix + string(id))
}

type MqttKeepAliveTimeout struct{}

const (
	ClientConnectTimeout   = 5 * time.Second
	ClientDefaultKeepalive = 10 * time.Second
)

type ClientEndpointCreated struct {
	endpoint Endpoint
}

type ClientConnectRecvFailed struct{}

type Client struct {
	id             MqttClientId
	endpoint       Endpoint
	conTimeout     hermes.Timer
	keepaliveTimer hermes.Timer
}

func newClient() *Client {
	return &Client{}
}

func (cl *Client) preConnectRecv(ctx hermes.Context, msg hermes.Message) {
	switch msg.Payload().(type) {
	case *hermes.Joined:

	case *ClientEndpointCreated:
		cl.onEndpoint(ctx, msg.Payload().(*ClientEndpointCreated))

	case *MqttConnectMessage:
		cl.onConnect(ctx, msg.Payload().(*MqttConnectMessage))

	case *ClientConnectRecvFailed:
		cl.endpoint.Close()

	default:
		cl.endpoint.Close()
	}
}

func (cl *Client) postConnectRecv(ctx hermes.Context, msg hermes.Message) {
	switch t := msg.Payload().(type) {
	case *MqttPublishMessage:
		pub := msg.Payload().(*MqttPublishMessage)
		err := cl.onPublish(ctx, pub)
		if err != nil {
			cl.endpoint.Close()
		} else if pub.QosLevel == MqttQoSLevel1 {
			cl.endpoint.Write(&MqttPubAckMessage{PacketId: pub.PacketId})
		} else if pub.QosLevel == MqttQoSLevel2 {
			cl.endpoint.Write(&MqttPubRecMessage{PacketId: pub.PacketId})
		}

	case *MqttPubAckMessage:
		pa := msg.Payload().(*MqttPubAckMessage)
		err := cl.onPubAck(ctx, pa.PacketId)
		if err != nil {
			cl.endpoint.Close()
		}

	case *MqttPubRecMessage:
		pr := msg.Payload().(*MqttPubRecMessage)
		err := cl.onPubRec(ctx, pr.PacketId)
		if err != nil {
			cl.endpoint.Close()
		} else {
			cl.endpoint.Write(&MqttPubRelMessage{PacketId: pr.PacketId})
		}

	case *MqttPubCompMessage:
		pc := msg.Payload().(*MqttPubCompMessage)
		err := cl.onPubComp(ctx, pc.PacketId)
		if err != nil {
			cl.endpoint.Close()
		}

	case *MqttPubRelMessage:
		prel := msg.Payload().(*MqttPubRelMessage)
		err := cl.onPubRel(ctx, prel)
		if err != nil {
			cl.endpoint.Close()
		} else {
			cl.endpoint.Write(&MqttPubCompMessage{PacketId: prel.PacketId})
		}

	case *MqttSubscribeMessage:
		ack, err := cl.onSubscribe(ctx, msg.Payload().(*MqttSubscribeMessage))
		if err != nil {
			cl.endpoint.Close()
		} else {
			cl.endpoint.Write(ack)
		}

	case *MqttUnsubscribeMessage:
		ack, err := cl.onUnsubscribe(ctx, msg.Payload().(*MqttUnsubscribeMessage))
		if err != nil {
			cl.endpoint.Close()
		} else {
			cl.endpoint.Write(ack)
		}

	case *MqttDisconnectMessage:

	case *SessionMessagePublished:
		smp := msg.Payload().(*SessionMessagePublished)
		if smp.Msg.State() == SMStatePublished {
			pub := cl.toPub(smp.Msg)
			cl.endpoint.Write(pub)
			if smp.Msg.QoS() == MqttQoSLevel0 {
				ctx.Send(sessionID(cl.id), &MqttPubAckMessage{PacketId: smp.Msg.ID()})
			}
		} else if smp.Msg.State() == SMStateAcked {
			if smp.Msg.QoS() == MqttQoSLevel2 {
				cl.endpoint.Write(&MqttPubRelMessage{PacketId: smp.Msg.ID()})
			} else {
				cl.endpoint.Close()
			}
		}

	default:
		fmt.Printf("received invalid message: %T", t)
		cl.endpoint.Close()
	}
}

func (cl *Client) onEndpoint(ctx hermes.Context, msg *ClientEndpointCreated) {
	cl.endpoint = msg.endpoint

	t, err := ctx.Schedule(ClientConnectTimeout, &ClientConnectRecvFailed{})
	if err != nil {
		cl.endpoint.Write(&MqttDisconnectMessage{})
		cl.endpoint.Close()
	}
	cl.conTimeout = t
}

func (cl *Client) onConnect(ctx hermes.Context, msg *MqttConnectMessage) {
	cl.conTimeout.Stop()
	cl.conTimeout = nil

	// [MQTT-3.1.0-2], [MQTT-3.1.2-1], [MQTT-3.1.2-2]
	if msg.protocol != "MQTT" {
		cl.endpoint.Close()
		return
	}

	if msg.protocolLevel != 4 {
		cl.endpoint.WriteAndClose(&MqttConnAckMessage{code: MqttConnackCodeInvalidProtocolVer})
		return
	}

	// [MQTT-3.1.2-3]
	if msg.Reserved() {
		cl.endpoint.Close()
		return
	}

	// [MQTT-3.1.2-4]
	sessionPresent, err := cl.register(ctx, msg.clientId)
	if err != nil {
		cl.endpoint.WriteAndClose(&MqttConnAckMessage{code: MqttConnackCodeUnavailable})
		return
	}

	// [MQTT-3.1.2-6]
	if msg.HasCleanSession() {
		err := cl.clean(ctx, msg.clientId)
		if err != nil {
			cl.endpoint.WriteAndClose(&MqttConnAckMessage{code: MqttConnackCodeUnavailable})
			return
		}
		sessionPresent = false
	}

	if msg.keepAlive > 0 {
		ka := time.Duration(msg.keepAlive) * time.Second
		if ka < ClientDefaultKeepalive {
			cl.keepaliveTimer, err = ctx.Schedule(ka, &MqttKeepAliveTimeout{})
			if err != nil {
				cl.endpoint.WriteAndClose(&MqttConnAckMessage{code: MqttConnackCodeUnavailable})
				return
			}
		}
	}

	cl.id = msg.clientId

	// [MQTT-3.2.2-1], [MQTT-3.2.2-2], [MQTT-3.2.2-3], [MQTT-3.2.2-4]
	ack := &MqttConnAckMessage{code: MqttConnackCodeAccepted}
	ack.SetSessionPresent(sessionPresent)
	cl.endpoint.Write(ack)

	ctx.SetReceiver(cl.postConnectRecv)
}

func (cl *Client) onSubscribe(ctx hermes.Context, msg *MqttSubscribeMessage) (*MqttSubAckMessage, error) {
	r, err := ctx.RequestWithTimeout(cl.sid(), msg, ClientRequestTimeout)
	if err != nil {
		return nil, err
	}

	rep := r.Payload().(*SessionSubscribeReply)
	if rep.Err != nil {
		return nil, rep.Err
	}

	return rep.Ack, nil
}

func (cl *Client) onUnsubscribe(ctx hermes.Context, msg *MqttUnsubscribeMessage) (*MqttUnSubAckMessage, error) {
	r, err := ctx.RequestWithTimeout(cl.sid(), msg, ClientRequestTimeout)
	if err != nil {
		return nil, err
	}

	rep := r.Payload().(*SessionUnsubscribeReply)
	if rep.Err != nil {
		return nil, rep.Err
	}

	return rep.Ack, nil
}

func (cl *Client) onPublish(ctx hermes.Context, msg *MqttPublishMessage) error {
	r, err := ctx.RequestWithTimeout(cl.sid(), &SessionPublishRequest{Msg: msg}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPublishReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func (cl *Client) onPubAck(ctx hermes.Context, pid MqttPacketId) error {
	r, err := ctx.RequestWithTimeout(cl.sid(), &SessionPubAckRequest{PacketID: pid}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPubAckReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func (cl *Client) onPubRec(ctx hermes.Context, pid MqttPacketId) error {
	r, err := ctx.RequestWithTimeout(cl.sid(), &SessionPubRecRequest{PacketID: pid}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPubRecReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func (cl *Client) onPubComp(ctx hermes.Context, pid MqttPacketId) error {
	r, err := ctx.RequestWithTimeout(cl.sid(), &SessionPubCompRequest{PacketID: pid}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPubCompReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func (cl *Client) onPubRel(ctx hermes.Context, msg *MqttPubRelMessage) error {
	r, err := ctx.RequestWithTimeout(cl.sid(), &SessionPubRelRequest{PacketID: msg.PacketId}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPubRelReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func (cl *Client) register(ctx hermes.Context, id MqttClientId) (bool, error) {
	sid := sessionID(id)
	res, err := ctx.RequestWithTimeout(sid, &SessionRegisterRequest{ConsumerID: ctx.ID()}, ClientRequestTimeout)
	if err != nil {
		return false, err
	}

	srr := res.Payload().(*SessionRegisterReply)

	return srr.present, srr.Err
}

func (cl *Client) clean(ctx hermes.Context, id MqttClientId) error {
	_, err := ctx.RequestWithTimeout(cl.sid(), &SessionCleanRequest{}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	return nil
}

func (cl *Client) sid() hermes.ReceiverID {
	return sessionID(cl.id)
}

func (cl *Client) toPub(sm SessionMessage) *MqttPublishMessage {
	return &MqttPublishMessage{
		TopicName: sm.Topic(),
		Payload:   sm.Payload(),
		PacketId:  sm.ID(),
		Duplicate: sm.SentCount() > 1,
		QosLevel:  sm.QoS(),
		Retain:    false,
	}
}
