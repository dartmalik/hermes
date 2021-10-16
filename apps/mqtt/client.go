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

func IsClientID(id hermes.ReceiverID) bool {
	return strings.HasPrefix(string(id), ClientIDPrefix)
}

func clientID(id string) hermes.ReceiverID {
	return hermes.ReceiverID(ClientIDPrefix + id)
}

type MqttKeepAliveTimeout struct{}

const (
	ClientConnectTimeout   = 5 * time.Second
	ClientDefaultKeepalive = 10 * time.Second
	EvClientConnected      = "client.connected"
	EvClientDisconnected   = "client.disconnected"
)

type ClientEndpointOpened struct {
	endpoint Endpoint
}

type ClientEndpointClosed struct{}

type ClientConnectRecvFailed struct{}

type ClientDisconnected struct {
	ClientID MqttClientId
	Manual   bool
}

type Client struct {
	id             MqttClientId
	endpoint       Endpoint
	conTimeout     hermes.Timer
	keepaliveTimer hermes.Timer
	manualDC       bool
}

func NewClientRecv() hermes.Receiver {
	return NewClient().preConnectRecv
}

func NewClient() *Client {
	return &Client{}
}

func (cl *Client) preConnectRecv(ctx hermes.Context, msg hermes.Message) {
	switch msg.Payload().(type) {
	case *hermes.Joined:

	case *ClientEndpointOpened:
		cl.onEndpoint(ctx, msg.Payload().(*ClientEndpointOpened))

	case *ClientEndpointClosed:
		cl.onEndpointClosed(ctx, false)

	case *MqttConnectMessage:
		cl.onConnect(ctx, msg.Payload().(*MqttConnectMessage))

	case *ClientConnectRecvFailed:
		cl.endpoint.Close()

	default:
		cl.endpoint.Close()
	}
}

func (cl *Client) postConnectRecv(ctx hermes.Context, hm hermes.Message) {
	switch msg := hm.Payload().(type) {
	case *ClientEndpointClosed:
		cl.onEndpointClosed(ctx, true)

	case *MqttPublishMessage:
		err := cl.onPublish(ctx, msg)
		if err != nil {
			cl.endpoint.Close()
		} else if msg.QosLevel == MqttQoSLevel1 {
			cl.endpoint.Write(&MqttPubAckMessage{PacketId: msg.PacketId})
		} else if msg.QosLevel == MqttQoSLevel2 {
			cl.endpoint.Write(&MqttPubRecMessage{PacketId: msg.PacketId})
		}

	case *MqttPubAckMessage:
		err := cl.onPubAck(ctx, msg.PacketId)
		if err != nil {
			cl.endpoint.Close()
		}

	case *MqttPubRecMessage:
		err := cl.onPubRec(ctx, msg.PacketId)
		if err != nil {
			cl.endpoint.Close()
		} else {
			cl.endpoint.Write(&MqttPubRelMessage{PacketId: msg.PacketId})
		}

	case *MqttPubCompMessage:
		err := cl.onPubComp(ctx, msg.PacketId)
		if err != nil {
			cl.endpoint.Close()
		}

	case *MqttPubRelMessage:
		err := cl.onPubRel(ctx, msg)
		if err != nil {
			cl.endpoint.Close()
		} else {
			cl.endpoint.Write(&MqttPubCompMessage{PacketId: msg.PacketId})
		}

	case *MqttSubscribeMessage:
		ack, err := cl.onSubscribe(ctx, msg)
		if err != nil {
			cl.endpoint.Close()
		} else {
			cl.endpoint.Write(ack)
		}

	case *MqttUnsubscribeMessage:
		ack, err := cl.onUnsubscribe(ctx, msg)
		if err != nil {
			cl.endpoint.Close()
		} else {
			cl.endpoint.Write(ack)
		}

	case *MqttDisconnectMessage:
		cl.onDisconnect()

	case *SessionMessagePublished:
		if msg.Msg.State() == SMStatePublished {
			pub := cl.toPub(msg.Msg)
			cl.endpoint.Write(pub)
			if msg.Msg.QoS() == MqttQoSLevel0 {
				ctx.Send(sessionID(cl.id), &MqttPubAckMessage{PacketId: msg.Msg.ID()})
			}
		} else if msg.Msg.State() == SMStateAcked {
			if msg.Msg.QoS() == MqttQoSLevel2 {
				cl.endpoint.Write(&MqttPubRelMessage{PacketId: msg.Msg.ID()})
			} else {
				cl.endpoint.Close()
			}
		}

	default:
		fmt.Printf("received invalid message: %T\n", msg)
		cl.endpoint.Close()
	}
}

func (cl *Client) dcRecv(ctx hermes.Context, msg hermes.Message) {
	fmt.Printf("[WARN] received msg after disconnect: %T\n", msg.Payload())
}

func (cl *Client) onDisconnect() {
	cl.manualDC = true
	cl.endpoint.Close()
}

func (cl *Client) onEndpoint(ctx hermes.Context, msg *ClientEndpointOpened) {
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

	Emit(ctx, EvClientConnected, msg)

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

func (cl *Client) onEndpointClosed(ctx hermes.Context, unregister bool) {
	fmt.Printf("[INFO] closing client\n")

	ctx.SetReceiver(cl.dcRecv)

	if unregister {
		cl.unregister(ctx, cl.id)
	}

	Emit(ctx, EvClientDisconnected, &ClientDisconnected{ClientID: cl.id, Manual: cl.manualDC})
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

func (cl *Client) unregister(ctx hermes.Context, id MqttClientId) error {
	sid := sessionID(id)
	res, err := ctx.RequestWithTimeout(sid, &SessionUnregisterRequest{ConsumerID: ctx.ID()}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	srr := res.Payload().(*SessionUnregisterReply)

	return srr.Err
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
