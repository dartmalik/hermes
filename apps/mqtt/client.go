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

type MqttEndpoint interface {
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
	endpoint MqttEndpoint
}

type ClientEndpointClosed struct{}

type ClientConnectRecvFailed struct{}

type ClientDisconnected struct {
	ClientID MqttClientId
	Manual   bool
}

type Client struct {
	id             MqttClientId
	endpoint       MqttEndpoint
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

func (cl *Client) preConnectRecv(ctx hermes.Context, hm hermes.Message) {
	switch msg := hm.Payload().(type) {
	case *hermes.Joined:

	case *ClientEndpointOpened:
		cl.onEndpointOpened(ctx, msg)

	case *ClientEndpointClosed:
		cl.onEndpointClosed(ctx, false)

	case *MqttConnectMessage:
		cl.onConnect(ctx, msg)

	case *ClientConnectRecvFailed:
		cl.endpoint.Close()

	default:
		fmt.Printf("received invalid message: %T\n", msg)
		cl.endpoint.Close()
	}
}

func (cl *Client) postConnectRecv(ctx hermes.Context, hm hermes.Message) {
	switch msg := hm.Payload().(type) {
	case *ClientEndpointClosed:
		cl.onEndpointClosed(ctx, true)

	case *MqttPublishMessage:
		cl.onPublish(ctx, hm, msg)

	case *MqttPubAckMessage:
		cl.onPubAck(ctx, msg)

	case *MqttPubRecMessage:
		cl.onPubRec(ctx, msg)

	case *MqttPubRelMessage:
		cl.onPubRel(ctx, msg)

	case *MqttPubCompMessage:
		cl.onPubComp(ctx, msg)

	case *MqttSubscribeMessage:
		cl.onSubscribe(ctx, msg)

	case *MqttUnsubscribeMessage:
		cl.onUnsubscribe(ctx, msg)

	case *MqttDisconnectMessage:
		cl.onDisconnect()

	case *SessionMessagePublished:
		cl.onSessionMessagePublished(ctx, msg)

	default:
		fmt.Printf("received invalid message: %T\n", msg)
		cl.endpoint.Close()
	}
}

func (cl *Client) dcRecv(ctx hermes.Context, msg hermes.Message) {
	fmt.Printf("[WARN] received msg after disconnect: %T\n", msg.Payload())
}

func (cl *Client) onEndpointOpened(ctx hermes.Context, msg *ClientEndpointOpened) {
	cl.endpoint = msg.endpoint

	t, err := ctx.Schedule(ClientConnectTimeout, &ClientConnectRecvFailed{})
	if err != nil {
		cl.endpoint.Write(&MqttDisconnectMessage{})
		cl.endpoint.Close()
	}
	cl.conTimeout = t
}

func (cl *Client) onEndpointClosed(ctx hermes.Context, unregister bool) {
	fmt.Printf("[INFO] closing client\n")

	ctx.SetReceiver(cl.dcRecv)

	if unregister {
		cl.unregister(ctx, cl.id)
	}

	Emit(ctx, EvClientDisconnected, &ClientDisconnected{ClientID: cl.id, Manual: cl.manualDC})
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

func (cl *Client) onSubscribe(ctx hermes.Context, msg *MqttSubscribeMessage) {
	ack, err := SessionSubscribe(ctx, cl.sid(), msg)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(ack)
	}
}

func (cl *Client) onUnsubscribe(ctx hermes.Context, msg *MqttUnsubscribeMessage) {
	ack, err := SessionUnsubscribe(ctx, cl.sid(), msg)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(ack)
	}
}

func (cl *Client) onPublish(ctx hermes.Context, msg hermes.Message, pub *MqttPublishMessage) {
	err := SessionPublish(ctx, cl.sid(), pub)
	if err != nil {
		cl.endpoint.Close()
	} else if pub.QosLevel == MqttQoSLevel1 {
		cl.endpoint.Write(&MqttPubAckMessage{PacketId: pub.PacketId})
	} else if pub.QosLevel == MqttQoSLevel2 {
		cl.endpoint.Write(&MqttPubRecMessage{PacketId: pub.PacketId})
	}
}

func (cl *Client) onPubAck(ctx hermes.Context, ack *MqttPubAckMessage) {
	err := SessionPubAck(ctx, cl.sid(), ack.PacketId)
	if err != nil {
		cl.endpoint.Close()
	}
}

func (cl *Client) onPubRec(ctx hermes.Context, msg *MqttPubRecMessage) {
	err := SessionPubRec(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(&MqttPubRelMessage{PacketId: msg.PacketId})
	}
}

func (cl *Client) onPubComp(ctx hermes.Context, msg *MqttPubCompMessage) {
	err := SessionPubComp(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	}
}

func (cl *Client) onPubRel(ctx hermes.Context, msg *MqttPubRelMessage) {
	err := SessionPubRel(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(&MqttPubCompMessage{PacketId: msg.PacketId})
	}
}

func (cl *Client) onSessionMessagePublished(ctx hermes.Context, msg *SessionMessagePublished) {
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
}

func (cl *Client) onDisconnect() {
	cl.manualDC = true
	cl.endpoint.Close()
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
