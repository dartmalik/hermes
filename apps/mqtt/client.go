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
	ClientID ClientId
	Manual   bool
}

type Client struct {
	id             ClientId
	endpoint       MqttEndpoint
	conTimeout     hermes.Timer
	keepaliveTimer hermes.Timer
	manualDC       bool
}

func NewClientRecv() hermes.Receiver {
	return newClient().preConnectRecv
}

func newClient() *Client {
	return &Client{}
}

func (cl *Client) preConnectRecv(ctx hermes.Context, hm hermes.Message) {
	switch msg := hm.Payload().(type) {
	case *hermes.Joined:

	case *ClientEndpointOpened:
		cl.onEndpointOpened(ctx, msg)

	case *ClientEndpointClosed:
		cl.onEndpointClosed(ctx, false)

	case *ConnectMessage:
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

	case *PublishMessage:
		cl.onPublish(ctx, hm, msg)

	case *PubAckMessage:
		cl.onPubAck(ctx, msg)

	case *PubRecMessage:
		cl.onPubRec(ctx, msg)

	case *PubRelMessage:
		cl.onPubRel(ctx, msg)

	case *PubCompMessage:
		cl.onPubComp(ctx, msg)

	case *SubscribeMessage:
		cl.onSubscribe(ctx, msg)

	case *UnsubscribeMessage:
		cl.onUnsubscribe(ctx, msg)

	case *DisconnectMessage:
		cl.onDisconnect()

	case *SessionMessagePublished:
		cl.onSessionMessagePublished(ctx, msg)

	case *SessionUnregistered:
		cl.endpoint.Close()

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
		cl.endpoint.Write(&DisconnectMessage{})
		cl.endpoint.Close()
	}
	cl.conTimeout = t
}

func (cl *Client) onEndpointClosed(ctx hermes.Context, unregister bool) {
	ctx.SetReceiver(cl.dcRecv)

	if unregister {
		cl.unregister(ctx, cl.id)
	}

	Emit(ctx, EvClientDisconnected, &ClientDisconnected{ClientID: cl.id, Manual: cl.manualDC})
}

func (cl *Client) onConnect(ctx hermes.Context, msg *ConnectMessage) {
	cl.conTimeout.Stop()
	cl.conTimeout = nil

	// [MQTT-3.1.0-2], [MQTT-3.1.2-1], [MQTT-3.1.2-2]
	if msg.Protocol != "MQTT" {
		cl.endpoint.Close()
		return
	}

	if msg.ProtocolLevel != 4 {
		cl.endpoint.WriteAndClose(&ConnAckMessage{code: ConnackCodeInvalidProtocolVer})
		return
	}

	// [MQTT-3.1.2-3]
	if msg.Reserved() {
		cl.endpoint.Close()
		return
	}

	// [MQTT-3.1.2-4]
	sessionPresent, err := cl.register(ctx, msg.ClientId)
	if err != nil {
		cl.endpoint.WriteAndClose(&ConnAckMessage{code: ConnackCodeUnavailable})
		return
	}

	// [MQTT-3.1.2-6]
	if msg.HasCleanSession() {
		err := cl.clean(ctx, msg.ClientId)
		if err != nil {
			cl.endpoint.WriteAndClose(&ConnAckMessage{code: ConnackCodeUnavailable})
			return
		}
		sessionPresent = false
	}

	if msg.KeepAlive > 0 {
		ka := time.Duration(msg.KeepAlive) * time.Second
		if ka < ClientDefaultKeepalive {
			cl.keepaliveTimer, err = ctx.Schedule(ka, &MqttKeepAliveTimeout{})
			if err != nil {
				cl.endpoint.WriteAndClose(&ConnAckMessage{code: ConnackCodeUnavailable})
				return
			}
		}
	}

	cl.id = msg.ClientId

	// [MQTT-3.2.2-1], [MQTT-3.2.2-2], [MQTT-3.2.2-3], [MQTT-3.2.2-4]
	ack := &ConnAckMessage{code: ConnackCodeAccepted}
	ack.SetSessionPresent(sessionPresent)
	cl.endpoint.Write(ack)

	Emit(ctx, EvClientConnected, msg)

	ctx.SetReceiver(cl.postConnectRecv)
}

func (cl *Client) onSubscribe(ctx hermes.Context, msg *SubscribeMessage) {
	ack, err := SessionSubscribe(ctx, cl.sid(), msg)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(ack)
	}
}

func (cl *Client) onUnsubscribe(ctx hermes.Context, msg *UnsubscribeMessage) {
	ack, err := SessionUnsubscribe(ctx, cl.sid(), msg)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(ack)
	}
}

func (cl *Client) onPublish(ctx hermes.Context, msg hermes.Message, pub *PublishMessage) {
	err := SessionPublish(ctx, cl.sid(), pub)
	if err != nil {
		cl.endpoint.Close()
	} else if pub.QosLevel == QoSLevel1 {
		cl.endpoint.Write(&PubAckMessage{PacketId: pub.PacketId})
	} else if pub.QosLevel == QoSLevel2 {
		cl.endpoint.Write(&PubRecMessage{PacketId: pub.PacketId})
	}
}

func (cl *Client) onPubAck(ctx hermes.Context, ack *PubAckMessage) {
	err := SessionPubAck(ctx, cl.sid(), ack.PacketId)
	if err != nil {
		cl.endpoint.Close()
	}
}

func (cl *Client) onPubRec(ctx hermes.Context, msg *PubRecMessage) {
	err := SessionPubRec(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(&PubRelMessage{PacketId: msg.PacketId})
	}
}

func (cl *Client) onPubComp(ctx hermes.Context, msg *PubCompMessage) {
	err := SessionPubComp(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	}
}

func (cl *Client) onPubRel(ctx hermes.Context, msg *PubRelMessage) {
	err := SessionPubRel(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(&PubCompMessage{PacketId: msg.PacketId})
	}
}

func (cl *Client) onSessionMessagePublished(ctx hermes.Context, msg *SessionMessagePublished) {
	if msg.Msg.State() == SMStatePublished {
		pub := cl.toPub(msg.Msg)
		cl.endpoint.Write(pub)
		if msg.Msg.QoS() == QoSLevel0 {
			ctx.Send(sessionID(cl.id), &PubAckMessage{PacketId: msg.Msg.ID()})
		}
	} else if msg.Msg.State() == SMStateAcked {
		if msg.Msg.QoS() == QoSLevel2 {
			cl.endpoint.Write(&PubRelMessage{PacketId: msg.Msg.ID()})
		} else {
			cl.endpoint.Close()
		}
	}
}

func (cl *Client) onDisconnect() {
	cl.manualDC = true
	cl.endpoint.Close()
}

func (cl *Client) register(ctx hermes.Context, id ClientId) (bool, error) {
	sid := sessionID(id)
	res, err := ctx.RequestWithTimeout(sid, &SessionRegisterRequest{ConsumerID: ctx.ID()}, ClientRequestTimeout)
	if err != nil {
		return false, err
	}

	srr := res.Payload().(*SessionRegisterReply)

	return srr.present, srr.Err
}

func (cl *Client) unregister(ctx hermes.Context, id ClientId) error {
	sid := sessionID(id)
	res, err := ctx.RequestWithTimeout(sid, &SessionUnregisterRequest{ConsumerID: ctx.ID()}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	srr := res.Payload().(*SessionUnregisterReply)

	return srr.Err
}

func (cl *Client) clean(ctx hermes.Context, id ClientId) error {
	_, err := ctx.RequestWithTimeout(cl.sid(), &SessionCleanRequest{}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	return nil
}

func (cl *Client) sid() hermes.ReceiverID {
	return sessionID(cl.id)
}

func (cl *Client) toPub(sm SessionMessage) *PublishMessage {
	return &PublishMessage{
		TopicName: sm.Topic(),
		Payload:   sm.Payload(),
		PacketId:  sm.ID(),
		Duplicate: sm.SentCount() > 1,
		QosLevel:  sm.QoS(),
		Retain:    false,
	}
}
