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

// MqttEndpoint represents the server endpoint of an MQTT connection. This endpoint
// should have an encoder attached that encodes the passed MQTT messages into binary
// which then should be sent to the client.
type MqttEndpoint interface {
	Write(msg interface{})
	WriteAndClose(msg interface{})
	Close()
}

// IsClientID checks if the passed in ID represents a client
func IsClientID(id hermes.ReceiverID) bool {
	return strings.HasPrefix(string(id), ClientIDPrefix)
}

// generates a Client receiver ID from the specified id
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

type client struct {
	id             ClientId
	endpoint       MqttEndpoint
	conTimeout     hermes.Timer
	keepaliveTimer hermes.Timer
	manualDC       bool
}

// NewClientRecv creates an MQTT client receiver. There should be one receiver per
// connection. The client manages the lifecycle of a connection by transitioning
// between three stages:
// 1. pre-connect: after an (socket) endpoint is created and before receiving CONNECT packet
// 2. post-connect: after receving a CONNECT packet
// 3. dc: after the endpoint disconnects
//
// pre-connect:
// After a connection is established, the pre-connect state waits for a CONNECT packet. And
// if the packet is not received the connection (endpoint) is closed pre the MQTT spec.
// If a valid CONNECT packet is received, the client will registered itself with a Session
// and transition to the post-connect state
//
// post-connect:
// After a successful connect, all MQTT packets (except CONNECt) are processed.
//
// dc:
// Anytime the endpoint is closed the Client transitions to the dc state. Here no messages are
// processed and all received messages are logged as warning. No state transitions take place from
// this state.
func NewClientRecv() hermes.Receiver {
	return newClient().preConnectRecv
}

func newClient() *client {
	return &client{}
}

func (cl *client) preConnectRecv(ctx hermes.Context, hm hermes.Message) {
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

func (cl *client) postConnectRecv(ctx hermes.Context, hm hermes.Message) {
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

func (cl *client) dcRecv(ctx hermes.Context, msg hermes.Message) {
	fmt.Printf("[WARN] received msg after disconnect: %T\n", msg.Payload())
}

func (cl *client) onEndpointOpened(ctx hermes.Context, msg *ClientEndpointOpened) {
	cl.endpoint = msg.endpoint

	t, err := ctx.Schedule(ClientConnectTimeout, &ClientConnectRecvFailed{})
	if err != nil {
		cl.endpoint.Write(&DisconnectMessage{})
		cl.endpoint.Close()
	}
	cl.conTimeout = t
}

func (cl *client) onEndpointClosed(ctx hermes.Context, unregister bool) {
	ctx.SetReceiver(cl.dcRecv)

	if unregister {
		cl.unregister(ctx, cl.id)
	}

	Emit(ctx, EvClientDisconnected, &ClientDisconnected{ClientID: cl.id, Manual: cl.manualDC})
}

func (cl *client) onConnect(ctx hermes.Context, msg *ConnectMessage) {
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

func (cl *client) onSubscribe(ctx hermes.Context, msg *SubscribeMessage) {
	ack, err := SessionSubscribe(ctx, cl.sid(), msg)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(ack)
	}
}

func (cl *client) onUnsubscribe(ctx hermes.Context, msg *UnsubscribeMessage) {
	ack, err := SessionUnsubscribe(ctx, cl.sid(), msg)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(ack)
	}
}

func (cl *client) onPublish(ctx hermes.Context, msg hermes.Message, pub *PublishMessage) {
	err := SessionPublish(ctx, cl.sid(), pub)
	if err != nil {
		cl.endpoint.Close()
	} else if pub.QosLevel == QoSLevel1 {
		cl.endpoint.Write(&PubAckMessage{PacketId: pub.PacketId})
	} else if pub.QosLevel == QoSLevel2 {
		cl.endpoint.Write(&PubRecMessage{PacketId: pub.PacketId})
	}
}

func (cl *client) onPubAck(ctx hermes.Context, ack *PubAckMessage) {
	err := SessionPubAck(ctx, cl.sid(), ack.PacketId)
	if err != nil {
		cl.endpoint.Close()
	}
}

func (cl *client) onPubRec(ctx hermes.Context, msg *PubRecMessage) {
	err := SessionPubRec(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(&PubRelMessage{PacketId: msg.PacketId})
	}
}

func (cl *client) onPubComp(ctx hermes.Context, msg *PubCompMessage) {
	err := SessionPubComp(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	}
}

func (cl *client) onPubRel(ctx hermes.Context, msg *PubRelMessage) {
	err := SessionPubRel(ctx, cl.sid(), msg.PacketId)
	if err != nil {
		cl.endpoint.Close()
	} else {
		cl.endpoint.Write(&PubCompMessage{PacketId: msg.PacketId})
	}
}

func (cl *client) onSessionMessagePublished(ctx hermes.Context, msg *SessionMessagePublished) {
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

func (cl *client) onDisconnect() {
	cl.manualDC = true
	cl.endpoint.Close()
}

func (cl *client) register(ctx hermes.Context, id ClientId) (bool, error) {
	sid := sessionID(id)
	res, err := ctx.RequestWithTimeout(sid, &SessionRegisterRequest{ConsumerID: ctx.ID()}, ClientRequestTimeout)
	if err != nil {
		return false, err
	}

	srr := res.Payload().(*SessionRegisterReply)

	return srr.present, srr.Err
}

func (cl *client) unregister(ctx hermes.Context, id ClientId) error {
	sid := sessionID(id)
	res, err := ctx.RequestWithTimeout(sid, &SessionUnregisterRequest{ConsumerID: ctx.ID()}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	srr := res.Payload().(*SessionUnregisterReply)

	return srr.Err
}

func (cl *client) clean(ctx hermes.Context, id ClientId) error {
	_, err := ctx.RequestWithTimeout(cl.sid(), &SessionCleanRequest{}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	return nil
}

func (cl *client) sid() hermes.ReceiverID {
	return sessionID(cl.id)
}

func (cl *client) toPub(sm SessionMessage) *PublishMessage {
	return &PublishMessage{
		TopicName: sm.Topic(),
		Payload:   sm.Payload(),
		PacketId:  sm.ID(),
		Duplicate: sm.SentCount() > 1,
		QosLevel:  sm.QoS(),
		Retain:    false,
	}
}
