package mqtt

import (
	"fmt"
	"time"

	"github.com/dartali/hermes"
)

func IsLWTID(rid hermes.ReceiverID) bool {
	return rid == LWTID()
}

func LWTID() hermes.ReceiverID {
	return hermes.ReceiverID("/ext/lwt")
}

type LWTJoinRequest struct{}
type LWTJoinReply struct {
	Err error
}

type LWT struct {
	msgs map[MqttClientId]*MqttPublishMessage
}

func NewLWTRecv() hermes.Receiver {
	return NewLWT().recv
}

func NewLWT() *LWT {
	return &LWT{msgs: make(map[MqttClientId]*MqttPublishMessage)}
}

func (lwt *LWT) recv(ctx hermes.Context, hm hermes.Message) {
	switch msg := hm.Payload().(type) {
	case *hermes.Joined:

	case *LWTJoinRequest:
		err := lwt.onJoin(ctx)
		ctx.Reply(hm, &LWTJoinReply{Err: err})

	case *MqttConnectMessage:
		lwt.onConnect(msg)

	case *ClientDisconnected:
		lwt.onDisconnect(ctx, msg)
	}
}

func (lwt *LWT) onJoin(ctx hermes.Context) error {
	return Join(ctx, []EventID{"client.connected", "client.disconnected"}, ctx.ID())
}

func (lwt *LWT) onConnect(msg *MqttConnectMessage) {
	if !msg.HasWill() {
		return
	}

	if msg.clientId == "" {
		fmt.Printf("[ERROR] received connect with invalid client ID")
		return
	}

	if msg.willMsg == nil {
		fmt.Printf("[ERROR] received connect with invalid will message")
		return
	}

	if msg.willTopic == "" {
		fmt.Printf("[ERROR] received connect with invalid will topic")
		return
	}

	lwt.msgs[msg.clientId] = &MqttPublishMessage{
		TopicName: msg.willTopic,
		Payload:   msg.willMsg,
		QosLevel:  msg.WillQoS(),
		Retain:    msg.HasWillRetain(),
	}
}

func (lwt *LWT) onDisconnect(ctx hermes.Context, ev *ClientDisconnected) {
	cid := ev.ClientID
	manual := ev.Manual

	if manual {
		return
	}
	if cid == "" {
		fmt.Printf("[WARN] received invalid client id in disconnect message")
		return
	}

	msg, ok := lwt.msgs[cid]
	if !ok {
		return
	}

	defer delete(lwt.msgs, cid)

	r, err := ctx.RequestWithTimeout(PubSubID(), &PubSubPublishRequest{msg: msg}, 1500*time.Millisecond)
	if err != nil {
		fmt.Printf("[ERROR] failed to publish lwt msg: %s\n", err.Error())
		return
	}

	rep := r.Payload().(*PubSubPublishReply)
	if rep.err != nil {
		fmt.Printf("[ERROR] failed to publish lwt msg: %s\n", rep.err.Error())
	}
}
