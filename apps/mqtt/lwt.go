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
	msgs map[ClientId]*PublishMessage
}

func NewLWTRecv() hermes.Receiver {
	return NewLWT().recv
}

func NewLWT() *LWT {
	return &LWT{msgs: make(map[ClientId]*PublishMessage)}
}

func (lwt *LWT) recv(ctx hermes.Context, hm hermes.Message) {
	switch msg := hm.Payload().(type) {
	case *hermes.Joined:

	case *LWTJoinRequest:
		err := lwt.onJoin(ctx)
		ctx.Reply(hm, &LWTJoinReply{Err: err})

	case *ConnectMessage:
		lwt.onConnect(msg)

	case *ClientDisconnected:
		lwt.onDisconnect(ctx, msg)
	}
}

func (lwt *LWT) onJoin(ctx hermes.Context) error {
	return Join(ctx, []EventID{"client.connected", "client.disconnected"}, ctx.ID())
}

func (lwt *LWT) onConnect(msg *ConnectMessage) {
	if !msg.HasWill() {
		return
	}

	if msg.ClientId == "" {
		fmt.Printf("[ERROR] received connect with invalid client ID")
		return
	}

	if msg.WillMsg == nil {
		fmt.Printf("[ERROR] received connect with invalid will message")
		return
	}

	if msg.WillTopic == "" {
		fmt.Printf("[ERROR] received connect with invalid will topic")
		return
	}

	lwt.msgs[msg.ClientId] = &PublishMessage{
		TopicName: msg.WillTopic,
		Payload:   msg.WillMsg,
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

	r, err := ctx.RequestWithTimeout(PubSubID(), &PubSubPublishRequest{Msg: msg}, 1500*time.Millisecond)
	if err != nil {
		fmt.Printf("[ERROR] failed to publish lwt msg: %s\n", err.Error())
		return
	}

	rep := r.Payload().(*PubSubPublishReply)
	if rep.Err != nil {
		fmt.Printf("[ERROR] failed to publish lwt msg: %s\n", rep.Err.Error())
	}
}
