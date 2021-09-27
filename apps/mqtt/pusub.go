package mqtt

import (
	"errors"
	"fmt"
	"time"

	"github.com/dartali/hermes"
)

const (
	PollTimeoutDur = 1 * time.Second
)

type PollResult struct {
	Msgs   []*MqttPublishMessage
	Offset []byte
}

type MsgStore interface {
	Put(msg *MqttPublishMessage) error
	Poll(offset []byte) (*PollResult, error)
}

type InMemMsgStore struct {
	msgs *hermes.Queue
}

func NewInMemMsgStore() *InMemMsgStore {
	return &InMemMsgStore{msgs: hermes.NewQueue()}
}

func (store *InMemMsgStore) Put(msg *MqttPublishMessage) error {
	if msg == nil {
		return errors.New("invalid_message")
	}

	_, err := store.msgs.Add(msg)

	return err
}

func (store *InMemMsgStore) Poll(offset []byte) (*PollResult, error) {
	res := &PollResult{Msgs: make([]*MqttPublishMessage, 0, 1024)}
	for len(res.Msgs) < cap(res.Msgs) && store.msgs.Size() > 0 {
		res.Msgs = append(res.Msgs, store.msgs.Remove().(*MqttPublishMessage))
	}

	return res, nil
}

func IsPubSubID(id hermes.ReceiverID) bool {
	return string(id) == "/pubsub"
}

func PubSubID() hermes.ReceiverID {
	return hermes.ReceiverID("/pubsub")
}

type PubSubSubscribeRequest struct {
	SubscriberID hermes.ReceiverID
	Topics       []MqttTopicName
}
type PubSubSubscribeReply struct {
	err error
}

type PubSubUnsubscribeRequest struct {
	SubscriberID hermes.ReceiverID
	Topics       []MqttTopicName
}
type PubSubUnsubscribeReply struct {
	err error
}

type PubSubPublishRequest struct {
	msg *MqttPublishMessage
}
type PubSubPublishReply struct {
	err error
}

type PubSubMessagePublished struct {
	msg *MqttPublishMessage
}

type pubsubProcessMessages struct{}

type PubSub struct {
	sub       map[MqttTopicName]map[hermes.ReceiverID]bool
	msgs      MsgStore
	offset    []byte
	pollTimer hermes.Timer
}

func NewPubSub(store MsgStore) (*PubSub, error) {
	if store == nil {
		return nil, errors.New("invalid_msg_store")
	}

	return &PubSub{
		sub:  make(map[MqttTopicName]map[hermes.ReceiverID]bool),
		msgs: store,
	}, nil
}

func (ps *PubSub) recv(ctx hermes.Context, msg hermes.Message) {
	switch msg.Payload().(type) {
	case *hermes.Joined:
		ps.scheduleProcess(ctx)

	case *PubSubSubscribeRequest:
		psr := msg.Payload().(*PubSubSubscribeRequest)
		err := ps.onSubscribe(psr.SubscriberID, psr.Topics)
		ctx.Reply(msg, &PubSubSubscribeReply{err: err})

	case *PubSubUnsubscribeRequest:
		pur := msg.Payload().(*PubSubUnsubscribeRequest)
		err := ps.onUnsubscribe(pur.SubscriberID, pur.Topics)
		ctx.Reply(msg, &PubSubUnsubscribeReply{err: err})

	case *PubSubPublishRequest:
		ppr := msg.Payload().(*PubSubPublishRequest)
		err := ps.onPublish(ctx, ppr.msg)
		ctx.Reply(msg, &PubSubPublishReply{err: err})

	case *pubsubProcessMessages:
		ps.onProcessMsgs(ctx)
	}
}

func (ps *PubSub) onSubscribe(id hermes.ReceiverID, topics []MqttTopicName) error {
	if id == "" {
		return errors.New("invalid_receiver_id")
	}
	if topics == nil {
		return errors.New("invalid_topics")
	}
	if len(topics) == 0 {
		return nil
	}

	for _, t := range topics {
		ps.receivers(t)[id] = true
	}

	return nil
}

func (ps *PubSub) onUnsubscribe(id hermes.ReceiverID, topics []MqttTopicName) error {
	if id == "" {
		return errors.New("invalid_receiver_id")
	}
	if topics == nil {
		return errors.New("invalid_topics")
	}
	if len(topics) == 0 {
		return nil
	}

	for _, t := range topics {
		delete(ps.receivers(t), id)
	}

	return nil
}

func (ps *PubSub) onPublish(ctx hermes.Context, msg *MqttPublishMessage) error {
	if msg == nil {
		return errors.New("invalid_message")
	}

	//ps.publish(ctx, msg)

	ps.msgs.Put(msg)
	if ps.pollTimer == nil {
		ps.scheduleProcess(ctx)
	}

	return nil
}

func (ps *PubSub) onProcessMsgs(ctx hermes.Context) {
	res, err := ps.msgs.Poll(ps.offset)
	if err != nil {
		fmt.Printf("failed to poll messages: %s\n", err.Error())
	}

	for _, msg := range res.Msgs {
		ps.publish(ctx, msg)
	}

	ps.offset = res.Offset

	ps.scheduleProcess(ctx)
}

func (ps *PubSub) publish(ctx hermes.Context, msg *MqttPublishMessage) {
	recv := ps.receivers(msg.TopicName)
	ev := &PubSubMessagePublished{msg: msg}
	for id := range recv {
		ctx.Send(id, ev)
	}
}

func (ps *PubSub) scheduleProcess(ctx hermes.Context) {
	if ps.pollTimer == nil {
		t, err := ctx.Schedule(PollTimeoutDur, &pubsubProcessMessages{})
		if err != nil {
			fmt.Printf("[FATAL] failed to schedule process timer: %s\n", err.Error())
		}
		ps.pollTimer = t
	} else {
		ps.pollTimer.Reset(PollTimeoutDur)
	}
}

func (ps *PubSub) receivers(topic MqttTopicName) map[hermes.ReceiverID]bool {
	set, ok := ps.sub[topic]
	if !ok {
		set = make(map[hermes.ReceiverID]bool)
		ps.sub[topic] = set
	}

	return set
}
