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

func IsPubSubID(id hermes.ReceiverID) bool {
	return string(id) == "/pubsub"
}

func PubSubID() hermes.ReceiverID {
	return hermes.ReceiverID("/pubsub")
}

type PubSubSubscribeRequest struct {
	SubscriberID hermes.ReceiverID
	Topics       []TopicName
}
type PubSubSubscribeReply struct {
	Err error
}

type PubSubUnsubscribeRequest struct {
	SubscriberID hermes.ReceiverID
	Topics       []TopicName
}
type PubSubUnsubscribeReply struct {
	Err error
}

type PubSubPublishRequest struct {
	Msg *PublishMessage
}
type PubSubPublishReply struct {
	Err error
}

type PubSubMessagePublished struct {
	Msg *PublishMessage
}

type pubsubProcessMessages struct{}

func PubSubPublish(ctx hermes.Context, msg *PublishMessage) error {
	rm, err := ctx.RequestWithTimeout(PubSubID(), &PubSubPublishRequest{Msg: msg}, 1500*time.Millisecond)
	if err != nil {
		return err
	}

	rep := rm.Payload().(*PubSubPublishReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

type PubSub struct {
	sub       map[TopicName]map[hermes.ReceiverID]bool
	msgs      MsgStore
	offset    []byte
	pollTimer hermes.Timer
}

func NewPubSubRecv(store MsgStore) (hermes.Receiver, error) {
	ps, err := NewPubSub(store)
	if err != nil {
		return nil, err
	}

	return ps.recv, nil
}

func NewPubSub(store MsgStore) (*PubSub, error) {
	if store == nil {
		return nil, errors.New("invalid_msg_store")
	}

	return &PubSub{
		sub:  make(map[TopicName]map[hermes.ReceiverID]bool),
		msgs: store,
	}, nil
}

func (ps *PubSub) recv(ctx hermes.Context, hm hermes.Message) {
	switch msg := hm.Payload().(type) {
	case *hermes.Joined:
		ps.scheduleProcess(ctx)

	case *PubSubSubscribeRequest:
		err := ps.onSubscribe(ctx, msg.SubscriberID, msg.Topics)
		ctx.Reply(hm, &PubSubSubscribeReply{Err: err})

	case *PubSubUnsubscribeRequest:
		err := ps.onUnsubscribe(msg.SubscriberID, msg.Topics)
		ctx.Reply(hm, &PubSubUnsubscribeReply{Err: err})

	case *PubSubPublishRequest:
		err := ps.onPublish(ctx, msg.Msg)
		ctx.Reply(hm, &PubSubPublishReply{Err: err})

	case *pubsubProcessMessages:
		ps.onProcessMsgs(ctx)
	}
}

func (ps *PubSub) onSubscribe(ctx hermes.Context, id hermes.ReceiverID, topics []TopicName) error {
	if id == "" {
		return errors.New("invalid_receiver_id")
	}
	if topics == nil {
		return errors.New("invalid_topics")
	}
	if len(topics) == 0 {
		return nil
	}

	subTopics := make([]TopicName, len(topics))
	for _, t := range topics {
		recv := ps.receivers(t)
		if _, ok := recv[id]; ok {
			continue
		}

		recv[id] = true
		subTopics = append(subTopics, t)
	}

	err := ps.sendRetainedMsgs(ctx, id, subTopics)
	if err != nil {
		fmt.Printf("[ERROR] failed to publish retained messages")
	}

	return nil
}

func (ps *PubSub) onUnsubscribe(id hermes.ReceiverID, topics []TopicName) error {
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

func (ps *PubSub) onPublish(ctx hermes.Context, msg *PublishMessage) error {
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

func (ps *PubSub) publish(ctx hermes.Context, msg *PublishMessage) {
	recv := ps.receivers(msg.TopicName)
	ev := &PubSubMessagePublished{Msg: msg}
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

func (ps *PubSub) sendRetainedMsgs(ctx hermes.Context, id hermes.ReceiverID, topics []TopicName) error {
	if len(topics) <= 0 {
		return nil
	}

	rms, err := ps.msgs.Get(topics)
	if err != nil {
		return err
	}

	for _, m := range rms {
		if m == nil {
			continue
		}

		ev := &PubSubMessagePublished{Msg: m}
		err = ctx.Send(id, ev)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ps *PubSub) receivers(topic TopicName) map[hermes.ReceiverID]bool {
	set, ok := ps.sub[topic]
	if !ok {
		set = make(map[hermes.ReceiverID]bool)
		ps.sub[topic] = set
	}

	return set
}
