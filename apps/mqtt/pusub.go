package mqtt

import (
	"errors"

	"github.com/dartali/hermes"
)

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

type PubSub struct {
	sub map[MqttTopicName]map[hermes.ReceiverID]bool
}

func (ps *PubSub) recv(ctx hermes.Context, msg hermes.Message) {
	switch msg.Payload().(type) {
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

	recv := ps.receivers(msg.TopicName)
	ev := &PubSubMessagePublished{msg: msg}
	for id := range recv {
		ctx.Send(id, ev)
	}

	return nil
}

func (ps *PubSub) receivers(topic MqttTopicName) map[hermes.ReceiverID]bool {
	set, ok := ps.sub[topic]
	if !ok {
		set = make(map[hermes.ReceiverID]bool)
		ps.sub[topic] = set
	}

	return set
}
