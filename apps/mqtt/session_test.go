package mqtt

import (
	"fmt"
	"testing"
	"time"

	"github.com/dartali/hermes"
)

const (
	SessionRepubTimeout = 1 * time.Second
)

type TestContext struct {
	id                   hermes.ReceiverID
	onSetReceiver        func(hermes.Receiver)
	onSend               func(hermes.ReceiverID, interface{}) error
	onRequest            func(hermes.ReceiverID, interface{}) (chan hermes.Message, error)
	onRequestWithTimeout func(hermes.ReceiverID, interface{}, time.Duration) (hermes.Message, error)
	onReply              func(hermes.Message, interface{}) error
	onSchedule           func(time.Duration, interface{}) (hermes.Timer, error)
}

func (ctx *TestContext) ID() hermes.ReceiverID {
	return ctx.id
}

func (ctx *TestContext) SetReceiver(recv hermes.Receiver) {
	ctx.onSetReceiver(recv)
}

func (ctx *TestContext) Send(to hermes.ReceiverID, payload interface{}) error {
	return ctx.onSend(to, payload)
}

func (ctx *TestContext) Request(to hermes.ReceiverID, request interface{}) (chan hermes.Message, error) {
	return ctx.onRequest(to, request)
}

func (ctx *TestContext) RequestWithTimeout(to hermes.ReceiverID, request interface{}, timeout time.Duration) (hermes.Message, error) {
	return ctx.onRequestWithTimeout(to, request, timeout)
}

func (ctx *TestContext) Reply(msg hermes.Message, reply interface{}) error {
	return ctx.onReply(msg, reply)
}

func (ctx *TestContext) Schedule(after time.Duration, msg interface{}) (hermes.Timer, error) {
	return ctx.onSchedule(after, msg)
}

type NoopTimer struct{}

func (t *NoopTimer) Reset(time.Duration) bool {
	return true
}

func (t *NoopTimer) Stop() bool {
	return true
}

type TestMessage struct {
	payload interface{}
}

func (msg *TestMessage) Payload() interface{} {
	return msg.payload
}

func messageOf(payload interface{}) hermes.Message {
	return &TestMessage{payload: payload}
}

func TestConsumerRegister(t *testing.T) {
	ctx := &TestContext{id: "test-session"}
	s := createSession(t)

	testConsumerRegister(t, ctx, s, "")
	testConsumerRegister(t, ctx, s, "c1")
	testConsumerRegister(t, ctx, s, "c2")
}

func TestMessageDelivery(t *testing.T) {
	ctx := &TestContext{id: "s1"}
	s := createSession(t)
	cid := hermes.ReceiverID("c1")
	topic := MqttTopicName("t")

	testConsumerRegister(t, ctx, s, cid)
	testSubscribeTopic(t, ctx, s, topic)
	testPublishMessage(t, ctx, s, topic, "test")
	testPublishProcess(t, ctx, s, topic, "test")

	time.Sleep(100*time.Millisecond + SessionRepubTimeout)

	testPublishProcess(t, ctx, s, topic, "test")
}

func TestMultipleMessageDelivery(t *testing.T) {
	ctx := &TestContext{id: "s1"}
	s := createSession(t)
	cid := hermes.ReceiverID("c1")
	topic := MqttTopicName("t")

	testConsumerRegister(t, ctx, s, cid)
	testSubscribeTopic(t, ctx, s, topic)

	for mi := 0; mi < SessionMaxOutboxSize; mi++ {
		testPublishMessage(t, ctx, s, topic, fmt.Sprintf("m%d", mi))
		testPublishProcess(t, ctx, s, topic, fmt.Sprintf("m%d", mi))
	}
}

func createSession(t *testing.T) *Session {
	s, err := newSession(NewInMemSessionStore(), SessionRepubTimeout)
	if err != nil {
		t.Fatalf("faild to create session: %s\n", err.Error())
	}

	return s
}

func testConsumerRegister(t *testing.T, ctx *TestContext, s *Session, cid hermes.ReceiverID) {
	replied := false
	ctx.onReply = func(m hermes.Message, ri interface{}) error {
		res, ok := ri.(*SessionRegisterReply)
		if !ok {
			t.Fatalf("expected message of type SessionRegisterReply")
		}
		if cid != "" && res.Err != nil {
			t.Fatalf("register failed with error: %s\n", res.Err.Error())
		}
		if cid == "" && res.Err == nil {
			t.Fatalf("session should have replied with error")
		}

		replied = true

		return nil
	}

	sent := s.consumer == ""
	ctx.onSend = func(ri hermes.ReceiverID, i interface{}) error {
		if _, ok := i.(*SessionUnregistered); ok {
			if !ok {
				t.Fatalf("expected SessionUnregisterd event")
			}
		}
		sent = true
		return nil
	}

	s.recv(ctx, messageOf(&SessionRegisterRequest{ConsumerID: cid}))

	if s.consumer != cid {
		t.Fatalf("expected consumer to be set in session, instead found: %s \n", s.consumer)
	}
	if !replied {
		t.Fatalf("expected session to reply to the consumer")
	}
	if !sent {
		t.Fatal("expected old consumer to be sent the SessionUnregistered event")
	}
}

func testSubscribeTopic(t *testing.T, ctx *TestContext, s *Session, topic MqttTopicName) {
	ctx.onRequestWithTimeout = func(ri hermes.ReceiverID, i interface{}, d time.Duration) (hermes.Message, error) {
		return messageOf(&PubSubSubscribeReply{}), nil
	}

	replied := false
	ctx.onReply = func(m hermes.Message, i interface{}) error {
		r, ok := i.(*SessionSubscribeReply)
		if !ok {
			t.Fatalf("expected session subscribe reply")
		}
		if r.Err != nil {
			t.Fatalf("subscribe failed with error: %s\n", r.Err.Error())
		}

		replied = true
		return nil
	}
	subs := []*MqttSubscription{
		{QosLevel: MqttQoSLevel1, TopicFilter: MqttTopicFilter(topic)},
	}
	s.recv(ctx, messageOf(&MqttSubscribeMessage{PacketId: 1, Subscriptions: subs}))

	if !replied {
		t.Fatalf("expected session to reply to subscribe")
	}
}

func testPublishMessage(t *testing.T, ctx *TestContext, s *Session, topic MqttTopicName, payload string) {
	sent := false
	ctx.onSend = func(ri hermes.ReceiverID, i interface{}) error {
		if _, ok := i.(*sessionProcessPublishes); !ok {
			t.Fatalf("expected session to send itself a process message")
		}
		sent = true
		return nil
	}

	pub := &MqttPublishMessage{TopicName: topic, PacketId: 1, Payload: []byte(payload)}
	s.recv(ctx, messageOf(&PubSubMessagePublished{Msg: pub}))

	if !sent {
		t.Fatalf("expected session to send itself a process message")
	}
}

func testPublishProcess(t *testing.T, ctx *TestContext, s *Session, topic MqttTopicName, pub string) {
	sent := false
	ctx.onSend = func(ri hermes.ReceiverID, i interface{}) error {
		smp, ok := i.(*SessionMessagePublished)
		if !ok {
			t.Fatalf("expected message of type publish")
		}

		m := smp.Msg
		if m.Topic() != topic {
			t.Fatalf("expected message to be published to %s but got %s", topic, m.Topic())
		}
		if string(m.Payload()) != pub {
			t.Fatalf("expected %s payload but got %s", pub, string(m.Payload()))
		}

		sent = true
		return nil
	}
	ctx.onSchedule = func(d time.Duration, i interface{}) (hermes.Timer, error) {
		return &NoopTimer{}, nil
	}

	s.recv(ctx, messageOf(SessionProcessPublishes))
	if !sent {
		t.Fatalf("expected message to be published to consumer")
	}
}
