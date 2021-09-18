package mqtt

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dartali/hermes"
)

const (
	SessionMaxOutboxSize  = 5
	SessionPublishTimeout = 10 * time.Second
)

func isSessionID(id hermes.ReceiverID) bool {
	return strings.HasPrefix(string(id), "/sessions")
}

func sessionID(id MqttClientId) hermes.ReceiverID {
	return hermes.ReceiverID("/sessions/" + string(id))
}

type SessionRegisterRequest struct {
	ConsumerID hermes.ReceiverID
}
type SessionRegisterReply struct {
	present bool
	Err     error
}

type SessionUnregisterRequest struct {
	ConsumerID hermes.ReceiverID
}
type SessionUnregisterReply struct {
	Err error
}
type SessionUnregistered struct{}

type SessionSubscribeReply struct {
	Ack *MqttSubAckMessage
	Err error
}

type SessionUnsubscribeReply struct {
	Ack *MqttUnSubAckMessage
	Err error
}

type SessionPublishRequest struct {
	Msg *MqttPublishMessage
}
type SessionPublishReply struct {
	Err error
}

type SessionPubAckRequest struct {
	PacketID MqttPacketId
}
type SessionPubAckReply struct {
	Err error
}

type SessionPubRelRequest struct {
	PacketID MqttPacketId
}
type SessionPubRelReply struct {
	Err error
}

type SessionCleanRequest struct{}
type SessionCleanReply struct {
	Err error
}

// events
type SessionMessagePublished struct {
	Msg *SessionMessage
}

type sessionProcessPublishes struct{}

var SessionProcessPublishes = &sessionProcessPublishes{}

type Session struct {
	consumer   hermes.ReceiverID
	store      SessionStore
	recvMsgs   *hermes.Queue
	repubTimer hermes.Timer
}

func newSession(store SessionStore) (*Session, error) {
	if store == nil {
		return nil, errors.New("invalid_session_store")
	}

	return &Session{store: store, recvMsgs: hermes.NewQueue()}, nil
}

func (s *Session) recv(ctx hermes.Context, msg hermes.Message) {
	switch msg.Payload().(type) {
	case *hermes.Joined:

	case *SessionRegisterRequest:
		present, err := s.onRegister(ctx, msg.Payload().(*SessionRegisterRequest))
		s.reply(ctx, msg, &SessionRegisterReply{present: present, Err: err})
		s.scheduleProcess(ctx)

	case *SessionUnregisterRequest:
		err := s.onUnregister(ctx, msg.Payload().(*SessionUnregisterRequest))
		s.reply(ctx, msg, &SessionUnregisterReply{Err: err})

	case *MqttSubscribeMessage:
		pid, codes, err := s.onSubscribe(ctx, msg.Payload().(*MqttSubscribeMessage))
		ack := &MqttSubAckMessage{PacketId: pid, ReturnCodes: codes}
		s.reply(ctx, msg, &SessionSubscribeReply{Ack: ack, Err: err})

	case *MqttUnsubscribeMessage:
		pid, err := s.onUnsubscribe(ctx, msg.Payload().(*MqttUnsubscribeMessage))
		ack := &MqttUnSubAckMessage{PacketId: pid}
		s.reply(ctx, msg, &SessionUnsubscribeReply{Ack: ack, Err: err})

	case *SessionPublishRequest:
		err := s.onPublishMessage(ctx, msg.Payload().(*SessionPublishRequest))
		s.reply(ctx, msg, &SessionPublishReply{Err: err})

	case *SessionPubRelRequest:
		err := s.onPubRel(ctx, msg.Payload().(*SessionPubRelRequest).PacketID)
		s.reply(ctx, msg, &SessionPubRelReply{Err: err})

	case *SessionPubAckRequest:
		err := s.onPubAck(ctx, msg.Payload().(*SessionPubAckRequest))
		s.reply(ctx, msg, &SessionPubAckReply{Err: err})
		s.scheduleProcess(ctx)

	case *PubSubMessagePublished:
		err := s.onStoreMessage(ctx, msg.Payload().(*PubSubMessagePublished).msg)
		if err != nil {
			fmt.Printf("failed to store message: %s\n", err.Error())
		}
		s.scheduleProcess(ctx)

	case *SessionCleanRequest:
		err := s.onClean(ctx)
		s.reply(ctx, msg, &SessionCleanReply{Err: err})

	case *sessionProcessPublishes:
		s.onProcessPublishes(ctx)
	}
}

func (s *Session) onRegister(ctx hermes.Context, msg *SessionRegisterRequest) (bool, error) {
	if msg.ConsumerID == "" {
		return false, errors.New("invalid_consumer_id")
	}

	if s.consumer != "" {
		ctx.Send(s.consumer, &SessionUnregistered{})
		s.consumer = ""
	}

	s.consumer = msg.ConsumerID

	return s.store.Create(string(ctx.ID()))
}

func (s *Session) onUnregister(ctx hermes.Context, msg *SessionUnregisterRequest) error {
	if msg.ConsumerID == "" || msg.ConsumerID != s.consumer {
		return errors.New("invalid_consumn")
	}

	s.consumer = ""

	return nil
}

func (s *Session) onSubscribe(ctx hermes.Context, msg *MqttSubscribeMessage) (MqttPacketId, []MqttSubAckStatus, error) {
	if msg.Subscriptions == nil || len(msg.Subscriptions) <= 0 { //[MQTT-3.8.3-3]
		return 0, nil, errors.New("missing_subscriptions")
	}

	codes, err := s.store.AddSub(string(ctx.ID()), msg.Subscriptions)
	if err != nil {
		return 0, nil, err
	}

	topics := s.topicNames(msg.topicFilter())
	req := &PubSubSubscribeRequest{SubscriberID: ctx.ID(), Topics: topics}
	rm, err := ctx.RequestWithTimeout(PubSubID(), req, 1500*time.Millisecond)
	if err != nil {
		return 0, nil, err
	}
	rep := rm.Payload().(*PubSubSubscribeReply)
	if rep.err != nil {
		return 0, nil, rep.err
	}

	return msg.PacketId, codes, err
}

func (s *Session) onUnsubscribe(ctx hermes.Context, msg *MqttUnsubscribeMessage) (MqttPacketId, error) {
	if msg.TopicFilters == nil || len(msg.TopicFilters) <= 0 {
		return 0, errors.New("missing_filters")
	}

	topics := s.topicNames(msg.TopicFilters)
	req := &PubSubUnsubscribeRequest{SubscriberID: ctx.ID(), Topics: topics}
	rm, err := ctx.RequestWithTimeout(PubSubID(), req, 1500*time.Millisecond)
	if err != nil {
		return 0, err
	}
	rep := rm.Payload().(*PubSubUnsubscribeReply)
	if rep.err != nil {
		return 0, rep.err
	}

	s.store.RemoveSub(string(ctx.ID()), msg.TopicFilters)

	return msg.PacketId, nil
}

func (s *Session) onPublishMessage(ctx hermes.Context, req *SessionPublishRequest) error {
	if req.Msg == nil {
		return errors.New("invalid_message")
	}

	rm, err := ctx.RequestWithTimeout(PubSubID(), &PubSubPublishRequest{msg: req.Msg}, 1500*time.Millisecond)
	if err != nil {
		return err
	}

	if req.Msg.QosLevel == MqttQoSLevel2 {
		s.recvMsgs.Add(req.Msg.PacketId)
	}

	rep := rm.Payload().(*PubSubPublishReply)
	if rep.err != nil {
		return rep.err
	}

	return nil
}

func (s *Session) onPubRel(ctx hermes.Context, pid MqttPacketId) error {
	rpid := s.recvMsgs.Peek().(MqttPacketId)
	if pid != rpid {
		return errors.New("invalid_release_pid")
	}

	s.recvMsgs.Remove()

	return nil
}

func (s *Session) onStoreMessage(ctx hermes.Context, msg *MqttPublishMessage) error {
	return s.store.Append(string(ctx.ID()), msg)
}

func (s *Session) onPubAck(ctx hermes.Context, msg *SessionPubAckRequest) error {
	sp, err := s.store.FetchLastInflightMessage(string(ctx.ID()))
	if err != nil {
		return err
	}

	if sp.id != msg.PacketID {
		return errors.New("invalid_packet_id")
	}

	s.store.Remove(string(ctx.ID()))

	s.scheduleRepublish(ctx)

	return nil
}

func (s *Session) onClean(ctx hermes.Context) error {
	return s.store.Clean(string(ctx.ID()))
}

func (s *Session) onProcessPublishes(ctx hermes.Context) {
	if s.consumer == "" {
		return
	}

	s.processPublishQueue(ctx)

	s.processOutbox(ctx)
}

func (s *Session) processPublishQueue(ctx hermes.Context) {
	msgs, err := s.store.FetchNewMessages(string(ctx.ID()))
	if err != nil {
		fmt.Printf("process publish queue failed: %s\n", err.Error())
		return
	}

	for _, m := range msgs {
		ctx.Send(s.consumer, &SessionMessagePublished{Msg: m})
	}
}

func (s *Session) processOutbox(ctx hermes.Context) {
	sm, err := s.store.FetchLastInflightMessage(string(ctx.ID()))
	if err != nil {
		fmt.Printf("process outbox failed: %s\n", err.Error())
		return
	}

	if sm == nil {
		return
	}

	if sm.hasTimeout() {
		sm.sentAt = time.Now()
		ctx.Send(s.consumer, &SessionMessagePublished{Msg: sm})
	}

	s.scheduleRepublish(ctx)
}

func (s *Session) scheduleRepublish(ctx hermes.Context) {
	if s.repubTimer != nil {
		sp, err := s.store.FetchLastInflightMessage(string(ctx.ID()))
		if err != nil {
			fmt.Printf("failed to reschedule repub timer: %s\n", err.Error())
			return
		}

		if sp != nil {
			s.repubTimer.Reset(sp.timeout())
		}
		return
	}

	t, err := ctx.Schedule(SessionPublishTimeout, SessionProcessPublishes)
	if err != nil {
		fmt.Printf("failed to schedule publishe process: %s\n", err.Error())
		return
	}

	s.repubTimer = t
}

func (s *Session) reply(ctx hermes.Context, msg hermes.Message, payload interface{}) {
	err := ctx.Reply(msg, payload)
	if err != nil {
		fmt.Printf("failed to reply to message: %s\n", err.Error())
	}
}

func (s *Session) shouldProcess(ctx hermes.Context) bool {
	if s.consumer == "" {
		return false
	}

	e, err := s.store.IsEmpty(string(ctx.ID()))
	if err != nil {
		fmt.Printf("failed to check if session is empty")
		return true
	}

	return !e
}

func (s *Session) scheduleProcess(ctx hermes.Context) {
	if !s.shouldProcess(ctx) {
		return
	}

	ctx.Send(ctx.ID(), SessionProcessPublishes)
}

func (s *Session) topicNames(filters []MqttTopicFilter) []MqttTopicName {
	topics := make([]MqttTopicName, 0, len(filters))
	for _, f := range filters {
		topics = append(topics, f.topicName())
	}

	return topics
}
