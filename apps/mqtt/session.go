package mqtt

import (
	"errors"
	"fmt"
	"math"
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
	id hermes.ReceiverID
}
type SessionRegisterReply struct {
	present bool
	Err     error
}

type SessionUnregisterRequest struct {
	id hermes.ReceiverID
}
type SessionUnregisterReply struct {
	Err error
}
type SessionUnregistered struct{}

type SessionSubscribeReply struct {
	ack *MqttSubAckMessage
	Err error
}

type SessionUnsubscribeReply struct {
	ack *MqttUnSubAckMessage
	Err error
}

type SessionPublishRequest struct {
	msg *MqttPublishMessage
}
type SessionPublishReply struct {
	Err error
}

type SessionPubAckRequest struct {
	id MqttPacketId
}
type SessionPubAckReply struct {
	Err error
}

type SessionCleanRequest struct{}
type SessionCleanReply struct{}

// events
type SessionMessagePublished struct {
	msg *SessionMessage
}

type sessionProcessPublishes struct{}

var SessionProcessPublishes = &sessionProcessPublishes{}

type SessionState struct {
	sub map[MqttTopicFilter]*MqttSubscription
	pub *hermes.Queue
}

func newSessionState() *SessionState {
	return &SessionState{sub: make(map[MqttTopicFilter]*MqttSubscription), pub: hermes.NewQueue()}
}

func (state *SessionState) subscribe(msg *MqttSubscribeMessage) ([]MqttSubAckStatus, error) {
	codes := make([]MqttSubAckStatus, len(msg.Subscriptions))
	for si, sub := range msg.Subscriptions {
		if strings.ContainsAny(string(sub.TopicFilter), "#+") { //[MQTT-3.8.3-2]
			codes[si] = MqttSubAckFailure
			continue
		}

		if sub.QosLevel > MqttQoSLevel2 { //[MQTT-3.8.3-4]
			return nil, errors.New("invalid_qos_level")
		}

		state.sub[sub.TopicFilter] = &MqttSubscription{QosLevel: sub.QosLevel, TopicFilter: sub.TopicFilter}
	}

	return codes, nil
}

func (state *SessionState) unsubscribe(filters []MqttTopicFilter) {
	for _, f := range filters {
		delete(state.sub, f)
	}
}

func (state *SessionState) append(msg *MqttPublishMessage) error {
	for _, s := range state.sub {
		if s.TopicFilter.topicName() == msg.TopicName {
			sm := &SessionMessage{msg: msg, qos: s.QosLevel}
			_, err := state.pub.Add(sm)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (state *SessionState) clean() {
	state.sub = make(map[MqttTopicFilter]*MqttSubscription)
	state.pub = hermes.NewQueue()
}

type SessionMessage struct {
	id     MqttPacketId
	qos    MqttQoSLevel
	msg    *MqttPublishMessage
	sentAt time.Time
}

func (msg *SessionMessage) hasTimeout() bool {
	return msg.sentAt.Add(SessionPublishTimeout).Before(time.Now())
}

func (msg *SessionMessage) timeout() time.Duration {
	t := time.Until(msg.sentAt.Add(SessionPublishTimeout))
	if t < 0 {
		return 0
	}

	return t
}

type Session struct {
	consumer   hermes.ReceiverID
	state      *SessionState
	outbox     *hermes.Queue
	packetId   MqttPacketId
	repubTimer hermes.Timer
}

func newSession() *Session {
	return &Session{outbox: hermes.NewQueue()}
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
		s.reply(ctx, msg, &SessionSubscribeReply{ack: ack, Err: err})

	case *MqttUnsubscribeMessage:
		pid, err := s.onUnsubscribe(ctx, msg.Payload().(*MqttUnsubscribeMessage))
		ack := &MqttUnSubAckMessage{PacketId: pid}
		s.reply(ctx, msg, &SessionUnsubscribeReply{ack: ack, Err: err})

	case *SessionPublishRequest:
		err := s.onPublishMessage(ctx, msg.Payload().(*SessionPublishRequest))
		s.reply(ctx, msg, &SessionPublishReply{Err: err})

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
		s.onClean()
		s.reply(ctx, msg, &SessionCleanReply{})

	case *sessionProcessPublishes:
		s.onProcessPublishes(ctx)
	}
}

func (s *Session) onRegister(ctx hermes.Context, msg *SessionRegisterRequest) (bool, error) {
	if msg.id == "" {
		return false, errors.New("invalid_consumer_id")
	}

	if s.consumer != "" {
		ctx.Send(s.consumer, &SessionUnregistered{})
		s.consumer = ""
	}

	s.consumer = msg.id

	_, present := s.getState()

	return present, nil
}

func (s *Session) onUnregister(ctx hermes.Context, msg *SessionUnregisterRequest) error {
	if msg.id == "" || msg.id != s.consumer {
		return errors.New("invalid_consumn")
	}

	s.consumer = ""

	return nil
}

func (s *Session) onSubscribe(ctx hermes.Context, msg *MqttSubscribeMessage) (MqttPacketId, []MqttSubAckStatus, error) {
	if msg.Subscriptions == nil || len(msg.Subscriptions) <= 0 { //[MQTT-3.8.3-3]
		return 0, nil, errors.New("missing_subscriptions")
	}

	state, _ := s.getState()
	codes, err := state.subscribe(msg)
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

	state, _ := s.getState()

	state.unsubscribe(msg.TopicFilters)

	return msg.PacketId, nil
}

func (s *Session) onPublishMessage(ctx hermes.Context, req *SessionPublishRequest) error {
	if req.msg == nil {
		return errors.New("invalid_message")
	}

	rm, err := ctx.RequestWithTimeout(PubSubID(), &PubSubPublishRequest{msg: req.msg}, 1500*time.Millisecond)
	if err != nil {
		return err
	}

	rep := rm.Payload().(*PubSubPublishReply)
	if rep.err != nil {
		return rep.err
	}

	return nil
}

func (s *Session) onStoreMessage(ctx hermes.Context, msg *MqttPublishMessage) error {
	if msg == nil {
		return errors.New("invalid_message")
	}

	state, _ := s.getState()

	return state.append(msg)
}

func (s *Session) onPubAck(ctx hermes.Context, msg *SessionPubAckRequest) error {
	sp := s.outbox.Peek().(*SessionMessage)
	if sp.id != msg.id {
		return errors.New("invalid_packet_id")
	}

	s.outbox.Remove()
	s.scheduleRepublish(ctx)

	return nil
}

func (s *Session) onClean() {
	state, _ := s.getState()
	state.clean()
}

func (s *Session) onProcessPublishes(ctx hermes.Context) {
	if s.consumer == "" {
		return
	}

	s.processPublishQueue(ctx)

	s.processOutbox(ctx)
}

func (s *Session) processPublishQueue(ctx hermes.Context) {
	state, _ := s.getState()
	cap := SessionMaxOutboxSize - s.outbox.Size()

	for mi := 0; mi < cap && state.pub.Size() > 0; mi++ {
		sm := state.pub.Remove().(*SessionMessage)
		sm.id = s.nextPacketId()
		sm.sentAt = time.Now()
		if sm.qos != MqttQoSLevel0 {
			s.outbox.Add(sm)
		}
		ctx.Send(s.consumer, &SessionMessagePublished{msg: sm})
	}
}

func (s *Session) processOutbox(ctx hermes.Context) {
	if s.outbox.Size() <= 0 {
		return
	}

	msg := s.outbox.Peek().(*SessionMessage)
	if msg.hasTimeout() {
		msg.sentAt = time.Now()
		ctx.Send(s.consumer, &SessionMessagePublished{msg: msg})
	}

	s.scheduleRepublish(ctx)
}

func (s *Session) scheduleRepublish(ctx hermes.Context) {
	if s.repubTimer != nil {
		if s.outbox.Size() > 0 {
			sp := s.outbox.Peek().(*SessionMessage)
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

func (s *Session) getState() (outState *SessionState, present bool) {
	if s.state != nil {
		return s.state, true
	}

	s.state = newSessionState()

	return s.state, false
}

func (s *Session) reply(ctx hermes.Context, msg hermes.Message, payload interface{}) {
	err := ctx.Reply(msg, payload)
	if err != nil {
		fmt.Printf("failed to reply to message: %s\n", err.Error())
	}
}

func (s *Session) shouldProcess() bool {
	if s.consumer == "" {
		return false
	}

	state, _ := s.getState()
	if state.pub.Size() <= 0 {
		return false
	}

	return (SessionMaxOutboxSize - s.outbox.Size()) > 0
}

func (s *Session) scheduleProcess(ctx hermes.Context) {
	if !s.shouldProcess() {
		return
	}

	ctx.Send(ctx.ID(), SessionProcessPublishes)
}

func (s *Session) nextPacketId() MqttPacketId {
	if s.packetId == math.MaxUint16 {
		s.packetId = 0
	} else {
		s.packetId++
	}

	return s.packetId
}

func (s *Session) topicNames(filters []MqttTopicFilter) []MqttTopicName {
	topics := make([]MqttTopicName, 0, len(filters))
	for _, f := range filters {
		topics = append(topics, f.topicName())
	}

	return topics
}
