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

func sessionID(id MqttClientId) hermes.ReceiverID {
	return hermes.ReceiverID("/sessions/" + string(id))
}

type SessionRegisterRequest struct {
	id hermes.ReceiverID
}
type SessionRegisterReply struct {
	present bool
	err     error
}

type SessionUnregisterRequest struct {
	id hermes.ReceiverID
}
type SessionUnregisterReply struct {
	err error
}
type SessionUnregistered struct{}

type SessionSubscribeReply struct {
	ack *MqttSubAckMessage
	err error
}

type SessionUnsubscribeReply struct {
	ack *MqttUnSubAckMessage
	err error
}

type SessionPublishReply struct {
	err error
}

type SessionPubAckRequest struct {
	id MqttPacketId
}
type SessionPubAckReply struct {
	err error
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
	for f := range state.sub {
		if f.topicName() == msg.TopicName {
			_, err := state.pub.Add(msg)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type SessionPublishMessage struct {
	id     MqttPacketId
	msg    *MqttPublishMessage
	sentAt time.Time
}

func (msg *SessionPublishMessage) hasTimeout() bool {
	return msg.sentAt.Add(SessionPublishTimeout).Before(time.Now())
}

func (msg *SessionPublishMessage) timeout() time.Duration {
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
	case *SessionRegisterRequest:
		present, err := s.onRegister(ctx, msg.Payload().(*SessionRegisterRequest))
		ctx.Reply(msg, &SessionRegisterReply{present: present, err: err})
		s.scheduleProcess(ctx)

	case *SessionUnregisterRequest:
		err := s.onUnregister(ctx, msg.Payload().(*SessionUnregisterRequest))
		ctx.Reply(msg, &SessionUnregisterReply{err: err})

	case *MqttSubscribeMessage:
		pid, codes, err := s.onSubscribe(ctx, msg.Payload().(*MqttSubscribeMessage))
		ack := &MqttSubAckMessage{PacketId: pid, ReturnCodes: codes}
		ctx.Reply(msg, &SessionSubscribeReply{ack: ack, err: err})

	case *MqttUnsubscribeMessage:
		pid, err := s.onUnsubscribe(ctx, msg.Payload().(*MqttUnsubscribeMessage))
		ack := &MqttUnSubAckMessage{PacketId: pid}
		ctx.Reply(msg, &SessionUnsubscribeReply{ack: ack, err: err})

	case *MqttPublishMessage:
		err := s.onPublish(ctx, msg.Payload().(*MqttPublishMessage))
		ctx.Reply(msg, &SessionPublishReply{err: err})
		s.scheduleProcess(ctx)

	case *SessionPubAckRequest:
		err := s.onPubAck(ctx, msg.Payload().(*SessionPubAckRequest))
		ctx.Reply(msg, &SessionPubAckReply{err: err})
		s.scheduleProcess(ctx)

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

	return msg.PacketId, codes, err
}

func (s *Session) onUnsubscribe(ctx hermes.Context, msg *MqttUnsubscribeMessage) (MqttPacketId, error) {
	if msg.TopicFilters == nil || len(msg.TopicFilters) <= 0 {
		return 0, errors.New("missing_filters")
	}

	state, _ := s.getState()

	state.unsubscribe(msg.TopicFilters)

	return msg.PacketId, nil
}

func (s *Session) onPublish(ctx hermes.Context, msg *MqttPublishMessage) error {
	state, _ := s.getState()

	return state.append(msg)
}

func (s *Session) onPubAck(ctx hermes.Context, msg *SessionPubAckRequest) error {
	sp := s.outbox.Peek().(*SessionPublishMessage)
	if sp.id != msg.id {
		return errors.New("invalid_packet_id")
	}

	s.outbox.Remove()
	s.scheduleRepublish(ctx)

	return nil
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
		pub := state.pub.Remove().(*MqttPublishMessage)
		sp := &SessionPublishMessage{msg: pub, id: s.nextPacketId(), sentAt: time.Now()}
		s.outbox.Add(sp)
		ctx.Send(s.consumer, sp)
	}
}

func (s *Session) processOutbox(ctx hermes.Context) {
	sp := s.outbox.Peek().(*SessionPublishMessage)
	if sp.hasTimeout() {
		sp.sentAt = time.Now()
		ctx.Send(s.consumer, sp)
	}

	s.scheduleRepublish(ctx)
}

func (s *Session) scheduleRepublish(ctx hermes.Context) {
	if s.repubTimer != nil {
		if s.outbox.Size() > 0 {
			sp := s.outbox.Peek().(*SessionPublishMessage)
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
