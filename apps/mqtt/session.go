package mqtt

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dartali/hermes"
	"github.com/elliotchance/orderedmap"
)

const (
	SessionMaxOutboxSize = 5
	SessionIDPrefix      = "/sessions/"
)

var (
	ErrSessionInvalidPacketID = errors.New("invalid_packet_id")
)

func IsSessionID(id hermes.ReceiverID) bool {
	return strings.HasPrefix(string(id), SessionIDPrefix)
}

func sessionID(id MqttClientId) hermes.ReceiverID {
	return hermes.ReceiverID(SessionIDPrefix + string(id))
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

type SessionPubRecRequest struct {
	PacketID MqttPacketId
}
type SessionPubRecReply struct {
	Err error
}

type SessionPubCompRequest struct {
	PacketID MqttPacketId
}
type SessionPubCompReply struct {
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
	Msg SessionMessage
}

type sessionProcessPublishes struct{}

var SessionProcessPublishes = &sessionProcessPublishes{}

func SessionPublish(ctx hermes.Context, sid hermes.ReceiverID, msg *MqttPublishMessage) error {
	r, err := ctx.RequestWithTimeout(sid, &SessionPublishRequest{Msg: msg}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPublishReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func SessionPubAck(ctx hermes.Context, sid hermes.ReceiverID, pid MqttPacketId) error {
	r, err := ctx.RequestWithTimeout(sid, &SessionPubAckRequest{PacketID: pid}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPubAckReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func SessionPubRec(ctx hermes.Context, sid hermes.ReceiverID, pid MqttPacketId) error {
	r, err := ctx.RequestWithTimeout(sid, &SessionPubRecRequest{PacketID: pid}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPubRecReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func SessionPubRel(ctx hermes.Context, sid hermes.ReceiverID, pid MqttPacketId) error {
	r, err := ctx.RequestWithTimeout(sid, &SessionPubRelRequest{PacketID: pid}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPubRelReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func SessionPubComp(ctx hermes.Context, sid hermes.ReceiverID, pid MqttPacketId) error {
	r, err := ctx.RequestWithTimeout(sid, &SessionPubCompRequest{PacketID: pid}, ClientRequestTimeout)
	if err != nil {
		return err
	}

	rep := r.Payload().(*SessionPubCompReply)
	if rep.Err != nil {
		return rep.Err
	}

	return nil
}

func SessionSubscribe(ctx hermes.Context, sid hermes.ReceiverID, msg *MqttSubscribeMessage) (*MqttSubAckMessage, error) {
	r, err := ctx.RequestWithTimeout(sid, msg, ClientRequestTimeout)
	if err != nil {
		return nil, err
	}

	rep := r.Payload().(*SessionSubscribeReply)
	if rep.Err != nil {
		return nil, rep.Err
	}

	return rep.Ack, nil
}

func SessionUnsubscribe(ctx hermes.Context, sid hermes.ReceiverID, msg *MqttUnsubscribeMessage) (*MqttUnSubAckMessage, error) {
	r, err := ctx.RequestWithTimeout(sid, msg, ClientRequestTimeout)
	if err != nil {
		return nil, err
	}

	rep := r.Payload().(*SessionUnsubscribeReply)
	if rep.Err != nil {
		return nil, rep.Err
	}

	return rep.Ack, nil
}

type Session struct {
	consumer     hermes.ReceiverID
	store        SessionStore
	inbox        *orderedmap.OrderedMap // TODO: move into store?
	repubTimer   hermes.Timer
	repubTimeout time.Duration
}

func NewSessionRecv(store SessionStore, repubTimeout time.Duration) (hermes.Receiver, error) {
	s, err := newSession(store, repubTimeout)
	if err != nil {
		return nil, err
	}

	return s.recv, nil
}

func newSession(store SessionStore, repubTimeout time.Duration) (*Session, error) {
	if store == nil {
		return nil, errors.New("invalid_session_store")
	}

	return &Session{
		store:        store,
		inbox:        orderedmap.NewOrderedMap(),
		repubTimeout: repubTimeout,
	}, nil
}

func (s *Session) recv(ctx hermes.Context, hm hermes.Message) {
	switch msg := hm.Payload().(type) {
	case *hermes.Joined:

	case *SessionRegisterRequest:
		present, err := s.onRegister(ctx, msg)
		s.reply(ctx, hm, &SessionRegisterReply{present: present, Err: err})
		s.scheduleProcess(ctx)

	case *SessionUnregisterRequest:
		err := s.onUnregister(ctx, msg)
		s.reply(ctx, hm, &SessionUnregisterReply{Err: err})

	case *MqttSubscribeMessage:
		pid, codes, err := s.onSubscribe(ctx, msg)
		ack := &MqttSubAckMessage{PacketId: pid, ReturnCodes: codes}
		s.reply(ctx, hm, &SessionSubscribeReply{Ack: ack, Err: err})

	case *MqttUnsubscribeMessage:
		pid, err := s.onUnsubscribe(ctx, msg)
		ack := &MqttUnSubAckMessage{PacketId: pid}
		s.reply(ctx, hm, &SessionUnsubscribeReply{Ack: ack, Err: err})

	case *SessionPublishRequest:
		err := s.onPublishMessage(ctx, msg)
		s.reply(ctx, hm, &SessionPublishReply{Err: err})

	case *SessionPubRelRequest:
		err := s.onPubRel(ctx, msg.PacketID)
		s.reply(ctx, hm, &SessionPubRelReply{Err: err})

	case *SessionPubAckRequest:
		err := s.onPubAck(ctx, msg)
		s.reply(ctx, hm, &SessionPubAckReply{Err: err})
		s.scheduleProcess(ctx)

	case *SessionPubRecRequest:
		err := s.onPubRec(ctx, msg.PacketID)
		s.reply(ctx, hm, &SessionPubRecReply{Err: err})
		s.scheduleProcess(ctx)

	case *SessionPubCompRequest:
		err := s.onPubComp(ctx, msg.PacketID)
		s.reply(ctx, hm, &SessionPubCompReply{Err: err})
		s.scheduleProcess(ctx)

	case *PubSubMessagePublished:
		err := s.onStoreMessage(ctx, msg.msg)
		if err != nil {
			fmt.Printf("failed to store message: %s\n", err.Error())
		}
		s.scheduleProcess(ctx)

	case *SessionCleanRequest:
		err := s.onClean(ctx)
		s.reply(ctx, hm, &SessionCleanReply{Err: err})

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

	if _, ok := s.inbox.Get(req.Msg.PacketId); ok {
		return errors.New("duplicate_publish")
	}

	rm, err := ctx.RequestWithTimeout(PubSubID(), &PubSubPublishRequest{msg: req.Msg}, 1500*time.Millisecond)
	if err != nil {
		return err
	}

	if req.Msg.QosLevel == MqttQoSLevel2 {
		s.inbox.Set(req.Msg.PacketId, true)
	}

	rep := rm.Payload().(*PubSubPublishReply)
	if rep.err != nil {
		return rep.err
	}

	return nil
}

func (s *Session) onPubRel(ctx hermes.Context, pid MqttPacketId) error {
	rpid := s.inbox.Front().Key.(MqttPacketId)
	if pid != rpid {
		return errors.New("invalid_release_pid")
	}

	s.inbox.Delete(pid)

	return nil
}

func (s *Session) onStoreMessage(ctx hermes.Context, msg *MqttPublishMessage) error {
	return s.store.AppendMsg(string(ctx.ID()), msg)
}

func (s *Session) onPubAck(ctx hermes.Context, msg *SessionPubAckRequest) error {
	return s.ackMsg(ctx, msg.PacketID, MqttQoSLevel1)
}

func (s *Session) onPubRec(ctx hermes.Context, pid MqttPacketId) error {
	return s.ackMsg(ctx, pid, MqttQoSLevel2)
}

func (s *Session) onPubComp(ctx hermes.Context, pid MqttPacketId) error {
	sid := string(ctx.ID())
	sp, err := s.store.FetchLastInflightMessage(sid, SMStateAcked)
	if err != nil {
		return err
	}
	if sp == nil {
		return ErrSessionInvalidPacketID
	}

	if sp.ID() != pid || sp.QoS() != MqttQoSLevel2 {
		return ErrSessionInvalidPacketID
	}

	ok, err := s.store.RemoveMsg(sid, pid)
	if !ok {
		return ErrSessionInvalidPacketID
	}
	if err != nil {
		return err
	}

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
		s.send(ctx, m)
	}
}

func (s *Session) processOutbox(ctx hermes.Context) {
	sm, err := s.store.FetchLastInflightMessage(string(ctx.ID()), SMStateAny)
	if err != nil {
		fmt.Printf("process outbox failed: %s\n", err.Error())
		return
	}

	if sm == nil {
		return
	}

	if s.hasTimeout(sm) {
		s.send(ctx, sm)
	}

	s.scheduleRepublish(ctx)
}

func (s *Session) send(ctx hermes.Context, msg SessionMessage) {
	s.store.SentMsg(string(ctx.ID()), msg.ID())
	ctx.Send(s.consumer, &SessionMessagePublished{Msg: msg})
}

func (s *Session) scheduleRepublish(ctx hermes.Context) {
	if s.repubTimer != nil {
		sp, err := s.store.FetchLastInflightMessage(string(ctx.ID()), SMStateAny)
		if err != nil {
			fmt.Printf("failed to reschedule repub timer: %s\n", err.Error())
			return
		}

		if sp != nil {
			s.repubTimer.Reset(s.timeout(sp))
		}
		return
	}

	t, err := ctx.Schedule(s.repubTimeout, SessionProcessPublishes)
	if err != nil {
		fmt.Printf("failed to schedule publishe process: %s\n", err.Error())
		return
	}

	s.repubTimer = t
}

func (s *Session) ackMsg(ctx hermes.Context, pid MqttPacketId, qos MqttQoSLevel) error {
	sid := string(ctx.ID())
	sp, err := s.store.FetchLastInflightMessage(sid, SMStatePublished)
	if err != nil {
		return err
	}
	if sp == nil {
		return ErrSessionInvalidPacketID
	}

	if sp.ID() != pid || sp.QoS() != qos {
		return ErrSessionInvalidPacketID
	}

	if qos == MqttQoSLevel1 {
		ok, err := s.store.RemoveMsg(sid, pid)
		if !ok {
			return ErrSessionInvalidPacketID
		}
		if err != nil {
			return err
		}

		s.scheduleRepublish(ctx)
	} else {
		ok, err := s.store.AckMsg(sid, pid)
		if err != nil {
			return err
		}
		if !ok {
			return ErrSessionInvalidPacketID
		}
	}

	return nil
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

func (s *Session) hasTimeout(msg SessionMessage) bool {
	return msg.SentAt().Add(s.repubTimeout).Before(time.Now())
}

func (s *Session) timeout(msg SessionMessage) time.Duration {
	t := time.Until(msg.SentAt().Add(s.repubTimeout))
	if t < 0 {
		return 0
	}

	return t
}
