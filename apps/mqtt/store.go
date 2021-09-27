package mqtt

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/dartali/hermes"
	"github.com/elliotchance/orderedmap"
)

var ErrSessionMissing = errors.New("missing_session")
var ErrSessionMissingMsg = errors.New("missing_message")

const (
	SMStateQueued = iota
	SMStatePublished
	SMStateAcked
	SMStateAny
)

type SessionMessage struct {
	id        MqttPacketId
	qos       MqttQoSLevel
	msg       *MqttPublishMessage
	state     int
	sentAt    time.Time
	sentCount int
}

func (msg *SessionMessage) Sent() {
	msg.sentCount++
	msg.sentAt = time.Now()
}

type sessionState struct {
	sub      map[MqttTopicFilter]*MqttSubscription
	pub      *hermes.Queue
	outbox   *orderedmap.OrderedMap
	packetId MqttPacketId
}

func newSessionState() *sessionState {
	return &sessionState{
		sub:    make(map[MqttTopicFilter]*MqttSubscription),
		pub:    hermes.NewQueue(),
		outbox: orderedmap.NewOrderedMap(),
	}
}

func (state *sessionState) subscribe(subs []MqttSubscription) ([]MqttSubAckStatus, error) {
	codes := make([]MqttSubAckStatus, len(subs))
	for si, sub := range subs {
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

func (state *sessionState) unsubscribe(filters []MqttTopicFilter) {
	for _, f := range filters {
		delete(state.sub, f)
	}
}

func (state *sessionState) append(msg *MqttPublishMessage) error {
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

func (s *sessionState) fetchNewMessages() []*SessionMessage {
	cap := s.inflightCap()
	msgs := make([]*SessionMessage, 0, cap)

	for mi := 0; mi < cap && s.pub.Size() > 0; mi++ {
		sm := s.pub.Remove().(*SessionMessage)
		sm.id, sm.sentAt = s.nextPacketId(), time.Now()
		sm.state = SMStatePublished

		if sm.qos != MqttQoSLevel0 {
			s.outbox.Set(sm.id, sm)
		}

		msgs = append(msgs, sm)
	}

	return msgs
}

func (s *sessionState) fetchLastInflightMessage(state int) *SessionMessage {
	for e := s.outbox.Front(); e != nil; e = e.Next() {
		sm := e.Value.(*SessionMessage)
		if sm.state == state || state == SMStateAny {
			return sm
		}
	}

	return nil
}

func (s *sessionState) removeMsg(pid MqttPacketId) bool {
	return s.outbox.Delete(pid)
}

func (s *sessionState) ackMsg(pid MqttPacketId) bool {
	i, ok := s.outbox.Get(pid)
	if !ok {
		return false
	}

	sm := i.(*SessionMessage)
	if sm.state != SMStatePublished {
		return false
	}

	sm.state = SMStateAcked

	return true
}

func (s *sessionState) sentMsg(pid MqttPacketId) error {
	i, ok := s.outbox.Get(pid)
	if !ok {
		return ErrSessionMissingMsg
	}

	sm := i.(*SessionMessage)
	sm.sentCount++
	sm.sentAt = time.Now()

	return nil
}

func (s *sessionState) inflightCap() int {
	return SessionMaxOutboxSize - s.outbox.Len()
}

func (state *sessionState) clean() {
	state.sub = make(map[MqttTopicFilter]*MqttSubscription)
	state.pub = hermes.NewQueue()
	state.outbox = orderedmap.NewOrderedMap()
}

func (s *sessionState) nextPacketId() MqttPacketId {
	if s.packetId == math.MaxUint16 {
		s.packetId = 0
	} else {
		s.packetId++
	}

	return s.packetId
}

type SessionStore interface {
	Create(id string) (present bool, err error)
	AddSub(id string, subs []MqttSubscription) ([]MqttSubAckStatus, error)
	RemoveSub(id string, filters []MqttTopicFilter) error
	AppendMsg(id string, msg *MqttPublishMessage) error
	RemoveMsg(id string, pid MqttPacketId) (deleted bool, err error)
	AckMsg(id string, pid MqttPacketId) (done bool, err error)
	SentMsg(id string, pid MqttPacketId) error
	Clean(id string) error
	FetchNewMessages(id string) ([]*SessionMessage, error)
	FetchLastInflightMessage(id string, state int) (*SessionMessage, error)
	IsEmpty(id string) (bool, error)
}

type InMemSessionStore struct {
	sessions *hermes.SyncMap
}

func NewInMemSessionStore() *InMemSessionStore {
	return &InMemSessionStore{sessions: hermes.NewSyncMap()}
}

func (store *InMemSessionStore) Create(id string) (bool, error) {
	if _, ok := store.sessions.Get(id); ok {
		return true, nil
	}

	err := store.sessions.Put(id, newSessionState(), false)
	if err == hermes.ErrMapElementAlreadyExists {
		return false, nil
	}

	return false, nil
}

func (store *InMemSessionStore) AddSub(id string, subs []MqttSubscription) ([]MqttSubAckStatus, error) {
	s, ok := store.sessions.Get(id)
	if !ok {
		return nil, ErrSessionMissing
	}

	return s.(*sessionState).subscribe(subs)
}

func (store *InMemSessionStore) RemoveSub(id string, filters []MqttTopicFilter) error {
	s, ok := store.sessions.Get(id)
	if !ok {
		return ErrSessionMissing
	}

	s.(*sessionState).unsubscribe(filters)

	return nil
}

func (store *InMemSessionStore) AppendMsg(id string, msg *MqttPublishMessage) error {
	if msg == nil {
		return errors.New("invalid_message")
	}

	s, ok := store.sessions.Get(id)
	if !ok {
		return ErrSessionMissing
	}

	return s.(*sessionState).append(msg)
}

func (store *InMemSessionStore) RemoveMsg(id string, pid MqttPacketId) (bool, error) {
	s, ok := store.sessions.Get(id)
	if !ok {
		return false, ErrSessionMissing
	}

	return s.(*sessionState).removeMsg(pid), nil
}

func (store *InMemSessionStore) AckMsg(id string, pid MqttPacketId) (bool, error) {
	s, ok := store.sessions.Get(id)
	if !ok {
		return false, ErrSessionMissing
	}

	return s.(*sessionState).ackMsg(pid), nil
}

func (store *InMemSessionStore) SentMsg(id string, pid MqttPacketId) error {
	s, ok := store.sessions.Get(id)
	if !ok {
		return ErrSessionMissing
	}

	return s.(*sessionState).sentMsg(pid)
}

func (store *InMemSessionStore) Clean(id string) error {
	s, ok := store.sessions.Get(id)
	if !ok {
		return ErrSessionMissing
	}

	s.(*sessionState).clean()

	return nil
}

func (store *InMemSessionStore) FetchNewMessages(id string) ([]*SessionMessage, error) {
	s, ok := store.sessions.Get(id)
	if !ok {
		return nil, ErrSessionMissing
	}

	return s.(*sessionState).fetchNewMessages(), nil
}

func (store *InMemSessionStore) FetchLastInflightMessage(id string, state int) (*SessionMessage, error) {
	s, ok := store.sessions.Get(id)
	if !ok {
		return nil, ErrSessionMissing
	}

	return s.(*sessionState).fetchLastInflightMessage(state), nil
}

func (store *InMemSessionStore) IsEmpty(id string) (bool, error) {
	s, ok := store.sessions.Get(id)
	if !ok {
		return false, ErrSessionMissing
	}

	st := s.(*sessionState)

	return st.pub.Size() <= 0 || st.inflightCap() <= 0, nil
}
