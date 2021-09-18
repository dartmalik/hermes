package mqtt

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/dartali/hermes"
)

var ErrSessionMissing = errors.New("missing_session")

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

type sessionState struct {
	sub      map[MqttTopicFilter]*MqttSubscription
	pub      *hermes.Queue
	outbox   *hermes.Queue
	packetId MqttPacketId
}

func newSessionState() *sessionState {
	return &sessionState{
		sub:    make(map[MqttTopicFilter]*MqttSubscription),
		pub:    hermes.NewQueue(),
		outbox: hermes.NewQueue(),
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
		sm.id = s.nextPacketId()
		sm.sentAt = time.Now()
		if sm.qos != MqttQoSLevel0 {
			s.outbox.Add(sm)
		}

		msgs = append(msgs, sm)
	}

	return msgs
}

func (s *sessionState) fetchLastInflightMessage() *SessionMessage {
	if s.outbox.Size() <= 0 {
		return nil
	}

	return s.outbox.Peek().(*SessionMessage)
}

func (s *sessionState) remove() {
	s.outbox.Remove()
}

func (s *sessionState) inflightCap() int {
	return SessionMaxOutboxSize - s.outbox.Size()
}

func (state *sessionState) clean() {
	state.sub = make(map[MqttTopicFilter]*MqttSubscription)
	state.pub = hermes.NewQueue()
	state.outbox = hermes.NewQueue()
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
	Append(id string, msg *MqttPublishMessage) error
	Remove(id string) error
	Clean(id string) error
	FetchNewMessages(id string) ([]*SessionMessage, error)
	FetchLastInflightMessage(id string) (*SessionMessage, error)
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

func (store *InMemSessionStore) Append(id string, msg *MqttPublishMessage) error {
	if msg == nil {
		return errors.New("invalid_message")
	}

	s, ok := store.sessions.Get(id)
	if !ok {
		return ErrSessionMissing
	}

	return s.(*sessionState).append(msg)
}

func (store *InMemSessionStore) Remove(id string) error {
	s, ok := store.sessions.Get(id)
	if !ok {
		return ErrSessionMissing
	}

	s.(*sessionState).remove()

	return nil
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

func (store *InMemSessionStore) FetchLastInflightMessage(id string) (*SessionMessage, error) {
	s, ok := store.sessions.Get(id)
	if !ok {
		return nil, ErrSessionMissing
	}

	return s.(*sessionState).fetchLastInflightMessage(), nil
}

func (store *InMemSessionStore) IsEmpty(id string) (bool, error) {
	s, ok := store.sessions.Get(id)
	if !ok {
		return false, ErrSessionMissing
	}

	st := s.(*sessionState)

	return st.pub.Size() <= 0 || st.inflightCap() <= 0, nil
}
