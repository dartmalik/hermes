package pubsub

import (
	"errors"
	"sync"

	iradix "github.com/hashicorp/go-immutable-radix"
)

const (
	MaxLogSize = 8192
)

type Group string

type CommandType uint8

const (
	JoinGroup CommandType = iota
	LeaveGroup
)

type LogEntry struct {
	t        CommandType
	g        Group
	snapshot *iradix.Tree
}

type GSConsumerState uint8

const (
	GSConsumerInstallingSnapshot GSConsumerState = iota
	GSConsumerReplicatingLog
)

type GSConsumer struct {
	state  GSConsumerState
	offset uint64
	it     *iradix.Iterator
}

type GroupStore struct {
	mu        sync.RWMutex
	leader    bool
	subs      *iradix.Tree
	log       map[uint64]*LogEntry
	head      uint64
	tail      uint64
	consumers map[string]*GSConsumer
}

func NewGroupStore(leader bool) *GroupStore {
	return &GroupStore{
		leader: leader,
		subs:   iradix.New(),
		log:    make(map[uint64]*LogEntry),
		head:   0,
		tail:   0,
	}
}

func (s *GroupStore) Join(g Group) error {
	if g == "" {
		return errors.New("invalid_group")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	subs, _, ok := s.subs.Insert([]byte(g), g)
	if !ok {
		return nil
	}

	s.subs = subs
	s.append(JoinGroup, g, subs)

	return nil
}

func (s *GroupStore) Leave(g Group) error {
	if g == "" {
		return errors.New("invalid_group")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	subs, _, ok := s.subs.Delete([]byte(g))
	if !ok {
		return nil
	}

	s.subs = subs
	s.append(LeaveGroup, g, subs)

	return nil
}

func (s *GroupStore) Clear() error {
	if s.leader {
		return errors.New("only_allowed_on_follower")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.subs = iradix.New()
	s.log = make(map[uint64]*LogEntry)
	s.head, s.tail = 0, 0

	return nil
}

func (s *GroupStore) Joined(g Group) (bool, error) {
	if g == "" {
		return false, errors.New("invalid_group")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	it := s.subs.Root().Iterator()
	it.SeekLowerBound([]byte(g))
	_, v, ok := it.Next()

	return ok && v.(Group) == g, nil
}

func (s *GroupStore) AddConsumer(uuid string) error {
	if uuid == "" {
		return errors.New("invalid_consumer_id")
	}

	_, ok := s.consumers[uuid]
	if ok {
		return nil
	}

	c := &GSConsumer{state: GSConsumerInstallingSnapshot, offset: s.head}
	s.consumers[uuid] = c

	return nil
}

func (s *GroupStore) RemoveConsumer(uuid string) {
	delete(s.consumers, uuid)
}

type InvalidOffsetError error

func (s *GroupStore) Read(offset uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < s.tail {
		return InvalidOffsetError(errors.New("invalid_offset"))
	}

	return nil
}

func (s *GroupStore) append(ct CommandType, g Group, t *iradix.Tree) {
	s.head++
	s.log[s.head] = &LogEntry{t: ct, g: g, snapshot: t}
	if s.tail == 0 {
		s.tail = s.head
	}
}
