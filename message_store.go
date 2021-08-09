package pubsub

import (
	"errors"
	"sync"

	iradix "github.com/hashicorp/go-immutable-radix"
)

type MessageId []byte

type Message struct {
	id      MessageId
	qid     StreamId
	payload interface{}
}

type StreamId string

type MessageStore struct {
	m        sync.RWMutex
	messages *iradix.Tree
}

func NewMessageStore() *MessageStore {
	return &MessageStore{messages: iradix.New()}
}

func (s *MessageStore) Add(sid StreamId, id MessageId, payload interface{}) error {
	if payload == nil {
		return errors.New("invaid_message_payload")
	}
	if sid == "" {
		return errors.New("invalid_queue_id")
	}

	s.m.Lock()
	defer s.m.Unlock()

	k := s.makeKey(sid, id)
	m := &Message{id: id, qid: sid, payload: payload}
	s.messages, _, _ = s.messages.Insert(k, m)

	return nil
}

func (s *MessageStore) Remove(qid StreamId, count int) error {
	if qid == "" {
		return errors.New("invalid_queue_id")
	}
	if count < 0 {
		return errors.New("invalid_count")
	}
	if count == 0 {
		return nil
	}

	s.m.Lock()
	defer s.m.Unlock()

	removed := 0
	it := s.messages.Root().Iterator()
	it.SeekLowerBound([]byte(qid))
	for k, i, ok := it.Next(); ok; k, i, ok = it.Next() {
		m, _ := i.(*Message)
		if m.qid != qid {
			break
		}

		s.messages, _, _ = s.messages.Delete(k)
		removed++
		if removed >= count {
			break
		}
	}

	return nil
}

func (s *MessageStore) Peek(qid StreamId, offset MessageId, count int) ([]interface{}, error) {
	if qid == "" {
		return nil, errors.New("invalid_queue_id")
	}
	if count < 0 {
		return nil, errors.New("invalid_entry_count")
	}
	if count == 0 {
		return make([]interface{}, 0), nil
	}

	s.m.RLock()
	defer s.m.RUnlock()

	out := make([]interface{}, 0, count)
	it := s.messages.Root().Iterator()

	if offset == nil {
		it.SeekLowerBound([]byte(qid))
	} else {
		it.SeekLowerBound(s.makeKey(qid, offset))
	}

	for _, i, ok := it.Next(); ok; _, i, ok = it.Next() {
		m := i.(*Message)
		if m.qid != qid {
			break
		}

		out = append(out, m.payload)
		if len(out) >= count {
			break
		}
	}

	return out, nil
}

func (s *MessageStore) Poll(qid StreamId, count int) ([]interface{}, error) {
	m, err := s.Peek(qid, nil, count)
	if err != nil {
		return nil, err
	}

	err = s.Remove(qid, len(m))
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (s *MessageStore) makeKey(qid StreamId, mid MessageId) []byte {
	return append([]byte(qid), mid...)
}
