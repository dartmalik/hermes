package hermes

import (
	"errors"
	"hash/maphash"
	"sync"
)

var ErrMapElementAlreadyExists = errors.New("already_exists")

type syncMapSegment struct {
	mu   sync.RWMutex
	elem map[string]interface{}
}

func newSyncMapSegment() *syncMapSegment {
	return &syncMapSegment{elem: make(map[string]interface{})}
}

func (seg *syncMapSegment) put(key string, value interface{}, overwrite bool) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if !overwrite {
		if _, ok := seg.elem[key]; ok {
			return ErrMapElementAlreadyExists
		}
	}

	seg.elem[key] = value

	return nil
}

func (seg *syncMapSegment) get(key string) (interface{}, bool) {
	seg.mu.RLock()
	defer seg.mu.RUnlock()

	v, ok := seg.elem[key]

	return v, ok
}

func (seg *syncMapSegment) delete(key string) {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	delete(seg.elem, key)
}

const SyncMapSegments = 64

type syncMap struct {
	seg  []*syncMapSegment
	seed maphash.Seed
}

func newSyncMap() *syncMap {
	seg := make([]*syncMapSegment, SyncMapSegments)
	for si := 0; si < SyncMapSegments; si++ {
		seg[si] = newSyncMapSegment()
	}

	return &syncMap{
		seg:  seg,
		seed: maphash.MakeSeed(),
	}
}

func (m *syncMap) put(key string, value interface{}, overwrite bool) error {
	return m.segment(key).put(key, value, overwrite)
}

func (m *syncMap) get(key string) (interface{}, bool) {
	return m.segment(key).get(key)
}

func (m *syncMap) delete(key string) {
	m.segment(key).delete(key)
}

func (m *syncMap) segment(key string) *syncMapSegment {
	var hash maphash.Hash

	hash.SetSeed(m.seed)
	hash.WriteString(key)
	si := int(hash.Sum64() % SyncMapSegments)

	return m.seg[si]
}
