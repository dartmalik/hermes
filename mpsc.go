package hermes

import (
	"errors"
	"sync"
)

type syncSegment struct {
	mu   sync.Mutex
	elem []interface{}
	head int
	tail int
	next *syncSegment
}

func newSegment(cap int) *syncSegment {
	return &syncSegment{elem: make([]interface{}, cap), head: 0, tail: -1}
}

func (seg *syncSegment) add(e interface{}) bool {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	seg.tail++
	if seg.tail >= len(seg.elem) {
		return false
	}

	seg.elem[seg.tail] = e

	return true
}

func (seg *syncSegment) peek() interface{} {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.isEmpty() {
		return nil
	}

	return seg.elem[seg.head]
}

func (seg *syncSegment) removeAndPeek() interface{} {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.isEmpty() {
		return nil
	}

	seg.elem[seg.head] = nil
	seg.head++

	if seg.isEmpty() {
		return nil
	}

	return seg.elem[seg.head]
}

func (seg *syncSegment) isEmpty() bool {
	if int(seg.head) >= len(seg.elem) {
		return true
	}
	if seg.head > seg.tail {
		return true
	}

	return false
}

const (
	mpscDefaultSegLen = 1024
)

type Queue interface {
	Add(elem interface{})
	Peek() interface{}
	RemoveAndPeek() interface{}
	IsEmpty() bool
}

type SegmentedQueue struct {
	mu   sync.RWMutex
	cap  int
	head *syncSegment
	tail *syncSegment
}

func NewSegmentedQueue(cap int) *SegmentedQueue {
	if cap <= 0 {
		cap = mpscDefaultSegLen
	}

	q := &SegmentedQueue{cap: cap}

	seg := q.newSegment()
	q.head, q.tail = seg, seg

	return q
}

func (q *SegmentedQueue) Add(elem interface{}) {
	if q.addFast(elem) {
		return
	}

	q.addSlow(elem)
}

func (q *SegmentedQueue) Peek() interface{} {
	if q.IsEmpty() {
		return nil
	}
	if q.head.isEmpty() {
		q.head = q.head.next
	}

	return q.head.peek()
}

func (q *SegmentedQueue) RemoveAndPeek() interface{} {
	if q.IsEmpty() {
		return nil
	}
	if q.head.isEmpty() {
		q.head = q.head.next
	}

	return q.head.removeAndPeek()
}

func (q *SegmentedQueue) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if !q.head.isEmpty() {
		return false
	}

	if q.head.next != nil {
		return q.head.next.isEmpty()
	}

	return true
}

func (q *SegmentedQueue) addFast(elem interface{}) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.tail.add(elem)
}

func (q *SegmentedQueue) addSlow(elem interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tail.add(elem) {
		return
	}

	seg := q.newSegment()
	q.tail.next = seg
	q.tail = seg

	if !q.tail.add(elem) {
		panic(errors.New("new_segment_full"))
	}
}

func (q *SegmentedQueue) newSegment() *syncSegment {
	return newSegment(q.cap)
}

type UnboundedQueue struct {
	mu       sync.Mutex
	elements map[uint64]interface{}
	head     uint64
	tail     uint64
}

func NewUnboundedQueue() *UnboundedQueue {
	return &UnboundedQueue{elements: make(map[uint64]interface{}), head: 0, tail: 0}
}

func (q *UnboundedQueue) Add(element interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.tail++
	q.elements[q.tail] = element

	if q.head == 0 {
		q.head = q.tail
	}
}

func (q *UnboundedQueue) Peek() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.elements) <= 0 {
		return nil
	}

	e := q.elements[q.head]

	return e
}

func (q *UnboundedQueue) RemoveAndPeek() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.elements) <= 0 {
		return nil
	}

	delete(q.elements, q.head)
	q.head++

	if len(q.elements) <= 0 {
		return nil
	}
	e := q.elements[q.head]

	return e
}

func (q *UnboundedQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.elements) <= 0
}
