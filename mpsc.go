package hermes

import (
	"errors"
	"sync"
	"sync/atomic"
)

type mpscSegment struct {
	elem []interface{}
	head int32
	tail int32
	next *mpscSegment
}

func newSegment(cap int) *mpscSegment {
	return &mpscSegment{elem: make([]interface{}, cap), head: 0, tail: -1}
}

func (seg *mpscSegment) add(e interface{}) bool {
	if int(seg.tail) >= len(seg.elem) {
		return false
	}

	idx := atomic.AddInt32(&seg.tail, 1)
	if int(idx) >= len(seg.elem) {
		return false
	}

	seg.elem[idx] = e

	return true
}

func (seg *mpscSegment) peek() interface{} {
	if seg.isEmpty() {
		return nil
	}

	return seg.elem[seg.head]
}

func (seg *mpscSegment) removeAndPeek() interface{} {
	if seg.isEmpty() {
		return nil
	}
	seg.head++

	if seg.isEmpty() {
		return nil
	}

	return seg.elem[seg.head]
}

func (seg *mpscSegment) isEmpty() bool {
	if int(seg.head) >= len(seg.elem) {
		return true
	}
	if seg.head > atomic.LoadInt32(&seg.tail) {
		return true
	}

	return false
}

const (
	mpscDefaultSegLen = 1024
)

type MPSC struct {
	mu   sync.RWMutex
	cap  int
	head *mpscSegment
	tail *mpscSegment
}

func NewMPSC(cap int) *MPSC {
	if cap <= 0 {
		cap = mpscDefaultSegLen
	}

	seg := newSegment(cap)

	return &MPSC{cap: cap, head: seg, tail: seg}
}

func (q *MPSC) Add(elem interface{}) {
	if q.addFast(elem) {
		return
	}

	q.addSlow(elem)
}

func (q *MPSC) Peek() interface{} {
	if q.IsEmpty() {
		return nil
	}
	if q.head.isEmpty() {
		q.head = q.head.next
	}

	return q.head.peek()
}

func (q *MPSC) RemoveAndPeek() interface{} {
	if q.IsEmpty() {
		return nil
	}
	if q.head.isEmpty() {
		q.head = q.head.next
	}

	return q.head.removeAndPeek()
}

func (q *MPSC) IsEmpty() bool {
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

func (q *MPSC) addFast(elem interface{}) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.tail.add(elem)
}

func (q *MPSC) addSlow(elem interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tail.add(elem) {
		return
	}

	seg := newSegment(q.cap)
	q.tail.next = seg
	q.tail = seg

	if !q.tail.add(elem) {
		panic(errors.New("new_segment_full"))
	}
}
