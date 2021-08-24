package pubsub

import (
	"errors"
	"fmt"
	"hash/maphash"
	"sync"

	"github.com/satori/uuid"
)

var ErrInterruption = errors.New("interrupted")
var ErrIllegalState = errors.New("no_capacity")

type Queue struct {
	mu       sync.Mutex
	elements map[uint64]interface{}
	head     uint64
	tail     uint64
}

func NewQueue() *Queue {
	return &Queue{elements: make(map[uint64]interface{}), head: 0, tail: 0}
}

func (q *Queue) Add(element interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.elements) > 8192 {
		return ErrIllegalState
	}

	q.tail++
	q.elements[q.tail] = element

	if q.head == 0 {
		q.head = q.tail
	}

	return nil
}

func (q *Queue) Remove() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.elements) <= 0 {
		return nil
	}

	e := q.elements[q.head]
	delete(q.elements, q.head)
	q.head++

	return e
}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.elements)
}

type runnable func()

type worker struct {
	tasks   *Queue
	taskCh  chan runnable
	closeCh chan struct{}
}

func newWorker() *worker {
	w := &worker{
		tasks:   NewQueue(),
		taskCh:  make(chan runnable, 1),
		closeCh: make(chan struct{}),
	}

	go w.run()

	return w
}

func (w *worker) submitTask(r runnable) error {
	if r == nil {
		return errors.New("invalid_runnable")
	}

	return w.tasks.Add(r)
}

func (w *worker) run() {
	for {
		select {
		case <-w.closeCh:
			return

		default:
			w.onProcess()
		}
	}
}

func (w *worker) onProcess() {
	if w.tasks.Size() <= 0 {
		return
	}

	r := w.tasks.Remove().(runnable)
	r()
}

func (w *worker) close() {
	close(w.closeCh)
}

type Scheduler struct {
	mu      sync.Mutex
	hash    maphash.Hash
	workers []worker
}

func NewScheduler(size int) (*Scheduler, error) {
	if size <= 0 {
		return nil, errors.New("invalid_pool_size")
	}

	w := make([]worker, size)
	for wi := 0; wi < size; wi++ {
		w[wi] = *newWorker()
	}

	return &Scheduler{workers: w}, nil
}

func (s *Scheduler) run(r runnable) {
	s.submit("", r)
}

func (s *Scheduler) submit(key string, r runnable) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if key == "" {
		key = uuid.NewV4().String()
	}

	s.hash.Reset()
	s.hash.WriteString(key)
	hkey := s.hash.Sum64()
	ki := hkey % uint64(len(s.workers))

	fmt.Printf("submitting to worker: %d\n", ki)

	s.workers[ki].submitTask(r)
}
