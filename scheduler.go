package pubsub

import (
	"errors"
	"hash/maphash"
	"sync"

	"github.com/satori/uuid"
)

var InterruptionError = errors.New("interrupted")
var IllegalStateError = errors.New("no_capacity")

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
		return IllegalStateError
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

	select {
	case w.taskCh <- r:

	case <-w.closeCh:
		return InterruptionError
	}

	return nil
}

func (w *worker) run() {
	for {
		select {
		case r := <-w.taskCh:
			w.onSubmitTask(r)

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

func (w *worker) onSubmitTask(r runnable) error {
	return w.tasks.Add(r)
}

func (w *worker) close() {
	close(w.closeCh)
}

// 1. Clients can submit tasks that are queued
// 2. Tasks are assigned to idle workers
// 3. A task is a
type Scheduler struct {
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
	if key == "" {
		key = uuid.NewV4().String()
	}

	var h maphash.Hash
	h.WriteString(key)
	hkey := h.Sum64()
	ki := hkey % uint64(len(s.workers))

	s.workers[ki].submitTask(r)
}
