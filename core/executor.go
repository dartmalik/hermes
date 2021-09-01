package hermes

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

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

	_, err := w.tasks.Add(r)

	return err
}

func (w *worker) backlog() int {
	return w.tasks.Size()
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

type Executor struct {
	mu        sync.RWMutex
	workers   map[string]*worker
	closeOnce sync.Once
	closeCh   chan struct{}
}

func NewExecutor() (*Executor, error) {
	e := &Executor{
		workers: make(map[string]*worker),
		closeCh: make(chan struct{}, 1),
	}

	go e.pump()

	return e, nil
}

func (e *Executor) Close() {
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
}

func (e *Executor) Run(r runnable) {
	e.Submit("", r)
}

func (e *Executor) Submit(key string, r runnable) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if key == "" {
		key = uuid.NewString()
	}

	w, ok := e.workers[key]
	if !ok {
		//fmt.Printf("creating worker\n")

		w = newWorker()
		e.workers[key] = w
	}

	w.submitTask(r)
}

func (e *Executor) pump() {
	for {
		select {
		case <-time.After(1500 * time.Millisecond):
			e.removeIdleWorkers()

		case <-e.closeCh:
			return
		}
	}
}

func (e *Executor) removeIdleWorkers() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for k, w := range e.workers {
		if w.backlog() <= 0 {
			w.close()
			delete(e.workers, k)
		}
	}
}
