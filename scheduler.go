package pubsub

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/satori/uuid"
)

/*
	goals:
	- (no-locks, no-async) single threaded, synchronous execution of app code
	- (single-source-of-exec) each actor can linearize commands by executing one command at a time
	- (location-transparency) scalability by spreading out state to a cluster
*/

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
		key = uuid.NewV4().String()
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

type ActorID string

type Actor interface {
	Receive(sys *ActorSystem, msg ActorMessage)
}

type ActorMessage interface {
	Payload() interface{}
}

type actorMessage struct {
	from    ActorID
	to      ActorID
	corID   string
	payload interface{}
	replyCh chan ActorMessage
}

func (m *actorMessage) isRequest() bool {
	return m.replyCh != nil
}

func (m *actorMessage) Payload() interface{} {
	return m.payload
}

type ActorSystem struct {
	mu       sync.Mutex
	exec     *Executor
	actors   map[ActorID]Actor
	requests map[string]*actorMessage
}

func NewActorSystem() (*ActorSystem, error) {
	e, err := NewExecutor()
	if err != nil {
		return nil, err
	}

	return &ActorSystem{
		exec:     e,
		actors:   make(map[ActorID]Actor),
		requests: make(map[string]*actorMessage),
	}, nil
}

func (sys *ActorSystem) Register(key ActorID, a Actor) error {
	if key == "" {
		return errors.New("invalid_actor_key")
	}

	sys.mu.Lock()
	defer sys.mu.Unlock()

	if _, ok := sys.actors[key]; ok {
		fmt.Printf("[WARN] actor already registered")
	}

	sys.actors[key] = a

	return nil
}

func (sys *ActorSystem) Send(from ActorID, to ActorID, payload interface{}) error {
	return sys.localSend(&actorMessage{from: from, to: to, payload: payload})
}

func (sys *ActorSystem) Request(from ActorID, to ActorID, request interface{}) chan ActorMessage {
	m := &actorMessage{from: from, to: to, corID: uuid.NewV4().String(), payload: request, replyCh: make(chan ActorMessage)}

	sys.localSend(m)

	return m.replyCh
}

func (sys *ActorSystem) RequestWithTimeout(from ActorID, to ActorID, request interface{}, timeout time.Duration) (ActorMessage, error) {
	select {
	case reply := <-sys.Request(from, to, request):
		return reply, nil

	case <-time.After(timeout):
		return nil, errors.New("request_timeout")
	}
}

func (sys *ActorSystem) Reply(msg ActorMessage, reply interface{}) error {
	am, ok := msg.(*actorMessage)
	if !ok {
		return errors.New("invalid_message")
	}

	return sys.localSend(&actorMessage{from: am.to, to: am.from, corID: am.corID, payload: reply})
}

func (sys *ActorSystem) localSend(msg *actorMessage) error {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	a := sys.actors[msg.to]
	if a == nil {
		return errors.New("unregistered_actor")
	}

	if msg.corID != "" {
		if msg.isRequest() {
			if _, ok := sys.requests[msg.corID]; ok {
				return errors.New("request_already_exists")
			}

			sys.requests[msg.corID] = msg
		} else {
			r, ok := sys.requests[msg.corID]
			if ok {
				r.replyCh <- msg
				delete(sys.requests, msg.corID)

				return nil
			} else {
				return errors.New("invalid_reply_correlation-id")
			}
		}
	}

	sys.exec.Submit(string(msg.to), func() {
		a.Receive(sys, msg)
	})

	return nil
}
