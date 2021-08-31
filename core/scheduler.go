package hermes

import (
	"errors"
	"sync"
	"time"

	"github.com/satori/uuid"
)

/*
	goals:
	- (no-locks, no-async) single threaded, synchronous execution of app code
	- (single-source-of-exec) each receiver can linearize commands by executing one command at a time
	- (location-transparency) scalability by spreading out state to a cluster
*/
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

type ReceiverID string

type Receiver func(ctx *Context, msg Message)

type ReceiverFactory func(id ReceiverID) (Receiver, error)

type Context struct {
	id   ReceiverID
	net  *Hermes
	recv Receiver
}

func newContext(id ReceiverID, net *Hermes, recv Receiver) (*Context, error) {
	if id == "" {
		return nil, errors.New("invalid_id")
	}
	if net == nil {
		return nil, errors.New("invalid_system")
	}
	if recv == nil {
		return nil, errors.New("invalid_receiver")
	}

	return &Context{id: id, net: net, recv: recv}, nil
}

func (ctx *Context) SetReceiver(recv Receiver) {
	ctx.recv = recv
}

func (ctx *Context) Join(id ReceiverID) error {
	return ctx.net.Join(id)
}

func (ctx *Context) Send(to ReceiverID, payload interface{}) error {
	return ctx.net.Send(ctx.id, to, payload)
}

func (ctx *Context) Request(to ReceiverID, request interface{}) chan Message {
	return ctx.net.Request(to, request)
}

func (ctx *Context) RequestWithTimeout(to ReceiverID, request interface{}, timeout time.Duration) (Message, error) {
	return ctx.net.RequestWithTimeout(to, request, timeout)
}

func (ctx *Context) Reply(msg Message, reply interface{}) error {
	im := msg.(*message)
	if im.to != ctx.id {
		return errors.New("not_the_recipient")
	}

	return ctx.net.reply(msg, reply)
}

//type Joined struct{}

//type Left struct{}

type Message interface {
	Payload() interface{}
}

type message struct {
	from    ReceiverID
	to      ReceiverID
	corID   string
	payload interface{}
	replyCh chan Message
}

func (m *message) isRequest() bool {
	return m.replyCh != nil
}

func (m *message) Payload() interface{} {
	return m.payload
}

var ErrMapElementAlreadyExists = errors.New("already_exists")

type syncMap struct {
	mu       sync.RWMutex
	elements map[string]interface{}
}

func newSyncMap() *syncMap {
	return &syncMap{elements: make(map[string]interface{})}
}

func (m *syncMap) put(key string, value interface{}, overwrite bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !overwrite {
		if _, ok := m.elements[key]; ok {
			return ErrMapElementAlreadyExists
		}
	}

	m.elements[key] = value

	return nil
}

func (m *syncMap) get(key string) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.elements[key]

	return v, ok
}

func (m *syncMap) delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.elements, key)
}

/*
	- delivers messages from senders to receivers
	- a request message is delivered to a receiver
	- a response message is delivered to the system (which notifies using the request channel)
	- the 'to' field is required for send a message and a request
	- the 'from' field is required when sending a reply
*/
type Hermes struct {
	//mu       sync.RWMutex
	exec     *Executor
	contexts *syncMap //map[ReceiverID]*Context
	requests *syncMap //map[string]*message
	factory  ReceiverFactory
}

func New(factory ReceiverFactory) (*Hermes, error) {
	if factory == nil {
		return nil, errors.New("invalid_factory")
	}

	e, err := NewExecutor()
	if err != nil {
		return nil, err
	}

	return &Hermes{
		exec: e,
		//contexts: make(map[ReceiverID]*Context),
		//requests: make(map[string]*message),
		contexts: newSyncMap(),
		requests: newSyncMap(),
		factory:  factory,
	}, nil
}

func (net *Hermes) Join(id ReceiverID) error {
	r, err := net.factory(id)
	if err != nil {
		return err
	}
	if r == nil {
		return errors.New("invalid_receiver_created")
	}

	err = net.doJoin(id, r)
	if err != nil {
		return err
	}

	return nil
}

func (net *Hermes) Leave(id ReceiverID) error {
	if id == "" {
		return errors.New("invalid_id")
	}

	err := net.doLeave(id)
	if err != nil {
		return err
	}

	return nil
}

func (net *Hermes) Send(from ReceiverID, to ReceiverID, payload interface{}) error {
	return net.localSend(&message{from: from, to: to, payload: payload})
}

func (net *Hermes) Request(to ReceiverID, request interface{}) chan Message {
	m := &message{to: to, corID: uuid.NewV4().String(), payload: request, replyCh: make(chan Message, 1)}

	net.localSend(m)

	return m.replyCh
}

func (net *Hermes) RequestWithTimeout(to ReceiverID, request interface{}, timeout time.Duration) (Message, error) {
	select {
	case reply := <-net.Request(to, request):
		return reply, nil

	case <-time.After(timeout):
		return nil, errors.New("request_timeout")
	}
}

func (net *Hermes) reply(msg Message, reply interface{}) error {
	im, ok := msg.(*message)
	if !ok {
		return errors.New("invalid_message")
	}

	return net.localSend(&message{from: im.to, corID: im.corID, payload: reply})
}

func (net *Hermes) localSend(msg *message) error {
	if msg.corID != "" {
		if msg.isRequest() /*&& msg.from != ""*/ {
			/*
				if _, ok := net.requests[msg.corID]; ok {
					return errors.New("request_already_exists")
				}

				net.requests[msg.corID] = msg
			*/
			net.requests.put(msg.corID, msg, false)
		} else {
			/*
				r, ok := net.requests[msg.corID]
				if ok {
					r.replyCh <- msg
					delete(net.requests, msg.corID)

					return nil
				} else {
					return errors.New("invalid_reply_correlation-id")
				}
			*/
			r, ok := net.requests.get(msg.corID)
			if ok {
				r.(*message).replyCh <- msg
				net.requests.delete(msg.corID)

				return nil
			} else {
				return errors.New("invalid_reply_correlation-id")
			}
		}
	}

	//net.mu.RLock()
	//defer net.mu.RUnlock()

	//ctx := net.contexts[msg.to]
	ctx, ok := net.contexts.get(string(msg.to))
	if !ok {
		return errors.New("unknown_receiver")
	}

	net.exec.Submit(string(msg.to), func() {
		ctx.(*Context).recv(ctx.(*Context), msg)
	})

	return nil
}

func (net *Hermes) doJoin(id ReceiverID, a Receiver) error {
	if id == "" {
		return errors.New("invalid_id")
	}

	//net.mu.Lock()
	//defer net.mu.Unlock()

	//if _, ok := net.contexts[id]; ok {
	//	return errors.New("already_joined")
	//}

	ctx, err := newContext(id, net, a)
	if err != nil {
		return err
	}

	net.contexts.put(string(id), ctx, false)
	//net.contexts[id] = ctx

	return nil
}

func (net *Hermes) doLeave(id ReceiverID) error {
	//net.mu.Lock()
	//defer net.mu.Unlock()

	//_, ok := net.contexts[id]
	//if !ok {
	//	return errors.New("unknown_receiver")
	//}

	//delete(net.contexts, id)
	_, ok := net.contexts.get(string(id))
	if !ok {
		return errors.New("unknown_receiver")
	}

	net.contexts.delete(string(id))

	return nil
}
