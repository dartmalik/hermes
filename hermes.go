package hermes

import (
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
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

func (q *Queue) Add(element interface{}) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.elements) >= 8192 {
		return 8192, ErrIllegalState
	}

	q.tail++
	q.elements[q.tail] = element

	if q.head == 0 {
		q.head = q.tail
	}

	return len(q.elements), nil
}

func (q *Queue) Peek() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.elements) <= 0 {
		return nil
	}

	e := q.elements[q.head]

	return e
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

func (q *Queue) RemoveAndPeek() interface{} {
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

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.elements)
}

type Mailbox struct {
	msgs      *Queue
	onProcess func(Message)
	state     int32
}

func newMailbox(onProcess func(Message)) (*Mailbox, error) {
	if onProcess == nil {
		return nil, errors.New("invalid_process_cb")
	}

	return &Mailbox{msgs: NewQueue(), onProcess: onProcess, state: ContextIdle}, nil
}

func (box *Mailbox) stop() bool {
	return atomic.CompareAndSwapInt32(&box.state, ContextIdle, ContextStopped)
}

func (box *Mailbox) post(msg Message) error {
	if msg == nil {
		return ErrInvalidMessage
	}

	_, err := box.msgs.Add(msg)
	if err != nil {
		return err
	}

	if atomic.CompareAndSwapInt32(&box.state, ContextIdle, ContextProcessing) {
		go box.process()
	}

	return nil
}

func (box *Mailbox) process() {
	box.processMessages()

	for box.msgs.Size() > 0 &&
		atomic.CompareAndSwapInt32(&box.state, ContextIdle, ContextProcessing) {
		box.processMessages()
	}
}

func (box *Mailbox) processMessages() {
	for e := box.msgs.Peek(); e != nil; {
		box.onProcess(e.(Message))
		e = box.msgs.RemoveAndPeek()
	}
	atomic.CompareAndSwapInt32(&box.state, ContextProcessing, ContextIdle)
}

var ErrInvalidMessage = errors.New("invalid_message")

type ReceiverID string

type Receiver func(ctx *Context, msg Message)

type ReceiverFactory func(id ReceiverID) (Receiver, error)

const (
	ContextStopped = iota
	ContextIdle
	ContextProcessing
)

type Context struct {
	id      ReceiverID
	net     *Hermes
	recv    Receiver
	mailbox *Mailbox
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

	ctx := &Context{id: id, net: net, recv: recv}

	mb, err := newMailbox(ctx.onProcess)
	if err != nil {
		return nil, err
	}
	ctx.mailbox = mb

	return ctx, nil
}

func (ctx *Context) onProcess(msg Message) {
	ctx.recv(ctx, msg)
}

func (ctx *Context) Stop() bool {
	return ctx.mailbox.stop()
}

func (ctx *Context) SetReceiver(recv Receiver) {
	ctx.recv = recv
}

func (ctx *Context) Send(to ReceiverID, payload interface{}) error {
	return ctx.net.Send(ctx.id, to, payload)
}

func (ctx *Context) Request(to ReceiverID, request interface{}) (chan Message, error) {
	return ctx.net.Request(ctx.id, to, request)
}

func (ctx *Context) RequestWithTimeout(to ReceiverID, request interface{}, timeout time.Duration) (Message, error) {
	return ctx.net.RequestWithTimeout(ctx.id, to, request, timeout)
}

func (ctx *Context) Reply(msg Message, reply interface{}) error {
	im := msg.(*message)
	if im.to != ctx.id {
		return errors.New("not_the_recipient")
	}

	return ctx.net.reply(msg, reply)
}

func (ctx *Context) submit(msg Message) error {
	return ctx.mailbox.post(msg)
}

type Joined struct{}

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

func (m *message) Payload() interface{} {
	return m.payload
}

const (
	DefaultRouterCount = 20
	DefaultIdleTimeout = 5 * time.Second
)

/*
	- delivers messages from senders to receivers
	- a request message is delivered to a receiver
	- a response message is delivered to the system (which notifies using the request channel)
	- the 'to' field is required for send a message and a request
	- the 'from' field is required when sending a reply
*/
type Hermes struct {
	reqID   uint64
	factory ReceiverFactory
	routers []*Router
	reqs    *syncMap
	seed    maphash.Seed
}

func New(factory ReceiverFactory) (*Hermes, error) {
	if factory == nil {
		return nil, errors.New("invalid_factory")
	}

	net := &Hermes{
		routers: make([]*Router, DefaultRouterCount),
		reqs:    newSyncMap(),
		factory: factory,
		seed:    maphash.MakeSeed(),
	}

	for ri := 0; ri < DefaultRouterCount; ri++ {
		r, err := newRouter(net, factory)
		if err != nil {
			return nil, err
		}

		net.routers[ri] = r
	}

	return net, nil
}

func (net *Hermes) Send(from ReceiverID, to ReceiverID, payload interface{}) error {
	return net.localSend(&message{from: from, to: to, payload: payload})
}

func (net *Hermes) Request(from, to ReceiverID, request interface{}) (chan Message, error) {
	m := &message{
		from:    from,
		to:      to,
		corID:   fmt.Sprintf("%d", net.nextReqID()),
		payload: request,
		replyCh: make(chan Message, 1),
	}

	err := net.reqs.put(m.corID, m, false)
	if err != nil {
		return nil, err
	}

	err = net.localSend(m)
	if err != nil {
		net.reqs.delete(m.corID)
		return nil, err
	}

	return m.replyCh, nil
}

func (net *Hermes) RequestWithTimeout(from, to ReceiverID, request interface{}, timeout time.Duration) (Message, error) {
	replyCh, err := net.Request(from, to, request)
	if err != nil {
		return nil, err
	}

	select {
	case reply := <-replyCh:
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

	req, ok := net.reqs.get(im.corID)
	if !ok {
		return errors.New("invalid_cor-id")
	}

	rm := req.(*message)
	rm.replyCh <- &message{from: im.to, to: im.from, corID: im.corID, payload: reply}

	return nil
}

func (net *Hermes) localSend(msg *message) error {
	r := net.router(string(msg.to))
	return r.send(msg)
}

func (net *Hermes) router(key string) *Router {
	var hash maphash.Hash

	hash.SetSeed(net.seed)
	hash.WriteString(key)
	ri := int(hash.Sum64() % DefaultRouterCount)

	return net.routers[ri]
}

func (net *Hermes) nextReqID() uint64 {
	return atomic.AddUint64(&net.reqID, 1)
}

type SendMessage struct {
	msg     *message
	replyCh chan error
}

type ReceiverIdle struct {
	id ReceiverID
}

type Router struct {
	factory    ReceiverFactory
	net        *Hermes
	ctx        map[string]*Context
	idleTimers map[string]*time.Timer
	cmds       *Mailbox
}

func newRouter(net *Hermes, factory ReceiverFactory) (*Router, error) {
	r := &Router{
		net:        net,
		factory:    factory,
		ctx:        make(map[string]*Context),
		idleTimers: make(map[string]*time.Timer),
	}

	box, err := newMailbox(r.onMessage)
	if err != nil {
		return nil, err
	}

	r.cmds = box

	return r, nil
}

func (r *Router) send(msg *message) error {
	sm := &SendMessage{msg: msg, replyCh: make(chan error, 1)}
	err := r.cmds.post(&message{payload: sm})
	if err != nil {
		return err
	}

	return <-sm.replyCh
}

func (r *Router) onMessage(msg Message) {
	switch msg.Payload().(type) {
	case *SendMessage:
		sm := msg.Payload().(*SendMessage)
		sm.replyCh <- r.onSend(sm.msg)

	case *ReceiverIdle:
		ri := msg.Payload().(*ReceiverIdle)
		r.onIdleReceiver(ri.id)
	}
}

func (r *Router) onIdleReceiver(id ReceiverID) {
	ctx, ok := r.ctx[string(id)]
	if !ok {
		return
	}

	if ctx.Stop() {
		delete(r.ctx, string(id))
	}
}

func (r *Router) onSend(msg *message) error {
	ctx, err := r.context(msg.to)
	if err != nil {
		return err
	}

	tm := r.idleTimers[string(msg.to)]
	tm.Reset(DefaultIdleTimeout)

	return ctx.submit(msg)
}

func (r *Router) context(id ReceiverID) (*Context, error) {
	ctx, ok := r.ctx[string(id)]
	if ok {
		return ctx, nil
	}

	recv, err := r.factory(id)
	if err != nil {
		return nil, err
	}

	ctx, err = newContext(id, r.net, recv)
	if err != nil {
		return nil, err
	}

	r.ctx[string(id)] = ctx
	ctx.submit(&message{to: ctx.id, payload: &Joined{}})

	t := time.AfterFunc(DefaultIdleTimeout, func() {
		r.onIdleTimeout(ctx)
	})
	r.idleTimers[string(id)] = t

	return ctx, nil
}

func (r *Router) onIdleTimeout(ctx *Context) {
	r.cmds.post(&message{payload: &ReceiverIdle{id: ctx.id}})
}
