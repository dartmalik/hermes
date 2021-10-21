package hermes

import (
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

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

const (
	MailboxIdle = iota
	MailboxProcessing
	MailboxStopped
)

type Mailbox struct {
	msgs      *Queue
	onProcess func(Message)
	state     int32
}

func newMailbox(onProcess func(Message)) (*Mailbox, error) {
	if onProcess == nil {
		return nil, errors.New("invalid_process_cb")
	}

	return &Mailbox{msgs: NewQueue(), onProcess: onProcess, state: MailboxIdle}, nil
}

func (box *Mailbox) stop() bool {
	return atomic.CompareAndSwapInt32(&box.state, MailboxIdle, MailboxStopped)
}

func (box *Mailbox) post(msg Message) error {
	if msg == nil {
		return ErrInvalidMessage
	}

	_, err := box.msgs.Add(msg)
	if err != nil {
		return err
	}

	if atomic.CompareAndSwapInt32(&box.state, MailboxIdle, MailboxProcessing) {
		go box.process()
	}

	return nil
}

func (box *Mailbox) len() int {
	return box.msgs.Size()
}

func (box *Mailbox) process() {
	box.processMessages()

	for box.msgs.Size() > 0 &&
		atomic.CompareAndSwapInt32(&box.state, MailboxIdle, MailboxProcessing) {
		box.processMessages()
	}
}

func (box *Mailbox) processMessages() {
	for e := box.msgs.Peek(); e != nil; {
		box.onProcess(e.(Message))
		e = box.msgs.RemoveAndPeek()
	}
	atomic.CompareAndSwapInt32(&box.state, MailboxProcessing, MailboxIdle)
}

var ErrInvalidMessage = errors.New("invalid_message")

type ReceiverID string

type Receiver func(ctx Context, msg Message)

type ReceiverFactory func(id ReceiverID) (Receiver, error)

type Timer interface {
	Reset(time.Duration) bool
	Stop() bool
}

type Context interface {
	ID() ReceiverID
	SetReceiver(Receiver)
	Send(to ReceiverID, payload interface{}) error
	Request(to ReceiverID, request interface{}) (chan Message, error)
	RequestWithTimeout(to ReceiverID, request interface{}, timeout time.Duration) (Message, error)
	Reply(msg Message, reply interface{}) error
	Schedule(after time.Duration, msg interface{}) (Timer, error)
}

type context struct {
	id      ReceiverID
	net     *Hermes
	recv    Receiver
	mailbox *Mailbox
}

func newContext(id ReceiverID, net *Hermes, recv Receiver) (*context, error) {
	if id == "" {
		return nil, errors.New("invalid_id")
	}
	if net == nil {
		return nil, errors.New("invalid_system")
	}
	if recv == nil {
		return nil, errors.New("invalid_receiver")
	}

	ctx := &context{id: id, net: net, recv: recv}

	mb, err := newMailbox(ctx.onProcess)
	if err != nil {
		return nil, err
	}
	ctx.mailbox = mb

	return ctx, nil
}

func (ctx *context) ID() ReceiverID {
	return ctx.id
}

func (ctx *context) SetReceiver(recv Receiver) {
	ctx.recv = recv
}

func (ctx *context) Send(to ReceiverID, payload interface{}) error {
	return ctx.net.Send(ctx.id, to, payload)
}

func (ctx *context) Request(to ReceiverID, request interface{}) (chan Message, error) {
	return ctx.net.Request(ctx.id, to, request)
}

func (ctx *context) RequestWithTimeout(to ReceiverID, request interface{}, timeout time.Duration) (Message, error) {
	return ctx.net.RequestWithTimeout(ctx.id, to, request, timeout)
}

func (ctx *context) Reply(msg Message, reply interface{}) error {
	im := msg.(*message)
	if im.to != ctx.id {
		return errors.New("not_the_recipient")
	}

	return ctx.net.reply(msg, reply)
}

func (ctx *context) Schedule(after time.Duration, msg interface{}) (Timer, error) {
	if msg == nil {
		return nil, ErrInvalidMessage
	}

	t := time.AfterFunc(after, func() {
		ctx.submit(&message{to: ctx.id, payload: msg})
	})

	return t, nil
}

func (ctx *context) stop() bool {
	return ctx.mailbox.stop()
}

func (ctx *context) onProcess(msg Message) {
	ctx.recv(ctx, msg)
}

func (ctx *context) submit(msg Message) error {
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
	DefaultIdleTimeout = 5 * 60 * time.Second
)

var (
	ErrInstanceClosed  = errors.New("hermes_instance_closed")
	ErrInvalidInstance = errors.New("invalid_instance")
)

type Hermes struct {
	reqID   uint64
	factory ReceiverFactory
	routers []*Router
	reqs    *SyncMap
	seed    maphash.Seed
	closed  uint32
}

func New(factory ReceiverFactory) (*Hermes, error) {
	if factory == nil {
		return nil, errors.New("invalid_factory")
	}

	net := &Hermes{
		routers: make([]*Router, DefaultRouterCount),
		reqs:    NewSyncMap(),
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

// Close closes the hermes instance. This is a blocking call and will
// wait on all the routers to be empitied of work.
func Close(npp **Hermes) error {
	if npp == nil || *npp == nil {
		return ErrInvalidInstance
	}

	net := *npp
	if !atomic.CompareAndSwapUint32(&net.closed, 0, 1) {
		return ErrInstanceClosed
	}

	for _, r := range net.routers {
		for r.len() > 0 {
		}
	}

	*npp = nil

	return nil
}

// Send sends the specified message payload to the specified receiver
func (net *Hermes) Send(from ReceiverID, to ReceiverID, payload interface{}) error {
	if net.isClosed() {
		return ErrInstanceClosed
	}

	return net.localSend(&message{from: from, to: to, payload: payload})
}

// Request implements a Request-Reply pattern by exchaning messages between the sender and receiver
// The returned channel can be used to wait on the reply.
func (net *Hermes) Request(from, to ReceiverID, request interface{}) (chan Message, error) {
	if net.isClosed() {
		return nil, ErrInstanceClosed
	}

	m := &message{
		from:    from,
		to:      to,
		corID:   fmt.Sprintf("%d", net.nextReqID()),
		payload: request,
		replyCh: make(chan Message, 1),
	}

	err := net.reqs.Put(m.corID, m, false)
	if err != nil {
		return nil, err
	}

	err = net.localSend(m)
	if err != nil {
		net.reqs.Delete(m.corID)
		return nil, err
	}

	return m.replyCh, nil
}

// RequestWithTimeout wraps the Request function and waits on the reply channel for the specified
// time. This is helpful in production to avoid being deadlocked. Sending a large timeout is equivalent
// to wating on a blocking channel.
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

	req, ok := net.reqs.Get(im.corID)
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

func (net *Hermes) isClosed() bool {
	return atomic.LoadUint32(&net.closed) == 1
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
	ctx        map[string]*context
	idleTimers map[string]*time.Timer
	cmds       *Mailbox
}

func newRouter(net *Hermes, factory ReceiverFactory) (*Router, error) {
	r := &Router{
		net:        net,
		factory:    factory,
		ctx:        make(map[string]*context),
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

func (r *Router) len() int {
	return r.cmds.len()
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

	if ctx.stop() {
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

func (r *Router) context(id ReceiverID) (*context, error) {
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

func (r *Router) onIdleTimeout(ctx *context) {
	r.cmds.post(&message{payload: &ReceiverIdle{id: ctx.id}})
}
