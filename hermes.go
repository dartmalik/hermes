package hermes

import (
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MailboxIdle = iota
	MailboxProcessing
	MailboxStopped

	DefaultNumRouters = 20
	RouterIdleTimeout = 5 * 60 * time.Second
)

var (
	ErrQueueIllegalState     = errors.New("no_capacity")
	ErrContextInvalidMessage = errors.New("invalid_message")
	ErrInstanceClosed        = errors.New("hermes_instance_closed")
	ErrInvalidInstance       = errors.New("invalid_instance")
)

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
		return 8192, ErrQueueIllegalState
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

	return &Mailbox{msgs: NewQueue(), onProcess: onProcess, state: MailboxIdle}, nil
}

func (box *Mailbox) stop() bool {
	return atomic.CompareAndSwapInt32(&box.state, MailboxIdle, MailboxStopped)
}

func (box *Mailbox) post(msg Message) error {
	if msg == nil {
		return ErrContextInvalidMessage
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
		return nil, ErrContextInvalidMessage
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

type sendMessage struct {
	msg     *message
	replyCh chan error
}

type receiverIdle struct {
	id ReceiverID
}

// Hermes is an overlay network that routes messages between senders and receivers.
//
// Concepts
// ===========
// Receivers
// * Functions that receives messages from Hermes (routed from senders).
// * Receivers are uniquely identified by application specified ID (strings). This
// allows any sender to send messages to receivers via it's ID.
// * Each receiver is called in it's own goroutine on receipt of a message. This meas:
//   - Each receiver runs independently.
//	 - Each receiver can be treated as single threaded wrt it's own state.
//   - Passive receivers (that do have messages waiting in it's mailbox) dont use a goroutine.
//   - Each receiver runs processes one message at a time which allows managing contention.
//
// Receiver Factory
// * Function that is invoked by Hermes to instantiate a receiver.
// * Can be called concurrently by Hermes and therefore must be safe to call.
//
// Request Reply
// * Implements the request reply pattern via two messages, one from requester to requestee and back.
// * Returns a reply channel that can be used to wait on the requestee to respond. This allows go idomatic code to be written.
// * This cal cause deadlocks if there is a cyclical request dependency between multiple receivers.
// * Can use the RequestWithTimeout function to prevent deadlocks in production and log such errors for fixing these.
//
// Why Hermes
// ============
// * Hermes manages the lifecycle of receivers i.e. receivers are instantiated (via
// the provided receiver factory), activated on message receipt and deactivated
// when idle. Closing hermes will also deactivates all receivers.
// * The number of goroutines is limited to the number active receivers not the total receivers in the system.
// * Receivers can be tested independently as receivers communicate only via messages and dependent receivers can be mocked.
// * Receivers can set other functions as receivers while processing messages. This allows entities to be in different states.
// * Receivers execute one messages at a time which can reduce contention in some domains (collaborative domains).
// * Receivers can be backed by durable state (from a database for instance). This allows state to be cached and allows the system to scale.
// * Since state is cached on the application, low-latency workload can be supported.
// * Supports timed message delivery for handling things like heartbeats, timeouts, etc
// * (planned) Supports a single abstraction for support both concurrent and distributed applications.
//
type Hermes struct {
	reqID      uint64
	factory    ReceiverFactory
	reqs       *SyncMap
	ctx        map[string]*context
	idleTimers map[string]*time.Timer
	seed       maphash.Seed
	cmds       *Mailbox
	closed     uint32
}

func New(factory ReceiverFactory) (*Hermes, error) {
	if factory == nil {
		return nil, errors.New("invalid_factory")
	}

	net := &Hermes{
		reqs:       NewSyncMap(),
		factory:    factory,
		seed:       maphash.MakeSeed(),
		ctx:        make(map[string]*context),
		idleTimers: make(map[string]*time.Timer),
	}

	cmds, err := newMailbox(net.onCmd)
	if err != nil {
		return nil, err
	}
	net.cmds = cmds

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
func (net *Hermes) RequestWithTimeout(
	from, to ReceiverID,
	request interface{},
	timeout time.Duration) (Message, error) {
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
	//r := net.router(string(msg.to))
	//return net.send(msg)

	sm := &sendMessage{msg: msg, replyCh: make(chan error, 1)}
	err := net.cmds.post(&message{payload: sm})
	if err != nil {
		return err
	}

	return <-sm.replyCh
}

func (net *Hermes) onCmd(msg Message) {
	switch msg.Payload().(type) {
	case *sendMessage:
		sm := msg.Payload().(*sendMessage)
		sm.replyCh <- net.onSend(sm.msg)

	case *receiverIdle:
		ri := msg.Payload().(*receiverIdle)
		net.onIdleReceiver(ri.id)
	}
}

func (net *Hermes) onIdleReceiver(id ReceiverID) {
	ctx, ok := net.ctx[string(id)]
	if !ok {
		return
	}

	if ctx.stop() {
		delete(net.ctx, string(id))
	}
}

func (net *Hermes) onSend(msg *message) error {
	ctx, err := net.context(msg.to)
	if err != nil {
		return err
	}

	tm := net.idleTimers[string(msg.to)]
	tm.Reset(RouterIdleTimeout)

	return ctx.submit(msg)
}

func (net *Hermes) context(id ReceiverID) (*context, error) {
	ctx, ok := net.ctx[string(id)]
	if ok {
		return ctx, nil
	}

	recv, err := net.factory(id)
	if err != nil {
		return nil, err
	}

	ctx, err = newContext(id, net, recv)
	if err != nil {
		return nil, err
	}

	net.ctx[string(id)] = ctx
	ctx.submit(&message{to: ctx.id, payload: &Joined{}})

	t := time.AfterFunc(RouterIdleTimeout, func() {
		net.onIdleTimeout(ctx)
	})
	net.idleTimers[string(id)] = t

	return ctx, nil
}

func (net *Hermes) onIdleTimeout(ctx *context) {
	net.cmds.post(&message{payload: &receiverIdle{id: ctx.id}})
}

func (net *Hermes) nextReqID() uint64 {
	return atomic.AddUint64(&net.reqID, 1)
}

func (net *Hermes) isClosed() bool {
	return atomic.LoadUint32(&net.closed) == 1
}
