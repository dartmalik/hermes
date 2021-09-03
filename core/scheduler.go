package hermes

import (
	"errors"
	"fmt"
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
	mailbox *Queue
	state   int32
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

	return &Context{state: ContextIdle, id: id, net: net, recv: recv, mailbox: NewQueue()}, nil
}

func (ctx *Context) Stop() {
	atomic.StoreInt32(&ctx.state, ContextStopped)
}

func (ctx *Context) SetReceiver(recv Receiver) {
	ctx.recv = recv
}

func (ctx *Context) Send(to ReceiverID, payload interface{}) error {
	return ctx.net.Send(ctx.id, to, payload)
}

func (ctx *Context) Request(to ReceiverID, request interface{}) (chan Message, error) {
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

func (ctx *Context) submit(msg Message) error {
	if msg == nil {
		return ErrInvalidMessage
	}

	_, err := ctx.mailbox.Add(msg)
	if err != nil {
		return err
	}

	if atomic.CompareAndSwapInt32(&ctx.state, ContextIdle, ContextProcessing) {
		go ctx.process()
	}

	return nil
}

func (ctx *Context) process() {
	ctx.processMessages()

	for ctx.mailbox.Size() > 0 &&
		atomic.CompareAndSwapInt32(&ctx.state, ContextIdle, ContextProcessing) {
		ctx.processMessages()
	}
}

func (ctx *Context) processMessages() {
	for e := ctx.mailbox.Peek(); e != nil; {
		ctx.recv(ctx, e.(Message))
		e = ctx.mailbox.RemoveAndPeek()
	}
	atomic.CompareAndSwapInt32(&ctx.state, ContextProcessing, ContextIdle)
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

func (m *message) isRequest() bool {
	return m.replyCh != nil
}

func (m *message) Payload() interface{} {
	return m.payload
}

/*
	- delivers messages from senders to receivers
	- a request message is delivered to a receiver
	- a response message is delivered to the system (which notifies using the request channel)
	- the 'to' field is required for send a message and a request
	- the 'from' field is required when sending a reply
*/
type Hermes struct {
	contexts *syncMap
	requests *syncMap
	reqID    uint64
	factory  ReceiverFactory
}

func New(factory ReceiverFactory) (*Hermes, error) {
	if factory == nil {
		return nil, errors.New("invalid_factory")
	}

	return &Hermes{
		contexts: newSyncMap(),
		requests: newSyncMap(),
		factory:  factory,
	}, nil
}

func (net *Hermes) Send(from ReceiverID, to ReceiverID, payload interface{}) error {
	return net.localSend(&message{from: from, to: to, payload: payload})
}

func (net *Hermes) Request(to ReceiverID, request interface{}) (chan Message, error) {
	m := &message{to: to, corID: fmt.Sprintf("%d", net.nextReqID()), payload: request, replyCh: make(chan Message, 1)}

	err := net.localSend(m)
	if err != nil {
		return nil, err
	}

	return m.replyCh, nil
}

func (net *Hermes) RequestWithTimeout(to ReceiverID, request interface{}, timeout time.Duration) (Message, error) {
	replyCh, err := net.Request(to, request)
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

	return net.localSend(&message{from: im.to, corID: im.corID, payload: reply})
}

func (net *Hermes) localSend(msg *message) error {
	if msg.corID != "" {
		if msg.isRequest() /*&& msg.from != ""*/ {
			net.requests.put(msg.corID, msg, false)
		} else {
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

	ctx, err := net.context(msg.to)
	if err != nil {
		return err
	}

	ctx.submit(msg)

	return nil
}

func (net *Hermes) context(id ReceiverID) (*Context, error) {
	elem, ok := net.contexts.get(string(id))
	if ok {
		return elem.(*Context), nil
	}

	recv, err := net.factory(id)
	if err != nil {
		return nil, err
	}

	ctx, err := newContext(id, net, recv)
	if err != nil {
		return nil, err
	}

	err = net.contexts.putAndInit(string(id), ctx, false, func() {
		ctx.submit(&message{to: id, payload: &Joined{}})
	})
	if err != nil {
		elem, _ = net.contexts.get(string(id))

		return elem.(*Context), nil
	}

	return ctx, nil
}

func (net *Hermes) nextReqID() uint64 {
	return atomic.AddUint64(&net.reqID, 1)
}
