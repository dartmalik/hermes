package hermes

import (
	"errors"
	"fmt"
	"hash/maphash"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	IdleTimeout = 5 * 60 * time.Second

	MailboxIdle = iota
	MailboxProcessing
	MailboxStopped
)

var (
	ErrInvalidInstance   = errors.New("invalid_instance")
	ErrInvalidMsgTo      = errors.New("invalid_msg_to")
	ErrInvalidMsgFrom    = errors.New("invalid_msg_from")
	ErrInvalidMsgPayload = errors.New("invalid_msg_payload")
	ErrInvalidRecvID     = errors.New("invalid_recv_id")

	ErrMailboxProcessCB = errors.New("invalid_process_cb")

	ErrContextMsg            = errors.New("invalid_message")
	ErrContextStopped        = errors.New("context_stopped")
	ErrContextId             = errors.New("invalid_id")
	ErrContextSys            = errors.New("invalid_system")
	ErrContextRecv           = errors.New("invalid_receiver")
	ErrContextIdleCB         = errors.New("invalid_idle_callback")
	ErrContextRequestTimeout = errors.New("request_timeout")
)

type MailboxProcessCB func(interface{})

// Mailbox maintains a queue for messages to be consumed by a consumer (process callback).
// It ensures that only a single consumer is processing the queue at one time by managing
// the state of the mailbox. The consuming function is called in goroutine.
// The mailbox methods can be called from multiple goroutines.
//
// A mailbox can be in one of three states: idle, processing, stopped.
// Idle
// The mailbox is empty and not being processed.
// Processing
// The mailbox is not empty and currently the processing goroutine it running. processMsgs
// is only run while the state is set to processing.
// Stopped
// The mailbox is empty and will no longer be processed. A mailbox can only be stopped if
// it is idle. Stop does not suspend the processing of messages.
type Mailbox struct {
	msgs      Queue
	onProcess MailboxProcessCB
	state     int32
}

func newMailbox(segCap int, cb MailboxProcessCB) (*Mailbox, error) {
	if cb == nil {
		return nil, ErrMailboxProcessCB
	}

	return &Mailbox{msgs: NewSegmentedQueue(segCap), onProcess: cb, state: MailboxIdle}, nil
}

// stop tries to transition the mailbox to a stopped state
// returns true if the mailbox state is now stopped
func (box *Mailbox) stop() bool {
	if atomic.CompareAndSwapInt32(&box.state, MailboxIdle, MailboxStopped) { // flip from idle to stopped
		return true
	}

	// if the flip failed, check if the current state is stopped.
	// will be false if current state if processing
	return atomic.LoadInt32(&box.state) == MailboxStopped
}

func (box *Mailbox) post(msg interface{}) error {
	if msg == nil {
		return ErrContextMsg
	}
	if atomic.LoadInt32(&box.state) == MailboxStopped {
		return ErrContextStopped
	}

	box.msgs.Add(msg)

	if atomic.CompareAndSwapInt32(&box.state, MailboxIdle, MailboxProcessing) {
		go box.process()
	}

	return nil
}

func (box *Mailbox) process() {
	box.processMsgs()

	for !box.msgs.IsEmpty() &&
		atomic.CompareAndSwapInt32(&box.state, MailboxIdle, MailboxProcessing) {
		box.processMsgs()
	}
}

func (box *Mailbox) processMsgs() {
	for e := box.msgs.Peek(); e != nil; {
		box.onProcess(e)
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
	Request(to ReceiverID, request interface{}) (<-chan Message, error)
	RequestWithTimeout(to ReceiverID, payload interface{}, timeout time.Duration) (Message, error)
	Reply(msg Message, reply interface{}) error
	Schedule(after time.Duration, msg interface{}) (Timer, error)
}

type context struct {
	id      ReceiverID
	net     *Hermes
	recv    Receiver
	mailbox *Mailbox
	idleDur time.Duration
	idleT   *time.Timer

	reqsLock sync.Mutex
	curReqID int
	reqs     map[string]chan Message
}

func newContext(id ReceiverID, net *Hermes, recv Receiver, idleDur time.Duration, onIdle func()) (*context, error) {
	if id == "" {
		return nil, ErrContextId
	}
	if net == nil {
		return nil, ErrContextSys
	}
	if recv == nil {
		return nil, ErrContextRecv
	}
	if onIdle == nil {
		return nil, ErrContextIdleCB
	}

	ctx := &context{
		id:      id,
		net:     net,
		recv:    recv,
		idleDur: idleDur,
		reqs:    make(map[string]chan Message),
	}

	mb, err := newMailbox(64, ctx.onProcess)
	if err != nil {
		return nil, err
	}
	ctx.mailbox = mb

	ctx.idleT = time.AfterFunc(idleDur, onIdle)

	return ctx, nil
}

func (ctx *context) ID() ReceiverID {
	return ctx.id
}

func (ctx *context) SetReceiver(recv Receiver) {
	ctx.recv = recv
}

func (ctx *context) Send(to ReceiverID, payload interface{}) error {
	ctx.idleT.Reset(ctx.idleDur)

	return ctx.net.Send(ctx.id, to, payload)
}

func (ctx *context) Request(to ReceiverID, payload interface{}) (<-chan Message, error) {
	ctx.idleT.Reset(ctx.idleDur)

	rid, replyCh := ctx.newReq()

	err := ctx.net.request(ctx.id, to, payload, rid)
	if err != nil {
		ctx.deleteReq(rid)
		return nil, err
	}

	return replyCh, nil
}

func (ctx *context) RequestWithTimeout(to ReceiverID, payload interface{}, timeout time.Duration) (Message, error) {
	replyCh, err := ctx.Request(to, payload)
	if err != nil {
		return nil, err
	}

	select {
	case reply := <-replyCh:
		return reply, nil

	case <-time.After(timeout):
		return nil, ErrContextRequestTimeout
	}
}

func (ctx *context) Reply(mi Message, reply interface{}) error {
	ctx.idleT.Reset(ctx.idleDur)

	msg := mi.(*message)
	if msg.to != ctx.id {
		return errors.New("not_the_recipient")
	}

	return ctx.net.reply(mi, reply)
}

func (ctx *context) Schedule(after time.Duration, msg interface{}) (Timer, error) {
	ctx.idleT.Reset(ctx.idleDur)

	if msg == nil {
		return nil, ErrContextMsg
	}

	t := time.AfterFunc(after, func() {
		ctx.submit(&message{to: ctx.id, payload: msg})
	})

	return t, nil
}

func (ctx *context) stop() bool {
	return ctx.mailbox.stop()
}

func (ctx *context) submit(mi Message) error {
	ctx.idleT.Reset(ctx.idleDur)

	msg := mi.(*message)
	if msg.replyTo == ctx.id {
		replyCh := ctx.deleteReq(msg.reqID)
		if replyCh != nil {
			replyCh <- mi
		} else {
			fmt.Printf("[WARN] invaid reqID: %s\n", msg.reqID)
		}

		return nil
	}

	return ctx.mailbox.post(mi)
}

func (ctx *context) onProcess(msg interface{}) {
	ctx.recv(ctx, msg.(Message))
}

func (ctx *context) newReq() (string, chan Message) {
	ctx.reqsLock.Lock()
	defer ctx.reqsLock.Unlock()

	ctx.curReqID++
	rid := strconv.Itoa(ctx.curReqID)

	replyCh := make(chan Message, 1)
	ctx.reqs[rid] = replyCh

	return rid, replyCh
}

func (ctx *context) deleteReq(cid string) chan Message {
	ctx.reqsLock.Lock()
	defer ctx.reqsLock.Unlock()

	ch, ok := ctx.reqs[cid]
	if !ok {
		return nil
	}
	delete(ctx.reqs, cid)

	return ch
}

type Joined struct{}

type Message interface {
	Payload() interface{}
}

type message struct {
	from    ReceiverID
	to      ReceiverID
	reqID   string
	replyTo ReceiverID
	payload interface{}
}

func (m *message) Payload() interface{} {
	return m.payload
}

type Opts struct {
	RF      ReceiverFactory
	IdleDur time.Duration
}

func NewOpts() *Opts {
	return &Opts{IdleDur: IdleTimeout}
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
	shs  []*scheduler
	seed maphash.Seed
}

func New(opts *Opts) (*Hermes, error) {
	if opts == nil {
		return nil, errors.New("invalid_options")
	}
	if opts.RF == nil {
		return nil, errors.New("invalid_factory")
	}

	net := &Hermes{
		seed: maphash.MakeSeed(),
	}

	numSh := runtime.NumCPU() * 2
	shs := make([]*scheduler, numSh)
	for si := 0; si < numSh; si++ {
		sh, err := newScheduler(0, net, opts.RF, opts.IdleDur)
		if err != nil {
			return nil, err
		}
		shs[si] = sh
	}
	net.shs = shs

	return net, nil
}

// Delete closes the hermes instance. This is a blocking call and will
// wait on all the routers to be empitied of work.
func Delete(npp **Hermes) error {
	if npp == nil || *npp == nil {
		return ErrInvalidInstance
	}

	net := *npp
	for _, sh := range net.shs {
		err := sh.stop()
		if err != nil {
			return err
		}
	}

	*npp = nil

	return nil
}

// Send sends the specified message payload to the specified receiver
func (net *Hermes) Send(from ReceiverID, to ReceiverID, payload interface{}) error {
	return net.sh(to).send(from, to, payload)
}

// request implements a request-Reply pattern by exchaning messages between the sender and receiver
// The returned channel can be used to wait on the reply.
func (net *Hermes) request(from, to ReceiverID, payload interface{}, reqID string) error {
	return net.sh(to).request(from, to, payload, reqID)
}

func (net *Hermes) reply(req Message, reply interface{}) error {
	rm := req.(*message)

	return net.sh(rm.from).reply(req, reply)
}

func (net *Hermes) sh(to ReceiverID) *scheduler {
	var h maphash.Hash

	h.SetSeed(net.seed)
	h.WriteString(string(to))

	id := int(h.Sum64() % uint64(len(net.shs)))

	return net.shs[id]
}
