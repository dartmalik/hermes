package hermes

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type Scheduler struct {
	id         int
	net        *Hermes
	reqID      uint64
	factory    ReceiverFactory
	reqs       map[string]chan Message
	ctx        map[string]*context
	idleTimers map[string]*time.Timer
	cmds       *Mailbox
	closed     uint32
}

func newScheduler(id int, net *Hermes, rf ReceiverFactory) (*Scheduler, error) {
	if net == nil {
		return nil, errors.New("invalid_hermes_instance")
	}
	if rf == nil {
		return nil, errors.New("invalid_factory")
	}

	sh := &Scheduler{
		id:         id,
		net:        net,
		factory:    rf,
		reqs:       make(map[string]chan Message),
		ctx:        make(map[string]*context),
		idleTimers: make(map[string]*time.Timer),
	}

	cmds, err := newMailbox(1024, sh.onCmd)
	if err != nil {
		return nil, err
	}
	sh.cmds = cmds

	return sh, nil
}

// Send sends the specified message payload to the specified receiver
func (sh *Scheduler) send(from ReceiverID, to ReceiverID, payload interface{}) error {
	if sh.isClosed() {
		return ErrInstanceClosed
	}

	cmd, err := sh.newSendCmd(from, to, payload)
	if err != nil {
		return err
	}

	err = sh.localSend(cmd)
	if err != nil {
		return err
	}

	return nil
}

// request implements a request-Reply pattern by exchaning messages between the sender and receiver
// The returned channel can be used to wait on the reply.
func (sh *Scheduler) request(from, to ReceiverID, request interface{}) (chan Message, error) {
	if sh.isClosed() {
		return nil, ErrInstanceClosed
	}

	cmd, err := sh.newRequestCmd(from, to, request)
	if err != nil {
		return nil, err
	}

	err = sh.localSend(cmd)
	if err != nil {
		return nil, err
	}

	return cmd.replyCh, nil
}

func (sh *Scheduler) reply(mi Message, reply interface{}) error {
	cmd, err := sh.newReplyCmd(mi, reply)
	if err != nil {
		return err
	}

	return sh.localSend(cmd)
}

func (sh *Scheduler) localSend(cmd *sendMsgCmd) error {
	err := sh.cmds.post(cmd)
	if err != nil {
		return err
	}

	return <-cmd.cmdReplyCh
}

func (sh *Scheduler) onCmd(ci interface{}) {
	switch cmd := ci.(type) {
	case *sendMsgCmd:
		cmd.cmdReplyCh <- sh.onSend(cmd.sendType(), &cmd.message)

	case *leaveCmd:
		sh.onIdleReceiver(cmd.id)
	}
}

func (sh *Scheduler) onIdleReceiver(id ReceiverID) {
	ctx, ok := sh.ctx[string(id)]
	if !ok {
		return
	}

	if ctx.stop() {
		delete(sh.ctx, string(id))
	}
}

func (sh *Scheduler) onSend(sendType int, msg *message) error {
	ctx, err := sh.context(msg.to)
	if err != nil {
		return err
	}

	tm := sh.idleTimers[string(msg.to)]
	tm.Reset(RouterIdleTimeout)

	switch sendType {
	case SendReply:
		fmt.Printf("%d sending reply\n", sh.id)

		replyCh, ok := sh.reqs[msg.corID]
		if !ok {
			return errors.New("invalid_cor-id")
		}
		delete(sh.reqs, msg.corID)

		replyCh <- msg

		return nil

	case SendRequest:
		fmt.Printf("%d sending request\n", sh.id)
		sh.reqs[msg.corID] = msg.replyCh
		if err != nil {
			return err
		}
		fallthrough

	default:
		return ctx.submit(msg)
	}
}

func (sh *Scheduler) context(id ReceiverID) (*context, error) {
	ctx, ok := sh.ctx[string(id)]
	if ok {
		return ctx, nil
	}

	recv, err := sh.factory(id)
	if err != nil {
		return nil, err
	}

	ctx, err = newContext(id, sh.net, recv)
	if err != nil {
		return nil, err
	}

	sh.ctx[string(id)] = ctx
	ctx.submit(&message{to: ctx.id, payload: &Joined{}})

	t := time.AfterFunc(RouterIdleTimeout, func() {
		sh.onIdleTimeout(ctx)
	})
	sh.idleTimers[string(id)] = t

	return ctx, nil
}

func (sh *Scheduler) onIdleTimeout(ctx *context) {
	cmd, err := sh.newLeaveCmd(ctx.ID())
	if err != nil {
		fmt.Printf("[ERROR] received idle timeout for invalid receiver")
		return
	}

	sh.cmds.post(cmd)
}

func (sh *Scheduler) nextReqID() uint64 {
	return atomic.AddUint64(&sh.reqID, 1)
}

func (sh *Scheduler) isClosed() bool {
	return atomic.LoadUint32(&sh.closed) == 1
}

func (sh *Scheduler) newSendCmd(from, to ReceiverID, payload interface{}) (*sendMsgCmd, error) {
	if to == "" {
		return nil, ErrInvalidMsgTo
	}
	if payload == nil {
		return nil, ErrInvalidMsgPayload
	}

	return &sendMsgCmd{
		message: message{
			from:    from,
			to:      to,
			payload: payload,
		},
		cmdReplyCh: make(chan error, 1),
	}, nil
}

func (sh *Scheduler) newRequestCmd(from, to ReceiverID, payload interface{}) (*sendMsgCmd, error) {
	if from == "" {
		return nil, ErrInvalidMsgFrom
	}

	cmd, err := sh.newSendCmd(from, to, payload)
	if err != nil {
		return nil, err
	}

	cmd.corID = fmt.Sprintf("%d", sh.nextReqID())
	cmd.replyCh = make(chan Message, 1)

	return cmd, nil
}

func (sh *Scheduler) newReplyCmd(req Message, reply interface{}) (*sendMsgCmd, error) {
	rm, ok := req.(*message)
	if !ok {
		return nil, ErrContextInvalidMessage
	}

	cmd, err := sh.newSendCmd(rm.to, rm.from, reply)
	if err != nil {
		return nil, err
	}

	cmd.corID = rm.corID

	return cmd, nil
}

func (sh *Scheduler) newLeaveCmd(id ReceiverID) (*leaveCmd, error) {
	if id == "" {
		return nil, ErrInvalidRecvID
	}

	return &leaveCmd{id: id}, nil
}
