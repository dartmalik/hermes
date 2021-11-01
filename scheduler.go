package hermes

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type sendMsgCmd struct {
	message
	replyCh chan error
}

type leaveCmd struct {
	id ReceiverID
}

type Scheduler struct {
	id         int
	net        *Hermes
	factory    ReceiverFactory
	ctx        map[string]*context
	idleTimers map[string]*time.Timer
	cmds       *Mailbox
	closed     uint32
	joined     *message
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
		ctx:        make(map[string]*context),
		idleTimers: make(map[string]*time.Timer),
		joined:     &message{payload: &Joined{}},
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
func (sh *Scheduler) request(from, to ReceiverID, payload interface{}, reqID string) error {
	if sh.isClosed() {
		return ErrInstanceClosed
	}

	cmd, err := sh.newRequestCmd(from, to, payload, reqID)
	if err != nil {
		return err
	}

	err = sh.localSend(cmd)
	if err != nil {
		return err
	}

	return nil
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

	return <-cmd.replyCh
}

func (sh *Scheduler) onCmd(ci interface{}) {
	switch cmd := ci.(type) {
	case *sendMsgCmd:
		cmd.replyCh <- sh.onSend(&cmd.message)

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

func (sh *Scheduler) onSend(msg *message) error {
	ctx, err := sh.context(msg.to)
	if err != nil {
		return err
	}

	tm := sh.idleTimers[string(msg.to)]
	tm.Reset(RouterIdleTimeout)

	return ctx.submit(msg)
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
	ctx.submit(sh.joined)

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
		replyCh: make(chan error, 1),
	}, nil
}

func (sh *Scheduler) newRequestCmd(from, to ReceiverID, payload interface{}, reqID string) (*sendMsgCmd, error) {
	if from == "" {
		return nil, ErrInvalidMsgFrom
	}

	cmd, err := sh.newSendCmd(from, to, payload)
	if err != nil {
		return nil, err
	}

	cmd.replyTo = from
	cmd.reqID = reqID

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

	cmd.reqID = rm.reqID

	return cmd, nil
}

func (sh *Scheduler) newLeaveCmd(id ReceiverID) (*leaveCmd, error) {
	if id == "" {
		return nil, ErrInvalidRecvID
	}

	return &leaveCmd{id: id}, nil
}
