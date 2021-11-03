package hermes

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

var (
	ErrShStopped = errors.New("scheduler_stopped")
)

type sendMsgCmd struct {
	message
	replyCh chan error
}

type leaveCmd struct {
	id ReceiverID
}

type scheduler struct {
	id         int
	net        *Hermes
	factory    ReceiverFactory
	ctxIdleDur time.Duration
	ctx        map[string]*context
	cmds       *Mailbox
	stopped    uint32
	joined     *message
}

func newScheduler(id int, net *Hermes, rf ReceiverFactory, idleDur time.Duration) (*scheduler, error) {
	if net == nil {
		return nil, errors.New("invalid_hermes_instance")
	}
	if rf == nil {
		return nil, errors.New("invalid_factory")
	}

	sh := &scheduler{
		id:         id,
		net:        net,
		factory:    rf,
		ctxIdleDur: idleDur,
		ctx:        make(map[string]*context),
		joined:     &message{payload: &Joined{}},
	}

	cmds, err := newMailbox(1024, sh.onCmd)
	if err != nil {
		return nil, err
	}
	sh.cmds = cmds

	return sh, nil
}

func (sh *scheduler) stop() error {
	if !atomic.CompareAndSwapUint32(&sh.stopped, 0, 1) {
		return ErrShStopped
	}

	for !sh.cmds.stop() {
		time.Sleep(10 * time.Millisecond)
	}

	for _, ctx := range sh.ctx {
		for !ctx.stop() {
			time.Sleep(10 * time.Millisecond)
		}
	}

	return nil
}

// Send sends the specified message payload to the specified receiver
func (sh *scheduler) send(from ReceiverID, to ReceiverID, payload interface{}) error {
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
func (sh *scheduler) request(from, to ReceiverID, payload interface{}, reqID string) error {
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

func (sh *scheduler) reply(mi Message, reply interface{}) error {
	cmd, err := sh.newReplyCmd(mi, reply)
	if err != nil {
		return err
	}

	return sh.localSend(cmd)
}

func (sh *scheduler) localSend(cmd *sendMsgCmd) error {
	if sh.isStopped() {
		return ErrShStopped
	}

	err := sh.cmds.post(cmd)
	if err != nil {
		return err
	}

	return <-cmd.replyCh
}

func (sh *scheduler) onCmd(ci interface{}) {
	switch cmd := ci.(type) {
	case *sendMsgCmd:
		cmd.replyCh <- sh.onSendMsg(&cmd.message)

	case *leaveCmd:
		sh.onRemoveRecv(cmd.id)
	}
}

func (sh *scheduler) onSendMsg(msg *message) error {
	ctx, err := sh.context(msg.to)
	if err != nil {
		return err
	}

	return ctx.submit(msg)
}

func (sh *scheduler) onRemoveRecv(id ReceiverID) {
	ctx, ok := sh.ctx[string(id)]
	if !ok {
		return
	}

	if ctx.stop() {
		delete(sh.ctx, string(id))
	}
}

func (sh *scheduler) onRecvIdle(id ReceiverID) {
	cmd, err := sh.newLeaveCmd(id)
	if err != nil {
		fmt.Printf("[ERROR] received idle timeout for invalid receiver")
		return
	}

	sh.cmds.post(cmd)
}

func (sh *scheduler) context(id ReceiverID) (*context, error) {
	ctx, ok := sh.ctx[string(id)]
	if ok {
		return ctx, nil
	}

	recv, err := sh.factory(id)
	if err != nil {
		return nil, err
	}

	ctx, err = newContext(id, sh.net, recv, sh.ctxIdleDur, func() {
		sh.onRecvIdle(id)
	})
	if err != nil {
		return nil, err
	}

	sh.ctx[string(id)] = ctx
	ctx.submit(sh.joined)

	return ctx, nil
}

func (sh *scheduler) isStopped() bool {
	return atomic.LoadUint32(&sh.stopped) == 1
}

func (sh *scheduler) newSendCmd(from, to ReceiverID, payload interface{}) (*sendMsgCmd, error) {
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

func (sh *scheduler) newRequestCmd(from, to ReceiverID, payload interface{}, reqID string) (*sendMsgCmd, error) {
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

func (sh *scheduler) newReplyCmd(req Message, reply interface{}) (*sendMsgCmd, error) {
	rm, ok := req.(*message)
	if !ok {
		return nil, ErrContextMsg
	}

	cmd, err := sh.newSendCmd(rm.to, rm.from, reply)
	if err != nil {
		return nil, err
	}

	cmd.reqID = rm.reqID

	return cmd, nil
}

func (sh *scheduler) newLeaveCmd(id ReceiverID) (*leaveCmd, error) {
	if id == "" {
		return nil, ErrInvalidRecvID
	}

	return &leaveCmd{id: id}, nil
}
