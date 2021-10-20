package mqtt

import (
	"errors"
	"fmt"
	"time"

	"github.com/dartali/hermes"
	"github.com/elliotchance/orderedmap"
)

var (
	ErrInvalidEventsList = errors.New("invalid_events")
	ErrInvalidHandler    = errors.New("invalid_handler")
	ErrNoHandler         = errors.New("no_handlers")
)

func IsEventBusID(rid hermes.ReceiverID) bool {
	return rid == EventBusID()
}

func EventBusID() hermes.ReceiverID {
	return hermes.ReceiverID("/events/bus")
}

func Join(ctx hermes.Context, events []EventID, handler hermes.ReceiverID) error {
	req := &EventBusJoinRequest{E: events, H: handler}
	r, err := ctx.RequestWithTimeout(EventBusID(), req, 1500*time.Millisecond)
	if err != nil {
		return err
	}

	rep := r.Payload().(*EventBusJoinReply)

	return rep.Err
}

func Emit(ctx hermes.Context, eid EventID, payload interface{}) {
	ctx.Send(EventBusID(), &EventBusSendRequest{Mode: EBusMulticast, EID: eid, Payload: payload})
}

type EventID string

type EventBusJoinRequest struct {
	E []EventID
	H hermes.ReceiverID
}
type EventBusJoinReply struct {
	Err error
}

type EventBusLeaveRequest struct {
	H hermes.ReceiverID
}
type EventBusLeaveReply struct {
	Err error
}

type EventBusDeliveryMode int

const (
	EBusMulticast = iota
	EBusChained
)

type EventBusSendRequest struct {
	Mode    EventBusDeliveryMode
	EID     EventID
	Payload interface{}
}
type EventBusSendReply struct {
	Payload interface{}
	Err     error
}

type EventBusChainedReply struct {
	Handled bool
	Payload interface{}
}

type eventbus struct {
	h map[EventID]*orderedmap.OrderedMap
}

func NewEventBusRecv() hermes.Receiver {
	b := &eventbus{h: make(map[EventID]*orderedmap.OrderedMap)}

	return b.recv
}

func (bus *eventbus) recv(ctx hermes.Context, msg hermes.Message) {
	switch t := msg.Payload().(type) {
	case *hermes.Joined:

	case *EventBusJoinRequest:
		req := msg.Payload().(*EventBusJoinRequest)
		err := bus.onJoin(req.E, req.H)
		ctx.Reply(msg, &EventBusJoinReply{Err: err})

	case *EventBusLeaveRequest:
		req := msg.Payload().(*EventBusLeaveRequest)
		err := bus.onLeave(req.H)
		ctx.Reply(msg, &EventBusLeaveReply{Err: err})

	case *EventBusSendRequest:
		bus.onSend(ctx, msg)

	default:
		fmt.Printf("event received invalid event type: %T\n", t)
	}
}

func (bus *eventbus) onJoin(evs []EventID, rid hermes.ReceiverID) error {
	if len(evs) == 0 {
		return ErrInvalidEventsList
	}
	if rid == "" {
		return ErrInvalidHandler
	}

	for _, ev := range evs {
		h := bus.handlers(ev)
		h.Set(rid, true)
	}

	return nil
}

func (bus *eventbus) onLeave(rid hermes.ReceiverID) error {
	if rid == "" {
		return ErrInvalidHandler
	}

	for _, m := range bus.h {
		m.Delete(rid)
	}

	return nil
}

func (bus *eventbus) onSend(ctx hermes.Context, msg hermes.Message) {
	sm := msg.Payload().(*EventBusSendRequest)
	eid := sm.EID
	mode := sm.Mode
	payload := sm.Payload
	hset := bus.handlers(eid)

	if mode == EBusMulticast {
		for el := hset.Front(); el != nil; el = el.Next() {
			h := el.Key.(hermes.ReceiverID)
			ctx.Send(h, payload)
		}
	} else if mode == EBusChained {
		rep := bus.chain(ctx, eid, payload)
		if rep != nil {
			ctx.Reply(msg, &EventBusSendReply{Payload: rep.Payload})
		} else {
			ctx.Reply(msg, &EventBusSendReply{Err: ErrNoHandler})
		}
	}
}

func (bus *eventbus) chain(ctx hermes.Context, eid EventID, req interface{}) *EventBusChainedReply {
	hset := bus.handlers(eid)

	var rep *EventBusChainedReply

	for el := hset.Front(); el != nil; el = el.Next() {
		h := el.Key.(hermes.ReceiverID)

		r, err := ctx.RequestWithTimeout(h, req, 1500*time.Millisecond)
		if err != nil {
			fmt.Printf("[ERROR] failed to handle event: %s, with error: %s", eid, err.Error())
			continue
		}

		rep, ok := r.Payload().(*EventBusChainedReply)
		if !ok {
			fmt.Printf("[ERROR] received invalid reply from handler")
			continue
		}

		if rep.Handled {
			return rep
		}
	}

	return rep
}

func (bus *eventbus) handlers(id EventID) *orderedmap.OrderedMap {
	m, ok := bus.h[id]
	if !ok {
		m = orderedmap.NewOrderedMap()
		bus.h[id] = m
	}

	return m
}
