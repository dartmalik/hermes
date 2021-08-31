package hermes

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type SendPingRequest struct {
	to ReceiverID
}
type SendPingResponse struct {
	err error
}

type Ping struct{}

type Pong struct{}

type PongActor struct {
	count int
}

func (a *PongActor) receive(ctx *Context, msg Message) {
	switch msg.Payload().(type) {
	case *SendPingRequest:
		err := a.onSendPing(ctx, msg)
		ctx.Reply(msg, &SendPingResponse{err: err})

	case *Ping:
		a.onPing(ctx, msg)
	}
}

func (a *PongActor) onSendPing(ctx *Context, msg Message) error {
	sp := msg.Payload().(*SendPingRequest)

	reply, err := ctx.RequestWithTimeout(sp.to, &Ping{}, 1500*time.Millisecond)
	if err != nil {
		return err
	}

	if _, ok := reply.Payload().(*Pong); !ok {
		return errors.New("invalid_reply_type")
	}

	return nil
}

func (a *PongActor) onPing(ctx *Context, msg Message) {
	a.count++

	fmt.Printf("received ping: %d\n", a.count)

	ctx.Reply(msg, &Pong{})
}

type Tester interface {
	Fatalf(format string, args ...interface{})
}

func TestBasic(t *testing.T) {
	e, err := NewExecutor()
	if err != nil {
		t.Fatalf("scheduler creation failed with error: %s\n", err.Error())
	}

	e.Run(func() {
		fmt.Printf("ran 1\n")
	})

	e.Run(func() {
		fmt.Printf("ran 2\n")
	})

	time.Sleep(1000 * time.Millisecond)
}

func TestMessagePassing(t *testing.T) {
	net, err := New(func(id ReceiverID) (Receiver, error) {
		a := &PongActor{}
		return a.receive, nil
	})
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	passMessage(t, net, 0, 1000)
}

func BenchmarkMessagePassing(b *testing.B) {
	net, err := New(func(id ReceiverID) (Receiver, error) {
		a := &PongActor{}
		return a.receive, nil
	})
	if err != nil {
		b.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	fmt.Printf("running test with runs: %d\n", b.N)
	for ri := 0; ri < b.N; ri++ {
		passMessage(b, net, ri, 1)
	}
}

func passMessage(t Tester, net *Hermes, run int, iter int) {
	a1 := ReceiverID(fmt.Sprintf("t1-%d", run))
	a2 := ReceiverID(fmt.Sprintf("t2-%d", run))

	err := net.Join(a1)
	if err != nil {
		t.Fatalf("actor registeration failed with error: %s\n", err.Error())
	}

	err = net.Join(a2)
	if err != nil {
		t.Fatalf("actor registeration failed with error: %s\n", err.Error())
	}

	for i := 0; i < iter; i++ {
		msg, err := net.RequestWithTimeout(a1, &SendPingRequest{to: a2}, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("failed to send ping request: %s\n", err.Error())
		}

		r := msg.Payload().(*SendPingResponse)
		if r.err != nil {
			t.Fatalf("failed to send ping request: %s\n", err.Error())
		}
	}

	net.Leave(a1)
	net.Leave(a2)
}

func TestReceiverMembership(t *testing.T) {
	net, err := New(func(id ReceiverID) (Receiver, error) {
		return func(ctx *Context, msg Message) {
			switch msg.Payload().(type) {
			case string:
				ctx.Reply(msg, msg.Payload().(string))
			}
		}, nil
	})
	if err != nil {
		t.Fatalf("failed to created system: %s\n", err.Error())
	}

	aid := ReceiverID("t")
	err = net.Join(aid)
	if err != nil {
		t.Fatalf("failed to register actor: %s\n", err.Error())
	}

	payload := "hello_world!"
	res, err := net.RequestWithTimeout(aid, payload, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("request failed: %s\n", err.Error())
	}

	if res.Payload().(string) != payload {
		t.Fatalf("invalid message echo'ed")
	}

	err = net.Leave(aid)
	if err != nil {
		t.Fatalf("failed to unregister: %s\n", err.Error())
	}

	_, err = net.RequestWithTimeout(aid, "hello_world!", 100*time.Millisecond)
	if err == nil {
		t.Fatal("request should fail after unregisteration")
	}
}
