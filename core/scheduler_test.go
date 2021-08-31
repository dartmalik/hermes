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

	//fmt.Printf("received ping: %d\n", a.count)

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

	passMessageSingleRun(t, net, 5000)
}

func BenchmarkCreateReceivers(b *testing.B) {
	net, err := New(func(id ReceiverID) (Receiver, error) {
		a := &PongActor{}
		return a.receive, nil
	})
	if err != nil {
		b.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	fmt.Printf("running test with runs: %d\n", b.N)

	createMessagePassers(b, net, b.N)
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

	createMessagePassers(b, net, b.N)
	//passMessages(b, net, b.N, 1)
	destroyMessagePassers(b, net, b.N)
}

func createMessagePassers(t Tester, net *Hermes, runs int) {
	for ri := 0; ri < runs; ri++ {
		a1 := ReceiverID(fmt.Sprintf("t1-%d", ri))
		a2 := ReceiverID(fmt.Sprintf("t2-%d", ri))

		err := net.Join(a1)
		if err != nil {
			t.Fatalf("actor registeration failed with error: %s\n", err.Error())
		}

		err = net.Join(a2)
		if err != nil {
			t.Fatalf("actor registeration failed with error: %s\n", err.Error())
		}
	}
}

func passMessages(t Tester, net *Hermes, runs, iter int) {
	replyChs := make([]chan Message, 0, runs*iter)

	for ri := 0; ri < runs; ri++ {
		a1 := ReceiverID(fmt.Sprintf("t1-%d", ri))
		a2 := ReceiverID(fmt.Sprintf("t2-%d", ri))

		for i := 0; i < iter; i++ {
			replyChs = append(replyChs, net.Request(a1, &SendPingRequest{to: a2}))
		}
	}

	for _, replyCh := range replyChs {
		select {
		case msg := <-replyCh:
			r := msg.Payload().(*SendPingResponse)
			if r.err != nil {
				t.Fatalf("failed to send ping request: %s\n", r.err.Error())
			}

		case <-time.After(1500 * time.Millisecond):
			t.Fatalf("request_timeout")
		}
	}
}

func destroyMessagePassers(t Tester, net *Hermes, runs int) {
	for ri := 0; ri < runs; ri++ {
		a1 := ReceiverID(fmt.Sprintf("t1-%d", ri))
		a2 := ReceiverID(fmt.Sprintf("t2-%d", ri))

		net.Leave(a1)
		net.Leave(a2)
	}
}

func passMessageSingleRun(t Tester, net *Hermes, iter int) {
	createMessagePassers(t, net, 1)

	passMessages(t, net, 1, iter)

	destroyMessagePassers(t, net, 1)
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
