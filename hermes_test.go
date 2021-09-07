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

func TestMessagePassing(t *testing.T) {
	net, err := New(func(id ReceiverID) (Receiver, error) {
		a := &PongActor{}
		return a.receive, nil
	})
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	passMessages(t, net, 1, 5000)
}

func TestIdleReceiver(t *testing.T) {
	joined := 0
	msgs := 0

	net, err := New(func(id ReceiverID) (Receiver, error) {
		recv := func(ctx *Context, m Message) {
			switch m.Payload().(type) {
			case *Joined:
				joined++
				msgs++

			case *Ping:
				msgs++
				ctx.Reply(m, &Pong{})

			default:
				msgs++
			}
		}

		return recv, nil
	})
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	err = net.Send("", "r", &Ping{})
	if err != nil {
		t.Fatalf("send failed with error: %s\n", err.Error())
	}

	time.Sleep(DefaultIdleTimeout + 100*time.Millisecond)

	_, err = net.RequestWithTimeout("", "r", &Ping{}, 5000*time.Millisecond)
	if err != nil {
		t.Fatalf("send failed with error: %s\n", err.Error())
	}

	if joined != 2 {
		t.Fatalf("expected %d joined events but got %d", 2, joined)
	}
	if msgs != 4 {
		t.Fatalf("expected %d events but got %d", 5, msgs)
	}
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

	passMessages(b, net, b.N, 1)
}

func passMessages(t Tester, net *Hermes, runs, iter int) {
	replyChs := make([]chan Message, 0, runs*iter)

	for ri := 0; ri < runs; ri++ {
		a1 := ReceiverID(fmt.Sprintf("t1-%d", ri))
		a2 := ReceiverID(fmt.Sprintf("t2-%d", ri))

		for i := 0; i < iter; i++ {
			replyCh, err := net.Request("", a1, &SendPingRequest{to: a2})
			if err != nil {
				t.Fatalf("failed to send ping request: %s\n", err.Error())
			}

			replyChs = append(replyChs, replyCh)
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
