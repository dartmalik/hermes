package hermes

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	BenchmarkBatchNum = 128
)

type SendPingRequest struct {
	to      ReceiverID
	replyCh chan error
}

type Ping struct{}

type Pong struct{}

type PongActor struct {
	count int
}

func (a *PongActor) receive(ctx Context, mi Message) {
	switch msg := mi.Payload().(type) {
	case *SendPingRequest:
		msg.replyCh <- a.onSendPing(ctx, msg)

	case *Ping:
		a.onPing(ctx, mi)
	}
}

func (a *PongActor) onSendPing(ctx Context, sp *SendPingRequest) error {
	reply, err := ctx.RequestWithTimeout(sp.to, &Ping{}, 1500*time.Millisecond)
	if err != nil {
		return err
	}

	if _, ok := reply.Payload().(*Pong); !ok {
		return errors.New("invalid_reply_type")
	}

	return nil
}

func (a *PongActor) onPing(ctx Context, msg Message) {
	a.count++

	//fmt.Printf("received ping: %d\n", a.count)

	ctx.Reply(msg, &Pong{})
}

type Tester interface {
	Fatalf(format string, args ...interface{})
}

func TestMessagePassing(t *testing.T) {
	opts := NewOpts()
	opts.RF = func(id ReceiverID) (Receiver, error) {
		a := &PongActor{}
		return a.receive, nil
	}

	net, err := New(opts)
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	passMessages(t, net, 1, 5000)
}

func TestIdleReceiver(t *testing.T) {
	joined := 0
	msgs := 0

	opts := NewOpts()
	opts.IdleDur = 100 * time.Millisecond
	opts.RF = func(id ReceiverID) (Receiver, error) {
		recv := func(ctx Context, m Message) {
			switch msg := m.Payload().(type) {
			case *Joined:
				joined++
				msgs++

			case *SendPingRequest:
				msgs++
				msg.replyCh <- nil

			default:
				msgs++
			}
		}

		return recv, nil
	}

	net, err := New(opts)
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	replyCh := make(chan error, 1)
	err = net.Send("", "r", &SendPingRequest{replyCh: replyCh})
	if err != nil {
		t.Fatalf("send failed with error: %s\n", err.Error())
	}
	<-replyCh

	time.Sleep(opts.IdleDur + 100*time.Millisecond)

	replyCh = make(chan error, 1)
	err = net.Send("", "r", &SendPingRequest{replyCh: replyCh})
	if err != nil {
		t.Fatalf("send failed with error: %s\n", err.Error())
	}
	<-replyCh

	if joined != 2 {
		t.Fatalf("expected %d joined events but got %d", 2, joined)
	}
	if msgs != 4 {
		t.Fatalf("expected %d events but got %d", 4, msgs)
	}
}

func BenchmarkSends(b *testing.B) {
	var mnum uint32 = 0
	rcv := func(ctx Context, hm Message) {
		switch msg := hm.Payload().(type) {
		case *Joined:

		case *SendPingRequest:
			atomic.AddUint32(&mnum, 1)

		default:
			fmt.Printf("received invalid message: %T\n", msg)
		}
	}

	opts := NewOpts()
	opts.RF = func(id ReceiverID) (Receiver, error) { return rcv, nil }
	net, err := New(opts)
	if err != nil {
		b.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	fmt.Printf("running test with runs: %d\n", b.N)

	req := &SendPingRequest{}
	batch(b.N, BenchmarkBatchNum, func(offset, runs int) {
		a1 := ReceiverID("t1")
		for ri := 0; ri < runs; ri++ {
			net.Send("", a1, req)
		}
	})

	Delete(&net)
	if atomic.LoadUint32(&mnum) != uint32(b.N) {
		b.Fatalf("expected to complete %d runs, but completed %d\n", b.N, mnum)
	}
}

func BenchmarkCreationAndSends(b *testing.B) {
	var mnum uint32 = 0

	rcv := func(ctx Context, hm Message) {
		switch msg := hm.Payload().(type) {
		case *Joined:

		case *SendPingRequest:
			atomic.AddUint32(&mnum, 1)

		default:
			fmt.Printf("received invalid message: %T\n", msg)
		}
	}

	opts := NewOpts()
	opts.RF = func(id ReceiverID) (Receiver, error) { return rcv, nil }
	net, err := New(opts)
	if err != nil {
		b.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	fmt.Printf("running test with runs: %d\n", b.N)

	req := &SendPingRequest{}
	batch(b.N, BenchmarkBatchNum, func(offset, runs int) {
		for ri := 0; ri < runs; ri++ {
			a1 := ReceiverID(strconv.Itoa(offset + ri))
			net.Send("", a1, req)
		}
	})

	Delete(&net)
	if atomic.LoadUint32(&mnum) != uint32(b.N) {
		b.Fatalf("expected to complete %d runs, but completed %d\n", b.N, mnum)
	}
}

func BenchmarkRequests(b *testing.B) {
	opts := NewOpts()
	opts.RF = func(id ReceiverID) (Receiver, error) {
		a := &PongActor{}
		return a.receive, nil
	}
	net, err := New(opts)
	if err != nil {
		b.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	fmt.Printf("running test with runs: %d\n", b.N)

	passMessages(b, net, b.N, 1)

	Delete(&net)
}

func passMessages(t Tester, net *Hermes, runs, iter int) {
	batch(runs, BenchmarkBatchNum, func(offset, len int) {
		replyChs := make([]chan error, 0, len*iter)

		for ri := 0; ri < len; ri++ {
			a1 := ReceiverID(fmt.Sprintf("t1-%d", offset+ri))
			a2 := ReceiverID(fmt.Sprintf("t2-%d", offset+ri))

			for i := 0; i < iter; i++ {
				replyCh := make(chan error, 1)
				err := net.Send("", a1, &SendPingRequest{to: a2, replyCh: replyCh})
				if err != nil {
					t.Fatalf("failed to send ping request: %s\n", err.Error())
				}

				replyChs = append(replyChs, replyCh)
			}
		}

		for _, replyCh := range replyChs {
			select {
			case err := <-replyCh:
				if err != nil {
					t.Fatalf("failed to send ping request: %s\n", err.Error())
				}

			case <-time.After(1500 * time.Millisecond):
				t.Fatalf("request_timeout")
			}
		}
	})
}

func batch(runs, bnum int, cb func(offset, runs int)) {
	bsz := runs / bnum

	if bsz == 0 {
		cb(0, runs)
		return
	}

	var wg sync.WaitGroup
	wg.Add(bnum)
	offset := 0
	for bi := 1; bi <= bnum; bi++ {
		sz := bsz
		if bi == bnum {
			sz = runs - offset
		}
		offset += sz

		go func(o, r int) {
			cb(o, r)
			wg.Done()
		}(offset, sz)
	}

	wg.Wait()
}
