package hermes

import (
	"fmt"
	"testing"
	"time"
)

type SendPing struct {
	to ActorID
}

type Ping struct{}

type Pong struct{}

type PongActor struct {
	count int
}

func (a *PongActor) receive(ctx *ActorContext, msg ActorMessage) {
	switch msg.Payload().(type) {
	case *SendPing:
		a.onSendPing(ctx, msg)

	case *Ping:
		a.onPing(ctx, msg)
	}
}

func (a *PongActor) onSendPing(ctx *ActorContext, msg ActorMessage) {
	sp := msg.Payload().(*SendPing)

	reply, err := ctx.RequestWithTimeout(sp.to, &Ping{}, 1500*time.Millisecond)
	if err != nil {
		fmt.Println("request timed out")
		return
	}

	if _, ok := reply.Payload().(*Pong); ok {
		fmt.Printf("received pong\n")
	}
}

func (a *PongActor) onPing(ctx *ActorContext, msg ActorMessage) {
	a.count++

	fmt.Printf("received ping: %d\n", a.count)

	ctx.Reply(msg, &Pong{})
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

func TestActorSys(t *testing.T) {
	sys, err := NewActorSystem(func(id ActorID) (Receiver, error) {
		a := &PongActor{}
		return a.receive, nil
	})
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	a1 := ActorID("t1")
	a2 := ActorID("t2")

	err = sys.Register(a1)
	if err != nil {
		t.Fatalf("actor registeration failed with error: %s\n", err.Error())
	}

	err = sys.Register(a2)
	if err != nil {
		t.Fatalf("actor registeration failed with error: %s\n", err.Error())
	}

	for i := 0; i < 100; i++ {
		sys.Send(a1, a1, &SendPing{to: a2})
	}

	time.Sleep(1000 * time.Millisecond)
}

func TestUnregister(t *testing.T) {
	sys, err := NewActorSystem(func(id ActorID) (Receiver, error) {
		return func(ctx *ActorContext, msg ActorMessage) {
			switch msg.Payload().(type) {
			case string:
				ctx.Reply(msg, msg.Payload().(string))
			}
		}, nil
	})
	if err != nil {
		t.Fatalf("failed to created system: %s\n", err.Error())
	}

	aid := ActorID("t")
	err = sys.Register(aid)
	if err != nil {
		t.Fatalf("failed to register actor: %s\n", err.Error())
	}

	payload := "hello_world!"
	res, err := sys.RequestWithTimeout(aid, payload, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("request failed: %s\n", err.Error())
	}

	if res.Payload().(string) != payload {
		t.Fatalf("invalid message echo'ed")
	}

	err = sys.Unregister(aid)
	if err != nil {
		t.Fatalf("failed to unregister: %s\n", err.Error())
	}

	_, err = sys.RequestWithTimeout(aid, "hello_world!", 100*time.Millisecond)
	if err == nil {
		t.Fatal("request should fail after unregisteration")
	}
}
