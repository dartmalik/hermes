package pubsub

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
	id    ActorID
	count int
}

func (a *PongActor) Receive(sys *ActorSystem, msg ActorMessage) {
	switch t := msg.Payload().(type) {
	case *SendPing:
		a.onSendPing(sys, msg)

	case *Ping:
		a.onPing(sys, msg)

	default:
		fmt.Printf("received unkown message: %s\n", t)
	}
}

func (a *PongActor) onSendPing(sys *ActorSystem, msg ActorMessage) {
	sp := msg.Payload().(*SendPing)

	reply, err := sys.RequestWithTimeout(a.id, sp.to, &Ping{}, 1500*time.Millisecond)
	if err != nil {
		fmt.Println("request timed out")
		return
	}

	if _, ok := reply.Payload().(*Pong); ok {
		fmt.Printf("received pong\n")
	}
}

func (a *PongActor) onPing(sys *ActorSystem, msg ActorMessage) {
	a.count++

	fmt.Printf("received ping: %d\n", a.count)

	sys.Reply(msg, &Pong{})
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
	sys, err := NewActorSystem()
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	a1 := &PongActor{id: "t1"}
	a2 := &PongActor{id: "t2"}

	err = sys.Register(a1.id, a1)
	if err != nil {
		t.Fatalf("actor registeration failed with error: %s\n", err.Error())
	}

	err = sys.Register(a2.id, a2)
	if err != nil {
		t.Fatalf("actor registeration failed with error: %s\n", err.Error())
	}

	for i := 0; i < 100; i++ {
		sys.Send(a1.id, a1.id, &SendPing{to: a2.id})
	}

	time.Sleep(1000 * time.Millisecond)
}
