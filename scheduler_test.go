package pubsub

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/satori/uuid"
)

type ActorId string

type Actor interface {
	Receive(sys *ActorSystem, message *ActorMessage)
}

type ActorMessage struct {
	from    ActorId
	to      ActorId
	corId   string
	payload interface{}
}

type ActorRequest struct {
	replyCh chan *ActorMessage
	m       *ActorMessage
}

type ActorSystem struct {
	mu        sync.Mutex
	scheduler *Scheduler
	actors    map[ActorId]Actor
	requests  map[string]*ActorRequest
}

func NewActorSystem() (*ActorSystem, error) {
	s, err := NewScheduler(16)
	if err != nil {
		return nil, err
	}

	return &ActorSystem{
		scheduler: s,
		actors:    make(map[ActorId]Actor),
		requests:  make(map[string]*ActorRequest),
	}, nil
}

func (sys *ActorSystem) register(key ActorId, a Actor) error {
	if key == "" {
		return errors.New("invalid_actor_key")
	}

	if _, ok := sys.actors[key]; ok {
		fmt.Printf("[WARN] actor already registered")
	}

	sys.actors[key] = a

	return nil
}

func (sys *ActorSystem) send(m *ActorMessage) error {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	a := sys.actors[m.to]
	if a == nil {
		return errors.New("unregistered_actor")
	}

	if m.corId != "" {
		r, ok := sys.requests[m.corId]
		if ok && r.m.to == m.from {
			r.replyCh <- m
			delete(sys.requests, m.corId)

			return nil
		}
	}

	sys.scheduler.submit(string(m.to), func() {
		a.Receive(sys, m)
	})

	return nil
}

func (sys *ActorSystem) request(from ActorId, to ActorId, request interface{}) chan *ActorMessage {
	m := &ActorMessage{from: from, to: to, corId: uuid.NewV4().String(), payload: request}
	ch := make(chan *ActorMessage, 1)
	r := &ActorRequest{replyCh: ch, m: m}
	sys.requests[m.corId] = r

	sys.send(m)

	return ch
}

type SendPing struct {
	to ActorId
}

type Ping struct{}

type Pong struct{}

type TestActor struct {
	id    ActorId
	count int
}

func (a *TestActor) Receive(sys *ActorSystem, message *ActorMessage) {
	switch t := message.payload.(type) {
	case *SendPing:
		a.onSendPing(sys, message.payload.(*SendPing))

	case *Ping:
		a.onPing(sys, message)

	default:
		fmt.Printf("received unkown messge: %s\n", t)
	}
}

func (a *TestActor) onSendPing(sys *ActorSystem, m *SendPing) {
	reply := <-sys.request(a.id, m.to, &Ping{})

	if _, ok := reply.payload.(*Pong); ok {
		fmt.Printf("received pong\n")
	}
}

func (a *TestActor) onPing(sys *ActorSystem, m *ActorMessage) {
	a.count++

	fmt.Printf("received ping: %d\n", a.count)

	sys.send(&ActorMessage{from: a.id, to: m.from, corId: m.corId, payload: &Pong{}})
}

func TestBasic(t *testing.T) {
	s, err := NewScheduler(4)
	if err != nil {
		t.Fatalf("scheduler creation failed with error: %s\n", err.Error())
	}

	s.run(func() {
		fmt.Printf("ran 1\n")
	})

	s.run(func() {
		fmt.Printf("ran 2\n")
	})

	time.Sleep(1000 * time.Millisecond)
}

func TestActorSys(t *testing.T) {
	sys, err := NewActorSystem()
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	a1 := &TestActor{id: "t1"}
	a2 := &TestActor{id: "t2"}

	err = sys.register(a1.id, a1)
	if err != nil {
		t.Fatalf("actor registeration failed with error: %s\n", err.Error())
	}

	err = sys.register(a2.id, a2)
	if err != nil {
		t.Fatalf("actor registeration failed with error: %s\n", err.Error())
	}

	for i := 0; i < 1; i++ {
		go sys.send(&ActorMessage{from: a1.id, to: a1.id, payload: &SendPing{to: a2.id}})
	}

	time.Sleep(5000 * time.Millisecond)
}
