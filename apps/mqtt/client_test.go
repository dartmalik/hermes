package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dartali/hermes"
)

const waitTime = 1500 * time.Millisecond

var curClientId uint64 = 0

func NewClientId() ClientId {
	return ClientId(fmt.Sprintf("c%d", atomic.AddUint64(&curClientId, 1)))
}

func TestClientConnect(t *testing.T) {
	net := createNet(t)

	NewTestClient(NewClientId(), t, net).Init()
}

func TestPubSubQoS0(t *testing.T) {
	payload := "m1"
	topic := TopicFilter("test")
	net := createNet(t)
	c1 := NewTestClient(NewClientId(), t, net).Init()
	c2 := NewTestClient(NewClientId(), t, net).Init()

	c2.Subscribe(topic, QoSLevel0)

	published := c2.AssertMessageReceived(topic.topicName(), payload)
	c1.Publish(topic.topicName(), payload, QoSLevel0)
	waitPID(t, published, "expected message to be published")

	c2.AssertMessageNotReceived(SessionRepubTimeout + 1000*time.Millisecond)
}
func TestPubSubQoS1(t *testing.T) {
	payload := "m1"
	topic := TopicFilter("test")
	net := createNet(t)
	c1 := NewTestClient(NewClientId(), t, net).Init()
	c2 := NewTestClient(NewClientId(), t, net).Init()

	c2.Subscribe(topic, QoSLevel1)

	// validate that c2 receives publishes via topic
	published := c2.AssertMessageReceived(topic.topicName(), payload)
	c1.Publish(topic.topicName(), payload, QoSLevel1)
	waitPID(t, published, "expected message to be published")

	// validate that messages are republished if not ack'd
	published = c2.AssertMessageReceived(topic.topicName(), payload)
	time.Sleep(SessionRepubTimeout + 100*time.Millisecond)
	pid := waitPID(t, published, "expected message to be published")

	// validat that messages are not republished after ack
	c2.PubAck(pid)
	c2.AssertMessageNotReceived(SessionRepubTimeout + 100*time.Millisecond)
}

func TestPubSubQoS2(t *testing.T) {
	payload := "m1"
	topic := TopicFilter("test")
	net := createNet(t)
	c1 := NewTestClient(NewClientId(), t, net).Init()
	c2 := NewTestClient(NewClientId(), t, net).Init()

	c2.Subscribe(topic, QoSLevel2)

	// validate c2 receives messages published to topic
	published := c2.AssertMessageReceived(topic.topicName(), payload)
	c1.Publish(topic.topicName(), payload, QoSLevel2)
	waitPID(t, published, "expected message to be published")

	// validate messages are republished if not ack'd
	published = c2.AssertMessageReceived(topic.topicName(), payload)
	time.Sleep(SessionRepubTimeout + 100*time.Millisecond)
	pid := waitPID(t, published, "expected message to be published")

	// ack the message
	c2.PubRec(pid)
	// TODO: validate that PUBREL is republished on timeout
	c2.PubComp(pid)
	c2.AssertMessageNotReceived(SessionRepubTimeout + 100*time.Millisecond)
}

type TestEndpoint struct {
	onWrite func(msg interface{})
	onClose func()
}

func (end *TestEndpoint) Write(msg interface{}) {
	end.onWrite(msg)
}

func (end *TestEndpoint) WriteAndClose(msg interface{}) {
	end.onWrite(msg)
	end.onClose()
}

func (end *TestEndpoint) Close() {
	end.onClose()
}

type TestClient struct {
	id  ClientId
	t   *testing.T
	net *hermes.Hermes
	end *TestEndpoint
}

func NewTestClient(id ClientId, t *testing.T, net *hermes.Hermes) *TestClient {
	return &TestClient{id: id, t: t, net: net}
}

func (tc *TestClient) Init() *TestClient {
	tc.createEndpoint()
	tc.connect()

	return tc
}

func (tc *TestClient) Subscribe(filter TopicFilter, qos QoSLevel) {
	pid := PacketId(1223)
	gotAck := make(chan bool, 1)
	tc.end.onWrite = func(msg interface{}) {
		ack, ok := msg.(*SubAckMessage)
		if !ok {
			tc.t.Fatalf("expected subscribe ack")
		}

		if ack.PacketId != pid {
			tc.t.Fatalf("invalid suback packet id")
		}

		if ack.ReturnCodes[0] == SubAckFailure {
			tc.t.Fatalf("subscribe failed")
		}

		gotAck <- true
	}

	sub := createSubscribe(pid, filter, qos)
	err := tc.net.Send("", clientID(string(tc.id)), sub)
	if err != nil {
		tc.t.Fatalf("failed to subscribe to topic: %s\n", err.Error())
	}

	waitBool(tc.t, gotAck, "expected to receive subscribe ack")
}

func (tc *TestClient) Publish(topic TopicName, payload string, qos QoSLevel) {
	pid := PacketId(33455)
	ackd := make(chan bool, 1)

	if qos == QoSLevel0 {
		ackd <- true
		tc.end.onWrite = func(msg interface{}) {
			_, ok := msg.(*PubAckMessage)
			if ok {
				tc.t.Fatalf("did not expecte QoS0 message to be ackd")
			}
		}
	} else if qos == QoSLevel1 {
		tc.end.onWrite = func(msg interface{}) {
			ack, ok := msg.(*PubAckMessage)
			if !ok {
				tc.t.Fatalf("expected PUBACK")
			}
			if ack.PacketId != pid {
				tc.t.Fatalf("received incorrect packet id in PUBACK")
			}
			ackd <- true
		}
	} else if qos == QoSLevel2 {
		tc.end.onWrite = func(msg interface{}) {
			ack, ok := msg.(*PubRecMessage)
			if !ok {
				tc.t.Fatalf("expected PUBREC")
			}
			if ack.PacketId != pid {
				tc.t.Fatalf("received incorrect packet id in PUBREC")
			}
			ackd <- true
		}
	}

	pub := &PublishMessage{TopicName: topic, Payload: []byte(payload), PacketId: pid, QosLevel: qos}
	err := tc.net.Send("", clientID(string(tc.id)), pub)
	if err != nil {
		tc.t.Fatalf("failed to publish message: %s\n", err.Error())
	}

	waitBool(tc.t, ackd, "expected to receive an ack for publish")

	if qos == QoSLevel2 { // send PUBREL
		tc.pubrel(pid)
	}
}

func (tc *TestClient) AssertMessageReceived(topic TopicName, payload string) chan PacketId {
	published := make(chan PacketId, 1)

	tc.end.onWrite = func(msg interface{}) {
		pub, ok := msg.(*PublishMessage)
		if !ok {
			tc.t.Fatalf("expected message to be published")
		}
		if string(pub.Payload) != payload {
			tc.t.Fatalf("invalid publish payload")
		}

		published <- pub.PacketId
	}

	return published
}

func (tc *TestClient) AssertMessageNotReceived(timeout time.Duration) {
	published := make(chan bool, 1)

	tc.end.onWrite = func(msg interface{}) {
		_, ok := msg.(*PublishMessage)
		if ok {
			published <- true
		}
	}

	select {
	case <-published:
		tc.t.Fatalf("did not expect message to be received")

	case <-time.After(timeout):
	}
}

func (tc *TestClient) PubAck(pid PacketId) {
	ack := &PubAckMessage{PacketId: pid}
	err := tc.net.Send("", clientID(string(tc.id)), ack)
	if err != nil {
		tc.t.Fatalf("failed to send PUBCOMP: %s\n", err.Error())
	}
}

func (tc *TestClient) PubRec(pid PacketId) {
	gotAck := make(chan bool, 1)
	tc.end.onWrite = func(mi interface{}) {
		msg, ok := mi.(*PubRelMessage)
		if !ok {
			tc.t.Fatalf("expected to receive PUBREL but got %T", mi)
		}

		if msg.PacketId != pid {
			tc.t.Fatalf("expected to receive PUBREL for the sent packet id")
		}

		gotAck <- true
	}

	ack := &PubRecMessage{PacketId: pid}
	err := tc.net.Send("", clientID(string(tc.id)), ack)
	if err != nil {
		tc.t.Fatalf("failed to send PUBREC: %s\n", err.Error())
	}

	waitBool(tc.t, gotAck, "expected PUBREC to be ack'd")
}

func (tc *TestClient) PubComp(pid PacketId) {
	ack := &PubCompMessage{PacketId: pid}
	err := tc.net.Send("", clientID(string(tc.id)), ack)
	if err != nil {
		tc.t.Fatalf("failed to send PUBCOMP: %s\n", err.Error())
	}
}

func (tc *TestClient) createEndpoint() {
	end := &TestEndpoint{}
	err := tc.net.Send("", clientID(string(tc.id)), &ClientEndpointOpened{endpoint: end})
	if err != nil {
		tc.t.Fatalf("failed to set endpoint: %s \n", err.Error())
	}

	tc.end = end
}

func (tc *TestClient) connect() {
	gotAck := make(chan bool, 1)
	tc.end.onWrite = func(mi interface{}) {
		msg, ok := mi.(*ConnAckMessage)
		if !ok {
			tc.t.Fatalf("expected a connack message")
		}

		if msg.SessionPresent() {
			tc.t.Fatalf("did not expect the session to be present")
		}

		if msg.code != ConnackCodeAccepted {
			tc.t.Fatalf("expected the code = accepted")
		}

		gotAck <- true
	}

	conn := createConnect(tc.id)
	err := tc.net.Send("", clientID(string(tc.id)), conn)
	if err != nil {
		tc.t.Fatalf("failed to send connect to client: %s\n", err.Error())
	}

	waitBool(tc.t, gotAck, "expected connect to be ack'd")
}

func (tc *TestClient) pubrel(pid PacketId) {
	completed := make(chan bool, 1)
	tc.end.onWrite = func(msg interface{}) {
		ack, ok := msg.(*PubCompMessage)
		if !ok {
			tc.t.Fatalf("expected PUBCOMP")
		}
		if ack.PacketId != pid {
			tc.t.Fatalf("received incorrect packet id in PUBCOMP")
		}
		completed <- true
	}

	pubrel := &PubRelMessage{PacketId: pid}
	err := tc.net.Send("", clientID(string(tc.id)), pubrel)
	if err != nil {
		tc.t.Fatalf("failed to publish message: %s\n", err.Error())
	}

	waitBool(tc.t, completed, "expected publish to be released")
}

func createConnect(cid ClientId) *ConnectMessage {
	return &ConnectMessage{Protocol: "MQTT", ProtocolLevel: 4, ClientId: cid}
}

func createSubscribe(pid PacketId, topic TopicFilter, qos QoSLevel) *SubscribeMessage {
	return &SubscribeMessage{
		PacketId: pid,
		Subscriptions: []*Subscription{
			{QosLevel: qos, TopicFilter: topic},
		},
	}
}

var sessionStore = NewInMemSessionStore()

func recvFactory(id hermes.ReceiverID) (hermes.Receiver, error) {
	if IsClientID(id) {
		return NewClientRecv(), nil
	} else if IsSessionID(id) {
		s, err := newSession(sessionStore, SessionRepubTimeout)
		if err != nil {
			return nil, err
		}

		return s.recv, nil
	} else if IsPubSubID(id) {
		p, err := NewPubSub(NewInMemMsgStore())
		if err != nil {
			return nil, err
		}
		return p.recv, nil
	}

	return nil, errors.New("unknown_receiver")
}

func createNet(t *testing.T) *hermes.Hermes {
	opts := hermes.NewOpts()
	opts.RF = recvFactory
	net, err := hermes.New(opts)
	if err != nil {
		t.Fatalf("failed to created network: %s\n", err.Error())
	}

	return net
}

func waitBool(t *testing.T, ch chan bool, err string) {
	select {
	case <-ch:

	case <-time.After(waitTime):
		t.Fatal(err)
	}
}

func waitPID(t *testing.T, ch chan PacketId, err string) PacketId {
	select {
	case pid := <-ch:
		return pid

	case <-time.After(waitTime):
		t.Fatal(err)
		return 0
	}
}
