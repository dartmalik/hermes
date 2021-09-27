package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dartali/hermes"
)

var curClientId uint64 = 0

func NewClientId() MqttClientId {
	return MqttClientId(fmt.Sprintf("c%d", atomic.AddUint64(&curClientId, 1)))
}

func TestClientConnect(t *testing.T) {
	net := createNet(t)

	NewTestClient(NewClientId(), t, net).Init()
}

func TestPubSubQoS0(t *testing.T) {
	payload := "m1"
	topic := MqttTopicFilter("test")
	net := createNet(t)
	c1 := NewTestClient(NewClientId(), t, net).Init()
	c2 := NewTestClient(NewClientId(), t, net).Init()

	c2.Subscribe(topic, MqttQoSLevel0)

	published := c2.AssertMessageReceived(topic.topicName(), payload)
	c1.Publish(topic.topicName(), payload, MqttQoSLevel0)
	waitPID(t, published, "expected message to be published")

	c2.AssertMessageNotReceived(SessionRepubTimeout + 1000*time.Millisecond)
}
func TestPubSubQoS1(t *testing.T) {
	payload := "m1"
	topic := MqttTopicFilter("test")
	net := createNet(t)
	c1 := NewTestClient(NewClientId(), t, net).Init()
	c2 := NewTestClient(NewClientId(), t, net).Init()

	c2.Subscribe(topic, MqttQoSLevel1)

	// validate that c2 receives publishes via topic
	published := c2.AssertMessageReceived(topic.topicName(), payload)
	c1.Publish(topic.topicName(), payload, MqttQoSLevel1)
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
	topic := MqttTopicFilter("test")
	net := createNet(t)
	c1 := NewTestClient(NewClientId(), t, net).Init()
	c2 := NewTestClient(NewClientId(), t, net).Init()

	c2.Subscribe(topic, MqttQoSLevel2)

	// validate c2 receives messages published to topic
	published := c2.AssertMessageReceived(topic.topicName(), payload)
	c1.Publish(topic.topicName(), payload, MqttQoSLevel2)
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
	id  MqttClientId
	t   *testing.T
	net *hermes.Hermes
	end *TestEndpoint
}

func NewTestClient(id MqttClientId, t *testing.T, net *hermes.Hermes) *TestClient {
	return &TestClient{id: id, t: t, net: net}
}

func (tc *TestClient) Init() *TestClient {
	tc.createEndpoint()
	tc.connect()

	return tc
}

func (tc *TestClient) Subscribe(filter MqttTopicFilter, qos MqttQoSLevel) {
	pid := MqttPacketId(1223)
	gotAck := make(chan bool, 1)
	tc.end.onWrite = func(msg interface{}) {
		ack, ok := msg.(*MqttSubAckMessage)
		if !ok {
			tc.t.Fatalf("expected subscribe ack")
		}

		if ack.PacketId != pid {
			tc.t.Fatalf("invalid suback packet id")
		}

		if ack.ReturnCodes[0] == MqttSubAckFailure {
			tc.t.Fatalf("subscribe failed")
		}

		gotAck <- true
	}

	sub := createSubscribe(pid, filter, qos)
	err := tc.net.Send("", clientID(tc.id), sub)
	if err != nil {
		tc.t.Fatalf("failed to subscribe to topic: %s\n", err.Error())
	}

	waitBool(tc.t, gotAck, "expected to receive subscribe ack")
}

func (tc *TestClient) Publish(topic MqttTopicName, payload string, qos MqttQoSLevel) {
	pid := MqttPacketId(33455)
	ackd := make(chan bool, 1)

	if qos == MqttQoSLevel0 {
		ackd <- true
		tc.end.onWrite = func(msg interface{}) {
			_, ok := msg.(*MqttPubAckMessage)
			if ok {
				tc.t.Fatalf("did not expecte QoS0 message to be ackd")
			}
		}
	} else if qos == MqttQoSLevel1 {
		tc.end.onWrite = func(msg interface{}) {
			ack, ok := msg.(*MqttPubAckMessage)
			if !ok {
				tc.t.Fatalf("expected PUBACK")
			}
			if ack.PacketId != pid {
				tc.t.Fatalf("received incorrect packet id in PUBACK")
			}
			ackd <- true
		}
	} else if qos == MqttQoSLevel2 {
		tc.end.onWrite = func(msg interface{}) {
			ack, ok := msg.(*MqttPubRecMessage)
			if !ok {
				tc.t.Fatalf("expected PUBREC")
			}
			if ack.PacketId != pid {
				tc.t.Fatalf("received incorrect packet id in PUBREC")
			}
			ackd <- true
		}
	}

	pub := &MqttPublishMessage{TopicName: topic, Payload: []byte(payload), PacketId: pid, QosLevel: qos}
	err := tc.net.Send("", clientID(tc.id), pub)
	if err != nil {
		tc.t.Fatalf("failed to publish message: %s\n", err.Error())
	}

	waitBool(tc.t, ackd, "expected to receive an ack for publish")

	if qos == MqttQoSLevel2 { // send PUBREL
		tc.pubrel(pid)
	}
}

func (tc *TestClient) AssertMessageReceived(topic MqttTopicName, payload string) chan MqttPacketId {
	published := make(chan MqttPacketId, 1)

	tc.end.onWrite = func(msg interface{}) {
		pub, ok := msg.(*MqttPublishMessage)
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
		_, ok := msg.(*MqttPublishMessage)
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

func (tc *TestClient) PubAck(pid MqttPacketId) {
	ack := &MqttPubAckMessage{PacketId: pid}
	err := tc.net.Send("", clientID(tc.id), ack)
	if err != nil {
		tc.t.Fatalf("failed to send PUBCOMP: %s\n", err.Error())
	}
}

func (tc *TestClient) PubRec(pid MqttPacketId) {
	gotAck := make(chan bool, 1)
	tc.end.onWrite = func(mi interface{}) {
		msg, ok := mi.(*MqttPubRelMessage)
		if !ok {
			tc.t.Fatalf("expected to receive PUBREL but got %T", mi)
		}

		if msg.PacketId != pid {
			tc.t.Fatalf("expected to receive PUBREL for the sent packet id")
		}

		gotAck <- true
	}

	ack := &MqttPubRecMessage{PacketId: pid}
	err := tc.net.Send("", clientID(tc.id), ack)
	if err != nil {
		tc.t.Fatalf("failed to send PUBREC: %s\n", err.Error())
	}

	waitBool(tc.t, gotAck, "expected PUBREC to be ack'd")
}

func (tc *TestClient) PubComp(pid MqttPacketId) {
	ack := &MqttPubCompMessage{PacketId: pid}
	err := tc.net.Send("", clientID(tc.id), ack)
	if err != nil {
		tc.t.Fatalf("failed to send PUBCOMP: %s\n", err.Error())
	}
}

func (tc *TestClient) createEndpoint() {
	end := &TestEndpoint{}
	err := tc.net.Send("", clientID(tc.id), &ClientEndpointCreated{endpoint: end})
	if err != nil {
		tc.t.Fatalf("failed to set endpoint: %s \n", err.Error())
	}

	tc.end = end
}

func (tc *TestClient) connect() {
	gotAck := make(chan bool, 1)
	tc.end.onWrite = func(mi interface{}) {
		msg, ok := mi.(*MqttConnAckMessage)
		if !ok {
			tc.t.Fatalf("expected a connack message")
		}

		if msg.SessionPresent() {
			tc.t.Fatalf("did not expect the session to be present")
		}

		if msg.code != MqttConnackCodeAccepted {
			tc.t.Fatalf("expected the code = accepted")
		}

		gotAck <- true
	}

	conn := createConnect(tc.id)
	err := tc.net.Send("", clientID(tc.id), conn)
	if err != nil {
		tc.t.Fatalf("failed to send connect to client: %s\n", err.Error())
	}

	waitBool(tc.t, gotAck, "expected connect to be ack'd")
}

func (tc *TestClient) pubrel(pid MqttPacketId) {
	completed := make(chan bool, 1)
	tc.end.onWrite = func(msg interface{}) {
		ack, ok := msg.(*MqttPubCompMessage)
		if !ok {
			tc.t.Fatalf("expected PUBCOMP")
		}
		if ack.PacketId != pid {
			tc.t.Fatalf("received incorrect packet id in PUBCOMP")
		}
		completed <- true
	}

	pubrel := &MqttPubRelMessage{PacketId: pid}
	err := tc.net.Send("", clientID(tc.id), pubrel)
	if err != nil {
		tc.t.Fatalf("failed to publish message: %s\n", err.Error())
	}

	waitBool(tc.t, completed, "expected publish to be released")
}

func createConnect(cid MqttClientId) *MqttConnectMessage {
	return &MqttConnectMessage{protocol: "MQTT", protocolLevel: 4, clientId: cid}
}

func createSubscribe(pid MqttPacketId, topic MqttTopicFilter, qos MqttQoSLevel) *MqttSubscribeMessage {
	return &MqttSubscribeMessage{
		PacketId: pid,
		Subscriptions: []MqttSubscription{
			{QosLevel: qos, TopicFilter: topic},
		},
	}
}

var sessionStore = NewInMemSessionStore()

func recvFactory(id hermes.ReceiverID) (hermes.Receiver, error) {
	if isClientID(id) {
		return newClient().preConnectRecv, nil
	} else if isSessionID(id) {
		s, err := newSession(sessionStore, SessionRepubTimeout)
		if err != nil {
			return nil, err
		}

		return s.recv, nil
	} else if IsPubSubID(id) {
		return NewPubSub().recv, nil
	}

	return nil, errors.New("unknown_receiver")
}

func createNet(t *testing.T) *hermes.Hermes {
	net, err := hermes.New(recvFactory)
	if err != nil {
		t.Fatalf("failed to created network: %s\n", err.Error())
	}

	return net
}

func waitBool(t *testing.T, ch chan bool, err string) {
	select {
	case <-ch:

	case <-time.After(1500 * time.Millisecond):
		t.Fatal(err)
	}
}

func waitPID(t *testing.T, ch chan MqttPacketId, err string) MqttPacketId {
	select {
	case pid := <-ch:
		return pid

	case <-time.After(1500 * time.Millisecond):
		t.Fatal(err)
		return 0
	}
}
