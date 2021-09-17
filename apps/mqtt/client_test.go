package mqtt

import (
	"errors"
	"testing"
	"time"

	"github.com/dartali/hermes"
)

func TestClientConnect(t *testing.T) {
	net := createNet(t)

	NewTestClient(MqttClientId("c1"), t, net).Init()
}

func TestPubSubQoS0(t *testing.T) {
	payload := "m1"
	topic := MqttTopicFilter("test")
	net := createNet(t)
	c1 := NewTestClient(MqttClientId("c1"), t, net).Init()
	c2 := NewTestClient(MqttClientId("c2"), t, net).Init()

	c2.Subscribe(topic)

	published := c2.AssertMessageReceived(topic.topicName(), payload)
	c1.Publish(topic.topicName(), payload, MqttQoSLevel0)
	wait(t, published, "expected message to be published")
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

func (tc *TestClient) Subscribe(filter MqttTopicFilter) {
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

	sub := createSubscribe(pid, filter)
	err := tc.net.Send("", clientID(tc.id), sub)
	if err != nil {
		tc.t.Fatalf("failed to subscribe to topic: %s\n", err.Error())
	}

	wait(tc.t, gotAck, "expected to receive subscribe ack")
}

func (tc *TestClient) Publish(topic MqttTopicName, payload string, qos MqttQoSLevel) {
	pid := MqttPacketId(33455)
	pub := &MqttPublishMessage{TopicName: topic, Payload: []byte(payload), PacketId: pid, QosLevel: qos}
	err := tc.net.Send("", clientID(tc.id), pub)
	if err != nil {
		tc.t.Fatalf("failed to publish message: %s\n", err.Error())
	}
}

func (tc *TestClient) AssertMessageReceived(topic MqttTopicName, payload string) chan bool {
	published := make(chan bool, 1)

	tc.end.onWrite = func(msg interface{}) {
		pub, ok := msg.(*SessionMessage)
		if !ok {
			tc.t.Fatalf("expected message to be published")
		}
		if string(pub.msg.Payload) != payload {
			tc.t.Fatalf("invalid publish payload")
		}

		published <- true
	}

	return published
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

	wait(tc.t, gotAck, "expected connect to be ack'd")
}

func createConnect(cid MqttClientId) *MqttConnectMessage {
	return &MqttConnectMessage{protocol: "MQTT", protocolLevel: 4, clientId: cid}
}

func createSubscribe(pid MqttPacketId, topic MqttTopicFilter) *MqttSubscribeMessage {
	return &MqttSubscribeMessage{
		PacketId: pid,
		Subscriptions: []MqttSubscription{
			{QosLevel: MqttQoSLevel0, TopicFilter: topic},
		},
	}
}

func recvFactory(id hermes.ReceiverID) (hermes.Receiver, error) {
	if isClientID(id) {
		return newClient().preConnectRecv, nil
	} else if isSessionID(id) {
		return newSession().recv, nil
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

func wait(t *testing.T, ch chan bool, err string) {
	select {
	case <-ch:

	case <-time.After(1500 * time.Millisecond):
		t.Fatal(err)
	}
}
