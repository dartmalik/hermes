package mqtt

import (
	"errors"
	"testing"
	"time"

	"github.com/dartali/hermes"
)

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

func TestClientConnect(t *testing.T) {
	net := createNet(t)
	cid := MqttClientId("c1")
	end := testSetEndpoint(t, net, cid)
	testConnect(t, net, cid, end)
}

func TestPubSubQoS0(t *testing.T) {
	net := createNet(t)
	topic := MqttTopicFilter("test")
	c1 := MqttClientId("c1")
	c2 := MqttClientId("c2")

	testConnectClient(t, net, c1)

	e2 := testConnectClient(t, net, c2)
	testSubscribe(t, net, c2, e2, topic)

	published := make(chan bool, 1)
	e2.onWrite = func(msg interface{}) {
		pub, ok := msg.(*SessionMessage)
		if !ok {
			t.Fatalf("expected message to be published")
		}
		if string(pub.msg.Payload) != "test" {
			t.Fatalf("invalid publish payload")
		}

		published <- true
	}
	testPublish(t, net, c1, topic.topicName(), "test")

	wait(t, published, "expected message to be published")
}

func testConnectClient(t *testing.T, net *hermes.Hermes, cid MqttClientId) *TestEndpoint {
	e1 := testSetEndpoint(t, net, cid)
	testConnect(t, net, cid, e1)

	return e1
}

func testSetEndpoint(t *testing.T, net *hermes.Hermes, cid MqttClientId) *TestEndpoint {
	end := &TestEndpoint{}
	err := net.Send("", clientID(cid), &ClientEndpointCreated{endpoint: end})
	if err != nil {
		t.Fatalf("failed to set endpoint: %s \n", err.Error())
	}

	return end
}

func testConnect(t *testing.T, net *hermes.Hermes, cid MqttClientId, end *TestEndpoint) {
	gotAck := make(chan bool, 1)
	end.onWrite = func(mi interface{}) {
		msg, ok := mi.(*MqttConnAckMessage)
		if !ok {
			t.Fatalf("expected a connack message")
		}

		if msg.SessionPresent() {
			t.Fatalf("did not expect the session to be present")
		}

		if msg.code != MqttConnackCodeAccepted {
			t.Fatalf("expected the code = accepted")
		}

		gotAck <- true
	}

	conn := createConnect(cid)
	err := net.Send("", clientID(cid), conn)
	if err != nil {
		t.Fatalf("failed to send connect to client: %s\n", err.Error())
	}

	wait(t, gotAck, "expected connect to be ack'd")
}

func testSubscribe(t *testing.T, net *hermes.Hermes, cid MqttClientId, end *TestEndpoint, topic MqttTopicFilter) {
	pid := MqttPacketId(1223)
	gotAck := make(chan bool, 1)
	end.onWrite = func(msg interface{}) {
		ack, ok := msg.(*MqttSubAckMessage)
		if !ok {
			t.Fatalf("expected subscribe ack")
		}

		if ack.PacketId != pid {
			t.Fatalf("invalid suback packet id")
		}

		if ack.ReturnCodes[0] == MqttSubAckFailure {
			t.Fatalf("subscribe failed")
		}

		gotAck <- true
	}

	sub := createSubscribe(pid, topic)
	err := net.Send("", clientID(cid), sub)
	if err != nil {
		t.Fatalf("failed to subscribe to topic: %s\n", err.Error())
	}

	wait(t, gotAck, "expected to receive subscribe ack")
}

func testPublish(t *testing.T, net *hermes.Hermes, cid MqttClientId, topic MqttTopicName, payload string) {
	pid := MqttPacketId(33455)
	pub := &MqttPublishMessage{TopicName: topic, Payload: []byte(payload), PacketId: pid, QosLevel: MqttQoSLevel0}
	err := net.Send("", clientID(cid), pub)
	if err != nil {
		t.Fatalf("failed to publish message: %s\n", err.Error())
	}
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
