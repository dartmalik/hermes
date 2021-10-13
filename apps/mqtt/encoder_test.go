package mqtt

import "testing"

func TestPublish(t *testing.T) {
	enc := newEncoder()
	dec := newDecoder()
	pub := &MqttPublishMessage{
		TopicName: MqttTopicName("t1"),
		Payload:   []byte("test"),
		QosLevel:  1,
		PacketId:  1,
		Duplicate: true,
		Retain:    true,
	}

	buff, err := enc.encode(pub)
	if err != nil {
		t.Fatal(err.Error())
	}

	msgs, err := dec.decode(buff)
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(msgs) != 1 {
		t.Fatalf("expected one message to be decoded")
	}

	m := msgs[0].(*MqttPublishMessage)
	if m.TopicName != pub.TopicName {
		t.Error("topic mismatch")
	}
	if string(m.Payload) != string(pub.Payload) {
		t.Error("payload mismatch")
	}
	if m.PacketId != pub.PacketId {
		t.Error("packet id mismatch")
	}
	if m.QosLevel != pub.QosLevel {
		t.Error("qos mismatch")
	}
	if m.Duplicate != pub.Duplicate {
		t.Error("duplicate flag mismatch")
	}
	if m.Retain != pub.Retain {
		t.Error("retain flag mismatch")
	}
}
