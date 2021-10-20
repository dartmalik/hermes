package mqtt

import "testing"

func TestPublish(t *testing.T) {
	enc := newEncoder()
	dec := newDecoder()
	pub := &PublishMessage{
		TopicName: TopicName("t1"),
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

	m := msgs[0].(*PublishMessage)
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

func TestConnack(t *testing.T) {
	enc := newEncoder()
	dec := newDecoder()
	ack := &ConnAckMessage{
		code: 1,
	}
	ack.SetSessionPresent(true)

	by, err := enc.encode(ack)
	if err != nil {
		t.Fatal(err.Error())
	}

	msgs, err := dec.decode(by)
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(msgs) != 1 {
		t.Fatal(err.Error())
	}

	da := msgs[0].(*ConnAckMessage)
	if !da.SessionPresent() {
		t.Error("invalid session present")
	}
	if da.code != 1 {
		t.Error("invalid return code")
	}
}

func TestPubAck(t *testing.T) {
	enc := newEncoder()
	dec := newDecoder()
	ack := &PubAckMessage{
		PacketId: 2,
	}

	by, err := enc.encode(ack)
	if err != nil {
		t.Fatal(err.Error())
	}

	mi, err := dec.decode(by)
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(mi) != 1 {
		t.Fatal("expected one message to be decoded")
	}

	da := mi[0].(*PubAckMessage)
	if da.PacketId != ack.PacketId {
		t.Error("packets ids mismatch")
	}
}

func TestSubscribe(t *testing.T) {
	enc := newEncoder()
	dec := newDecoder()
	sub := &SubscribeMessage{
		PacketId: 1,
		Subscriptions: []*Subscription{
			{QosLevel: 2, TopicFilter: "t1"},
			{QosLevel: 1, TopicFilter: "t2"},
		},
	}

	by, err := enc.encode(sub)
	if err != nil {
		t.Fatal(err.Error())
	}

	msgs, err := dec.decode(by)
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(msgs) != 1 {
		t.Fatal("expected one message to be decoded")
	}

	ds := msgs[0].(*SubscribeMessage)
	if ds.PacketId != sub.PacketId {
		t.Error("packet id mismatch")
	}
	if len(ds.Subscriptions) != len(sub.Subscriptions) {
		t.Error("subscriptions count mismatch")
	}

	for si := 0; si < len(ds.Subscriptions); si++ {
		if ds.Subscriptions[si].QosLevel != sub.Subscriptions[si].QosLevel {
			t.Error("sub qos mismatch")
		}
		if ds.Subscriptions[si].TopicFilter != sub.Subscriptions[si].TopicFilter {
			t.Error("sub topic filter mismatch")
		}
	}
}
