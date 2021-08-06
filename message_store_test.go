package persistence

import "testing"

func TestQueuePoll(t *testing.T) {
	s := NewMessageStore()
	q1 := QueueId("test_queue_1")
	q2 := QueueId("test_queue_2")

	s.Add(q1, []byte("001"), "m1")
	s.Add(q1, []byte("002"), "m2")
	s.Add(q1, []byte("003"), "m3")
	s.Add(q2, []byte("004"), "m4")
	s.Add(q2, []byte("005"), "m5")

	m, err := s.Poll(q1, 8)
	if err != nil {
		t.Fatalf("poll failed with error: %s\n", err)
	}
	if len(m) != 3 {
		t.Fatalf("expect %d messages, but received %d\n", 3, len(m))
	}
	if m[0] != "m1" || m[1] != "m2" || m[2] != "m3" {
		t.Fatalf("received messages out of order")
	}

	m, err = s.Poll(q1, 8)
	if err != nil {
		t.Fatalf("poll failed with error: %s\n", err)
	}
	if len(m) != 0 {
		t.Fatalf("expected no messages, but got %d\n", len(m))
	}

	m, err = s.Poll(q2, 1)
	if err != nil {
		t.Fatalf("poll failed with error: %s\n", err)
	}
	if len(m) != 1 {
		t.Fatalf("expected %d messages, but received %d\n", 1, len(m))
	}
	if m[0] != "m4" {
		t.Fatal("received invalid message")
	}
}
