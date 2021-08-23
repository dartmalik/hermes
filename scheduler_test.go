package pubsub

import (
	"fmt"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	s, err := NewScheduler(4)
	if err != nil {
		t.Fatal("could not create scheduler")
	}

	s.run(func() {
		fmt.Printf("ran 1\n")
	})

	s.run(func() {
		fmt.Printf("ran 2\n")
	})

	time.Sleep(1000 * time.Millisecond)
}
