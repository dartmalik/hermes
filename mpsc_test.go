package hermes

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func TestSingle(t *testing.T) {
	q := NewSegmentedQueue(2)
	if q == nil {
		t.Fatal("failed to create queue")
	}

	elems := []string{"m1", "m2", "m3", "m4"}
	for _, e := range elems {
		q.Add(e)
	}

	ei := 0
	for e := q.Peek(); e != nil; e = q.RemoveAndPeek() {
		if e.(string) != elems[ei] {
			t.Error("element does not match")
		}
		ei++
	}
}

type String struct {
	val string
}

func BenchmarkMPSCAddRemove(b *testing.B) {
	fmt.Printf("benchmark with runs: %d\n", b.N)

	q := NewSegmentedQueue(10_000)
	if q == nil {
		b.Fatal("failed to create queue")
	}

	var num int32 = 0
	go func() {
		for int(atomic.LoadInt32(&num)) != b.N {
			for e := q.Peek(); e != nil; e = q.RemoveAndPeek() {
				atomic.AddInt32(&num, 1)
			}
		}
	}()

	v := &String{val: "m1"}
	batch(b.N, 100_000, func(offset, runs int) {
		for ri := 0; ri < runs; ri++ {
			q.Add(v)
		}
	})

	for int(atomic.LoadInt32(&num)) != b.N {
	}
}
