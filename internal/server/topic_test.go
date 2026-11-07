package server

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"treenity/internal/protocol"
)

// farewell makes the topic goroutine send a shutdown delivery to every consumer.
func TestFarewellSendsShutdown(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	alice := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
	bob := subscribe(t, s, protocol.Request{Topic: "orders", Client: "bob"})

	s.topics["orders"].farewell <- struct{}{}
	for name, c := range map[string]*consumer{"alice": alice, "bob": bob} {
		if d := c.next(t); d.Type != protocol.DeliveryShutdown {
			t.Errorf("%s: got %+v, want a shutdown delivery", name, d)
		}
	}

	// The topic keeps working after farewell: it still serves detach.
	wantCode(t, "disconnect after farewell",
		s.handle(protocol.Request{Op: protocol.OpDisconnect, Client: "alice"}), protocol.ExitOK)
}

// stop closes and removes every consumer FIFO, then the goroutine ends.
func TestStopRemovesConsumerFIFOs(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	c := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
	path := s.endpoint + ":consumer:alice"

	done := make(chan struct{})
	go func() {
		s.stopTopics()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stopTopics did not return")
	}
	s.topics = nil // already stopped: nothing left for the cleanup

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Error("consumer FIFO still exists after stop")
	}
	// The server closed its side: the consumer now reads end of file.
	c.pipe.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.lines.ReadBytes('\n'); err != io.EOF {
		t.Errorf("consumer read after stop: got %v, want EOF", err)
	}
}

// A consumer that does not read must not block the topic: the others keep
// receiving, and the slow one gets everything later through the retry ticker.
func TestSlowConsumerDoesNotBlockOthers(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	slow := subscribe(t, s, protocol.Request{Topic: "orders", Client: "slow"})
	fast := subscribe(t, s, protocol.Request{Topic: "orders", Client: "fast"})

	// About 95 KB of deliveries: more than the pipe buffer (64 KB).
	const n = 70
	value := strings.Repeat("v", 1000)
	go func() {
		for i := 0; i < n; i++ {
			s.handle(protocol.Request{Op: protocol.OpProduce, Topic: "orders", Key: []byte("k"), Value: []byte(value)})
		}
	}()

	for i := int32(0); i < n; i++ {
		if d := fast.next(t); d.Offset != i {
			t.Fatalf("fast consumer: got offset %d, want %d", d.Offset, i)
		}
	}
	// Now the slow consumer starts reading: nothing is lost, nothing is out of order.
	for i := int32(0); i < n; i++ {
		if d := slow.next(t); d.Offset != i {
			t.Fatalf("slow consumer: got offset %d, want %d", d.Offset, i)
		}
	}
}
