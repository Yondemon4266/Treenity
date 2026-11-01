package server

import (
	"bytes"
	"time"

	"treenity/internal/protocol"
)

// message is one entry of a topic's log. Its offset is its position in the log.
type message struct {
	key   []byte
	value []byte
}

const retryEvery = 100 * time.Millisecond

type topic struct {
	name string
	log  []message

	// Only run() touches log and readers; the other goroutines talk to it
	// through these channels.
	incoming chan message
	attach   chan *reader
	detach   chan string
	readers  map[string]*reader
}

// add appends a message at the end of the log and returns its offset.
func (t *topic) add(key, value []byte) int32 {
	t.log = append(t.log, message{key: key, value: value})
	return int32(len(t.log) - 1)
}

func (t *topic) catchUp(r *reader) error {
	for r.next < int32(len(t.log)) {
		m := t.log[r.next]
		if bytes.HasPrefix(m.key, r.prefix) {
			d := protocol.Delivery{
				Type:   protocol.DeliveryMessage,
				Offset: r.next,
				Key:    m.key,
				Value:  m.value,
			}
			if err := r.writeLine(d); err != nil {
				return err
			}
		}
		r.next++
	}
	return nil
}

func newTopic(name string) *topic {
	t := &topic{
		name:     name,
		incoming: make(chan message, 64),
		attach:   make(chan *reader),
		detach:   make(chan string),
		readers:  make(map[string]*reader),
	}
	go t.run()
	return t
}

// run is the goroutine of the topic: it waits for something to happen and handles it.
func (t *topic) run() {
	retry := time.NewTicker(retryEvery)
	defer retry.Stop()
	for {
		select {
		case m := <-t.incoming:
			t.add(m.key, m.value)
			t.pushAll()
		case r := <-t.attach:
			t.readers[r.name] = r
			t.catchUp(r)
		case name := <-t.detach:
			if r, ok := t.readers[name]; ok {
				r.pipe.Close()
				delete(t.readers, name)
			}
		case <-retry.C:
			t.pushAll()
		}
	}
}

// pushAll gives every reader what it is missing. A failed write is not an error
// here: the reader keeps its position and is tried again at the next tick.
func (t *topic) pushAll() {
	for _, r := range t.readers {
		t.catchUp(r)
	}
}
