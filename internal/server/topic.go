package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"treenity/internal/fifo"
	"treenity/internal/protocol"
	"treenity/internal/trie"
)

// message is one entry of a topic's log. Its offset is its position in the log.
type message struct {
	key   []byte
	value []byte
}

const retryEvery = 100 * time.Millisecond

type topic struct {
	name     string
	endpoint string // main FIFO of the server, to report consumers that vanished
	log      []message

	// Only run() touches log and readers; the other goroutines talk to it
	// through these channels.
	incoming chan message
	attach   chan *reader
	detach   chan string
	readers  map[string]*reader
	prefixes *trie.Trie
	farewell chan struct{}
	stop     chan struct{}
	done     chan struct{}
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

func newTopic(name, endpoint string) *topic {
	t := &topic{
		name:     name,
		endpoint: endpoint,
		incoming: make(chan message, 64),
		attach:   make(chan *reader),
		detach:   make(chan string),
		readers:  make(map[string]*reader),
		prefixes: trie.New(),
		farewell: make(chan struct{}),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go t.run()
	return t
}

// vanished reports a consumer whose process died without a goodbye (SIGKILL).
// Only the main goroutine may change the client records, so the topic sends it
// the same disconnect request the client would have sent, on the main FIFO.
func (t *topic) vanished(name string) bool {
	data, _ := json.Marshal(protocol.Request{Op: protocol.OpDisconnect, Client: name})
	if err := fifo.Send(t.endpoint, data); err != nil {
		return false // tried again at the next tick
	}
	fmt.Fprintln(os.Stderr, "server: consumer", name, "is gone")
	return true
}

// run is the goroutine of the topic: it waits for something to happen and handles it.
func (t *topic) run() {
	retry := time.NewTicker(retryEvery)
	defer retry.Stop()
	for {
		select {
		case m := <-t.incoming:
			t.log = append(t.log, m)
			for _, name := range t.prefixes.Match(m.key) {
				t.catchUp(t.readers[name])
			}
		case r := <-t.attach:
			t.readers[r.name] = r
			t.prefixes.Add(r.prefix, r.name)
			t.catchUp(r)
		case name := <-t.detach:
			if r, ok := t.readers[name]; ok {
				t.prefixes.Remove(r.prefix, name)
				r.pipe.Close()
				delete(t.readers, name)
			}
		case <-t.farewell:
			for _, r := range t.readers {
				r.writeLine(protocol.Delivery{Type: protocol.DeliveryShutdown})
			}
		case <-t.stop:
			for _, r := range t.readers {
				r.pipe.Close()
				fifo.Remove(r.path)
			}
			close(t.done)
			return
		case <-retry.C:
			for _, r := range t.readers {
				if r.gone {
					continue
				}
				if r.pid > 0 && !alive(r.pid) {
					r.gone = t.vanished(r.name)
					continue
				}
				t.catchUp(r)
			}
		}
	}
}
