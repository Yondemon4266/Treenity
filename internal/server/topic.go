package server

import (
	"bytes"

	"treenity/internal/protocol"
)

// message is one entry of a topic's log. Its offset is its position in the log.
type message struct {
	key   []byte
	value []byte
}

type topic struct {
	name string
	log  []message
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
