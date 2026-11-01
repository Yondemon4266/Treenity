package server

import (
	"encoding/json"
	"os"
	"time"

	"treenity/internal/protocol"
)

const writeTimeout = 50 * time.Millisecond

type reader struct {
	name   string
	prefix []byte
	pipe   *os.File
	next   int32
}

func (r *reader) writeLine(d protocol.Delivery) error {
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	r.pipe.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = r.pipe.Write(append(data, '\n'))
	return err
}
