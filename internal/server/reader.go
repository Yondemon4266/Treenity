package server

import (
	"encoding/json"
	"errors"
	"os"
	"syscall"
	"time"

	"treenity/internal/protocol"
)

const writeTimeout = 50 * time.Millisecond

type reader struct {
	name   string
	prefix []byte
	pipe   *os.File
	path   string
	next   int32
	pid    int  // process of the client, 0 if unknown
	gone   bool // its process died: a disconnect has been sent for it
}

// alive tells whether a process still exists. Signal 0 sends nothing: the
// kernel only checks the PID. EPERM means it exists but belongs to someone else.
func alive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func (r *reader) writeLine(d protocol.Delivery) error {
	data, _ := json.Marshal(d)
	r.pipe.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := r.pipe.Write(append(data, '\n'))
	return err
}
