package fifo

import (
	"errors"
	"os"
	"syscall"
	"time"
)

import "treenity/internal/protocol"

func Create(path string) error {
	os.Remove(path)
	return syscall.Mkfifo(path, 0600)
}

func Remove(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Listen opens the server's pipe in read-write mode.
func Listen(path string) (*os.File, error) {
	pipe, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return pipe, nil
}

func Send(path string, data []byte) error {
	if len(data)+1 > protocol.MaxLine {
		return errors.New("fifo: line longer than 4096 bytes")
	}
	pipe, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer pipe.Close()

	pipe.SetWriteDeadline(time.Now().Add(time.Second))
	_, err = pipe.Write(append(data, '\n'))
	return err
}
