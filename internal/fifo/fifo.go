package fifo

import (
	"errors"
	"os"
	"syscall"
)

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
