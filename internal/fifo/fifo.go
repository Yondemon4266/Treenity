package fifo

import (
	"os"
	"syscall"
)

func Create(path string) error {
	os.Remove(path)
	return syscall.Mkfifo(path, 0600)
}
