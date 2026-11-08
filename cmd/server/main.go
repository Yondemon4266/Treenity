package main

import (
	"fmt"
	"os"
	"errors"
	"path/filepath"

	"treenity/internal/protocol"
	"treenity/internal/server"
)

func main() {
	srv, err := server.New()
	if err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(protocol.ExitGeneral)
	}
	code := srv.Serve()
	paths, err := filepath.Glob(protocol.ServerPath(os.Getpid()) + "*")
	if err != nil {
		return
	}
	for _, p := range paths {
		fi, err := os.Lstat(p)
		if err != nil || fi.Mode()&os.ModeNamedPipe == 0 {
			continue
		}
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			fmt.Printf("failed to remove %s: %v", p, err)
		}
	}
	os.Exit(code)
}

