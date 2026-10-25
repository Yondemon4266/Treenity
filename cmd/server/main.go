package main

import (
	"fmt"
	"os"

	"treenity/internal/protocol"
	"treenity/internal/server"
)

func main() {
	srv, err := server.New()
	if err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(protocol.ExitGeneral)
	}
	os.Exit(srv.Serve())
}
