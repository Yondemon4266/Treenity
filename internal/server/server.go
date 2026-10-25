package server

import (
	"fmt"
	"os"
	"bufio"

	"encoding/json"
	"treenity/internal/fifo"
	"treenity/internal/protocol"
)

type Server struct {
	endpoint string
	requests *os.File
}

func New() (*Server, error) {
	endpoint := protocol.ServerPath(os.Getpid())

	if err := fifo.Create(endpoint); err != nil {
		return nil, fmt.Errorf("create %s: %w", endpoint, err)
	}

	requests, err := fifo.Listen(endpoint)
	if err != nil {
		fifo.Remove(endpoint)
		return nil, fmt.Errorf("open %s: %w", endpoint, err)
	}

	fmt.Println(endpoint)
	return &Server{endpoint: endpoint, requests: requests}, nil
}

func (s *Server) Serve() int {
	defer fifo.Remove(s.endpoint)
	defer s.requests.Close()

	reader := bufio.NewReader(s.requests)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			fmt.Fprintln(os.Stderr, "server: read:", err)
			return protocol.ExitIPC
		}

		var req protocol.Request
		if err := json.Unmarshal(line, &req); err != nil {
			fmt.Fprintln(os.Stderr, "server: bad request:", err)
			continue
		}

		fmt.Fprintf(os.Stderr, "received: %+v\n", req)
	}
}
