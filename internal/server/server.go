package server

import (
	"fmt"
	"os"

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

	return protocol.ExitOK
}
