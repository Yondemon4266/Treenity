package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"treenity/internal/fifo"
	"treenity/internal/hashmap"
	"treenity/internal/protocol"
)

const graceTime = 5 * time.Second

type Server struct {
	endpoint string
	requests *os.File

	topics    map[string]*topic
	clients   *hashmap.Map[*subscriber]
	connected int
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
	return &Server{
		endpoint: endpoint,
		requests: requests,
		topics:   make(map[string]*topic),
		clients:  hashmap.New[*subscriber](),
	}, nil
}

func (s *Server) Serve() int {
	defer fifo.Remove(s.endpoint)
	defer s.requests.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		s.requests.SetReadDeadline(time.Now())
	}()

	stopping := false
	lines := bufio.NewReader(s.requests)
	for {
		line, err := lines.ReadBytes('\n')
		// Ctrl + C
		if errors.Is(err, os.ErrDeadlineExceeded) {
			if stopping {
				break
			}
			stopping = true
			for _, t := range s.topics {
				t.sayFarewell()
			}
			s.requests.SetReadDeadline(time.Now().Add(graceTime))
			if s.connected == 0 {
				break
			}
			continue
		}
		// Error Read
		if err != nil {
			fmt.Fprintln(os.Stderr, "server: read:", err)
			s.stopTopics()
			return protocol.ExitIPC
		}
		// Error requests
		var req protocol.Request
		if err := json.Unmarshal(line, &req); err != nil {
			fmt.Fprintln(os.Stderr, "server: bad request:", err)
			continue
		}
		resp := s.handle(req)
		s.answer(req.Reply, resp)
		if stopping && s.connected == 0 {
			break // everybody has said goodbye
		}
	}
	s.stopTopics()
	return protocol.ExitOK
}

func (s *Server) stopTopics() {
	for _, t := range s.topics {
		t.shutdown()
	}
}
