package server

import (
	"encoding/json"
	"fmt"
	"os"

	"treenity/internal/fifo"
	"treenity/internal/protocol"
)

// handle turns one request into its response.
func (s *Server) handle(req protocol.Request) protocol.Response {
	switch req.Op {
	case protocol.OpCreate:
		return s.createTopic(req.Topic)
	case protocol.OpList:
		return s.listTopics()
	}
	return failure(protocol.ExitGeneral, "unknown operation %q", req.Op)
}

func (s *Server) createTopic(name string) protocol.Response {
	if !protocol.ValidName(name) {
		return failure(protocol.ExitGeneral, "invalid topic name %q", name)
	}
	if _, exists := s.topics[name]; exists {
		return failure(protocol.ExitTopic, "topic %q already exists", name)
	}
	s.topics[name] = &topic{name: name}
	s.order = append(s.order, name)
	return protocol.Response{Code: protocol.ExitOK}
}

func (s *Server) listTopics() protocol.Response {
	return protocol.Response{Code: protocol.ExitOK, Topics: s.order}
}

// failure builds an error response; the client exits with code.
func failure(code int, format string, args ...any) protocol.Response {
	return protocol.Response{Code: code, Error: fmt.Sprintf(format, args...)}
}

// answer writes resp on the client's reply FIFO.
func (s *Server) answer(path string, resp protocol.Response) {
	if path == "" {
		return
	}
	data, err := json.Marshal(resp)
	if err != nil {
		fmt.Fprintln(os.Stderr, "server: encode reply:", err)
		return
	}
	if err := fifo.Send(path, data); err != nil {
		fmt.Fprintln(os.Stderr, "server: reply to", path+":", err)
	}
}
