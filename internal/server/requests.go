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
	case protocol.OpCheck:
		return s.checkTopic(req.Topic)
	case protocol.OpProduce:
		return s.produce(req.Topic, req.Key, req.Value)
	case protocol.OpSubscribe:
		return s.subscribe(req)
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

// checkTopic tells a producer whether its topic exists, before it reads stdin.
func (s *Server) checkTopic(name string) protocol.Response {
	if _, ok := s.topics[name]; !ok {
		return failure(protocol.ExitTopic, "topic %q does not exist", name)
	}
	return protocol.Response{Code: protocol.ExitOK}
}

func (s *Server) produce(name string, key, value []byte) protocol.Response {
	if len(key)+len(value) > protocol.MaxPayload {
		return failure(protocol.ExitGeneral, "message larger than %d bytes", protocol.MaxPayload)
	}
	t, ok := s.topics[name]
	if !ok {
		return failure(protocol.ExitTopic, "topic %q does not exist", name)
	}
	t.add(key, value)
	return protocol.Response{Code: protocol.ExitOK}
}

// consumerPath is the dedicated FIFO of a subscriber.
func (s *Server) consumerPath(name string) string {
	return fmt.Sprintf("%s:consumer:%s", s.endpoint, name)
}

func (s *Server) subscribe(req protocol.Request) protocol.Response {
	if !protocol.ValidName(req.Client) {
		return failure(protocol.ExitGeneral, "invalid client name %q", req.Client)
	}
	if req.HasOffset && req.Offset < 0 {
		return failure(protocol.ExitGeneral, "invalid offset %d", req.Offset)
	}
	t, ok := s.topics[req.Topic]
	if !ok {
		return failure(protocol.ExitTopic, "topic %q does not exist", req.Topic)
	}
	sub, known := s.clients.Get(req.Client)
	if known && sub.active {
		return failure(protocol.ExitTopic, "client %q is already connected", req.Client)
	}

	// Where to start: the offset asked for, else where this client stopped
	// on the same topic, else the beginning.
	var offset int32
	switch {
	case req.HasOffset:
		offset = req.Offset
	case known && sub.topic == t:
		offset = sub.offset
	}

	path := s.consumerPath(req.Client)
	fifo.Remove(path) // leftover from an earlier run
	if err := fifo.Create(path); err != nil {
		return failure(protocol.ExitIPC, "create %s: %v", path, err)
	}
	pipe, err := fifo.Listen(path)
	if err != nil {
		fifo.Remove(path)
		return failure(protocol.ExitIPC, "open %s: %v", path, err)
	}

	if !known {
		sub = &subscriber{name: req.Client}
		s.clients.Set(req.Client, sub)
	}
	sub.topic, sub.prefix, sub.offset = t, req.Prefix, offset
	sub.path, sub.pipe, sub.active = path, pipe, true
	return protocol.Response{Code: protocol.ExitOK, IPC: path}
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
