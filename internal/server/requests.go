package server

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"treenity/internal/fifo"
	"treenity/internal/protocol"
)

// handle turns one request into its response.
func (s *Server) handle(req protocol.Request) protocol.Response {
	// While stopping, consumers may still commit and say goodbye, nothing else.
	if s.stopping && req.Op != protocol.OpDisconnect && req.Op != protocol.OpAck {
		return failure(protocol.ExitIPC, "server is shutting down")
	}
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
	case protocol.OpAck:
		return s.ack(req.Client, req.Offset)
	case protocol.OpDisconnect:
		return s.disconnect(req.Client)
	case protocol.OpInfo:
		return s.info(req.Client)
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
	s.topics[name] = newTopic(name)
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
	t.incoming <- message{key: key, value: value}
	return protocol.Response{Code: protocol.ExitOK}
}

func (s *Server) subscribe(req protocol.Request) protocol.Response {
	if !protocol.ValidName(req.Client) {
		return failure(protocol.ExitGeneral, "invalid client name %q", req.Client)
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

	path := fmt.Sprintf("%s:consumer:%s", s.endpoint, req.Client)
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
	sub.path, sub.active = path, true
	t.attach <- &reader{name: req.Client, prefix: req.Prefix, pipe: pipe, path: path, next: offset}
	s.connected++
	return protocol.Response{Code: protocol.ExitOK, IPC: path}
}

func (s *Server) ack(name string, offset int32) protocol.Response {
	if sub, ok := s.clients.Get(name); ok && sub.active && offset > sub.offset {
		sub.offset = offset
	}
	return protocol.Response{Code: protocol.ExitOK}
}

func (s *Server) disconnect(name string) protocol.Response {
	sub, ok := s.clients.Get(name)
	if !ok || !sub.active {
		return failure(protocol.ExitTopic, "client %q is not connected", name)
	}
	sub.topic.detach <- name
	fifo.Remove(sub.path)
	sub.active = false
	s.connected--
	return protocol.Response{Code: protocol.ExitOK}
}

func (s *Server) info(name string) protocol.Response {
	sub, ok := s.clients.Get(name)
	if !ok {
		return failure(protocol.ExitTopic, "client %q does not exist", name)
	}
	return protocol.Response{Code: protocol.ExitOK, Info: &protocol.ClientInfo{
		Client: sub.name,
		Topic:  sub.topic.name,
		Offset: sub.offset,
		Prefix: string(sub.prefix),
		IPC:    sub.path,
	}}
}

// listTopics returns the topic names sorted, so the output is the same every time.
func (s *Server) listTopics() protocol.Response {
	names := make([]string, 0, len(s.topics))
	for name := range s.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return protocol.Response{Code: protocol.ExitOK, Topics: names}
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
	data, _ := json.Marshal(resp)
	if err := fifo.Send(path, data); err != nil {
		fmt.Fprintln(os.Stderr, "server: reply to", path+":", err)
	}
}
