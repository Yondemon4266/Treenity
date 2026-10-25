package server

import (
	"reflect"
	"testing"

	"treenity/internal/protocol"
)

func newTestServer() *Server {
	return &Server{topics: make(map[string]*topic)}
}

func TestCreateTopic(t *testing.T) {
	s := newTestServer()

	cases := []struct {
		name  string
		topic string
		code  int
	}{
		{"new topic", "orders", protocol.ExitOK},
		{"same topic again", "orders", protocol.ExitTopic},
		{"invalid name", "bad name!", protocol.ExitGeneral},
		{"empty name", "", protocol.ExitGeneral},
	}
	for _, c := range cases {
		resp := s.handle(protocol.Request{Op: protocol.OpCreate, Topic: c.topic})
		if resp.Code != c.code {
			t.Errorf("%s: got code %d, want %d (%s)", c.name, resp.Code, c.code, resp.Error)
		}
	}
}

func TestListKeepsCreationOrder(t *testing.T) {
	s := newTestServer()

	if resp := s.handle(protocol.Request{Op: protocol.OpList}); len(resp.Topics) != 0 {
		t.Fatalf("empty server: got %v", resp.Topics)
	}
	for _, name := range []string{"user_events", "orders", "audit"} {
		s.handle(protocol.Request{Op: protocol.OpCreate, Topic: name})
	}
	resp := s.handle(protocol.Request{Op: protocol.OpList})
	want := []string{"user_events", "orders", "audit"}
	if !reflect.DeepEqual(resp.Topics, want) {
		t.Errorf("got %v, want %v", resp.Topics, want)
	}
}

func TestUnknownOperation(t *testing.T) {
	resp := newTestServer().handle(protocol.Request{Op: "dance"})
	if resp.Code != protocol.ExitGeneral {
		t.Errorf("got code %d, want %d", resp.Code, protocol.ExitGeneral)
	}
}
