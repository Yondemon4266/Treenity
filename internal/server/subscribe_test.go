package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"treenity/internal/hashmap"
	"treenity/internal/protocol"
)

// newFullServer is a server with every field set. Its FIFOs live in a temp dir.
// Calls to handle play the role of the main goroutine.
func newFullServer(t *testing.T) *Server {
	t.Helper()
	s := &Server{
		endpoint: filepath.Join(t.TempDir(), "srv"),
		topics:   make(map[string]*topic),
		clients:  hashmap.New[*subscriber](),
	}
	t.Cleanup(s.stopTopics)
	return s
}

func wantCode(t *testing.T, what string, resp protocol.Response, code int) {
	t.Helper()
	if resp.Code != code {
		t.Fatalf("%s: got code %d (%q), want %d", what, resp.Code, resp.Error, code)
	}
}

func create(t *testing.T, s *Server, name string) {
	t.Helper()
	wantCode(t, "create "+name, s.handle(protocol.Request{Op: protocol.OpCreate, Topic: name}), protocol.ExitOK)
}

func produce(t *testing.T, s *Server, topic, key, value string) {
	t.Helper()
	req := protocol.Request{Op: protocol.OpProduce, Topic: topic, Key: []byte(key), Value: []byte(value)}
	wantCode(t, "produce "+key, s.handle(req), protocol.ExitOK)
}

// consumer reads the dedicated FIFO of one subscriber, like the client does.
type consumer struct {
	pipe  *os.File
	lines *bufio.Reader
}

func openConsumer(t *testing.T, path string) *consumer {
	t.Helper()
	pipe, err := os.Open(path)
	if err != nil {
		t.Fatalf("open consumer FIFO: %v", err)
	}
	t.Cleanup(func() { pipe.Close() })
	return &consumer{pipe: pipe, lines: bufio.NewReader(pipe)}
}

func subscribe(t *testing.T, s *Server, req protocol.Request) *consumer {
	t.Helper()
	req.Op = protocol.OpSubscribe
	resp := s.handle(req)
	wantCode(t, "subscribe "+req.Client, resp, protocol.ExitOK)
	return openConsumer(t, resp.IPC)
}

func (c *consumer) next(t *testing.T) protocol.Delivery {
	t.Helper()
	c.pipe.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := c.lines.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	var d protocol.Delivery
	if err := json.Unmarshal(line, &d); err != nil {
		t.Fatalf("bad delivery %q: %v", line, err)
	}
	return d
}

// expect reads the next deliveries and checks their offsets and keys.
func (c *consumer) expect(t *testing.T, offsets []int32, keys []string) {
	t.Helper()
	for i := range offsets {
		d := c.next(t)
		if d.Type != protocol.DeliveryMessage || d.Offset != offsets[i] || string(d.Key) != keys[i] {
			t.Fatalf("delivery %d: got %s offset %d key %q, want message offset %d key %q",
				i, d.Type, d.Offset, d.Key, offsets[i], keys[i])
		}
	}
}

func ack(s *Server, name string, offset int32) protocol.Response {
	return s.handle(protocol.Request{Op: protocol.OpAck, Client: name, Offset: offset})
}

func committed(t *testing.T, s *Server, name string) int32 {
	t.Helper()
	resp := s.handle(protocol.Request{Op: protocol.OpInfo, Client: name})
	wantCode(t, "info "+name, resp, protocol.ExitOK)
	return resp.Info.Offset
}

func TestCheckAndProduceErrors(t *testing.T) {
	s := newFullServer(t)
	wantCode(t, "check unknown", s.handle(protocol.Request{Op: protocol.OpCheck, Topic: "orders"}), protocol.ExitTopic)
	create(t, s, "orders")
	wantCode(t, "check known", s.handle(protocol.Request{Op: protocol.OpCheck, Topic: "orders"}), protocol.ExitOK)

	cases := []struct {
		name  string
		topic string
		key   string
		value string
		code  int
	}{
		{"unknown topic", "nope", "k", "v", protocol.ExitTopic},
		{"exactly 1024 bytes", "orders", "k", strings.Repeat("v", protocol.MaxPayload-1), protocol.ExitOK},
		{"1025 bytes", "orders", "k", strings.Repeat("v", protocol.MaxPayload), protocol.ExitGeneral},
	}
	for _, c := range cases {
		req := protocol.Request{Op: protocol.OpProduce, Topic: c.topic, Key: []byte(c.key), Value: []byte(c.value)}
		wantCode(t, c.name, s.handle(req), c.code)
	}
}

func TestSubscribeErrors(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")

	wantCode(t, "invalid client name",
		s.handle(protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "bad name"}), protocol.ExitGeneral)
	wantCode(t, "unknown topic",
		s.handle(protocol.Request{Op: protocol.OpSubscribe, Topic: "nope", Client: "alice"}), protocol.ExitTopic)

	alice := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
	wantCode(t, "same name while connected",
		s.handle(protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "alice"}), protocol.ExitTopic)

	// The first alice is not disturbed by the refused one.
	produce(t, s, "orders", "k", "v")
	alice.expect(t, []int32{0}, []string{"k"})
}

// If the consumer FIFO cannot be created, the subscriber gets the IPC error code.
func TestSubscribeFIFOError(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	s.endpoint = filepath.Join(t.TempDir(), "missing-dir", "srv")
	wantCode(t, "subscribe", s.handle(protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "alice"}), protocol.ExitIPC)
	if s.connected != 0 {
		t.Errorf("connected = %d after a failed subscribe", s.connected)
	}
}

// A new consumer starts at offset 0 and receives the log in order.
func TestNewConsumerStartsAtZero(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	produce(t, s, "orders", "user1", "hello")
	produce(t, s, "orders", "user2", "bye")

	c := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
	c.expect(t, []int32{0, 1}, []string{"user1", "user2"})

	// Messages produced after the subscription arrive live, with the next offsets.
	produce(t, s, "orders", "user3", "zz")
	c.expect(t, []int32{2}, []string{"user3"})
}

func TestDeliveryContent(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	produce(t, s, "orders", "user.create", `{"user":"reach","age":25}`)
	d := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"}).next(t)
	if d.Type != protocol.DeliveryMessage || d.Offset != 0 ||
		string(d.Key) != "user.create" || string(d.Value) != `{"user":"reach","age":25}` {
		t.Errorf("got %+v", d)
	}
}

// Exact match, prefix match and no match, from the history and live.
func TestPrefixFiltering(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "events")
	for _, key := range []string{"user", "user.login", "admin", "superuser"} {
		produce(t, s, "events", key, "x")
	}

	c := subscribe(t, s, protocol.Request{Topic: "events", Client: "alice", Prefix: []byte("user")})
	c.expect(t, []int32{0, 1}, []string{"user", "user.login"})

	// admin and superuser are skipped: the next one received is the next match.
	produce(t, s, "events", "admin.ban", "x")
	produce(t, s, "events", "user.logout", "x")
	c.expect(t, []int32{5}, []string{"user.logout"})
}

func TestEmptyPrefixReceivesEverything(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "events")
	keys := []string{"user", "admin", "x"}
	for _, key := range keys {
		produce(t, s, "events", key, "v")
	}
	subscribe(t, s, protocol.Request{Topic: "events", Client: "all"}).expect(t, []int32{0, 1, 2}, keys)
}

// Several consumers on one topic, each with its own filter.
func TestSeveralConsumers(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "events")
	a := subscribe(t, s, protocol.Request{Topic: "events", Client: "a", Prefix: []byte("a")})
	b := subscribe(t, s, protocol.Request{Topic: "events", Client: "b", Prefix: []byte("b")})
	all := subscribe(t, s, protocol.Request{Topic: "events", Client: "all"})

	produce(t, s, "events", "a1", "v")
	produce(t, s, "events", "b1", "v")
	produce(t, s, "events", "c1", "v")
	produce(t, s, "events", "a2", "v")

	a.expect(t, []int32{0, 3}, []string{"a1", "a2"})
	b.expect(t, []int32{1}, []string{"b1"})
	all.expect(t, []int32{0, 1, 2, 3}, []string{"a1", "b1", "c1", "a2"})
}

// Two topics do not see each other's messages.
func TestTopicsAreSeparate(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "one")
	create(t, s, "two")
	c := subscribe(t, s, protocol.Request{Topic: "two", Client: "alice"})
	produce(t, s, "one", "from-one", "v")
	produce(t, s, "two", "from-two", "v")
	c.expect(t, []int32{0}, []string{"from-two"})
}

// The committed offset only moves when the client acknowledges, and only forward.
func TestOffsetOnlyAdvancesAfterAck(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	c := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
	produce(t, s, "orders", "k0", "v")
	produce(t, s, "orders", "k1", "v")
	produce(t, s, "orders", "k2", "v")
	c.expect(t, []int32{0, 1, 2}, []string{"k0", "k1", "k2"})

	if got := committed(t, s, "alice"); got != 0 {
		t.Fatalf("delivered but not acknowledged: offset %d, want 0", got)
	}
	steps := []struct {
		ack  int32
		want int32
	}{
		{1, 1},
		{3, 3},
		{2, 3}, // a late ack never moves the offset back
		{3, 3}, // a duplicate changes nothing
	}
	for _, st := range steps {
		wantCode(t, "ack", ack(s, "alice", st.ack), protocol.ExitOK)
		if got := committed(t, s, "alice"); got != st.want {
			t.Errorf("after ack %d: offset %d, want %d", st.ack, got, st.want)
		}
	}

	// An ack for an unknown client is ignored, without error.
	wantCode(t, "ack unknown", ack(s, "nobody", 5), protocol.ExitOK)
}

// --offset lets a consumer replay old messages or skip ahead.
func TestCustomOffset(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	for _, k := range []string{"k0", "k1", "k2", "k3"} {
		produce(t, s, "orders", k, "v")
	}

	skip := subscribe(t, s, protocol.Request{Topic: "orders", Client: "skip", HasOffset: true, Offset: 2})
	skip.expect(t, []int32{2, 3}, []string{"k2", "k3"})

	// An offset past the end is accepted: the consumer waits for the messages.
	ahead := subscribe(t, s, protocol.Request{Topic: "orders", Client: "ahead", HasOffset: true, Offset: 5})
	produce(t, s, "orders", "k4", "v")
	produce(t, s, "orders", "k5", "v")
	ahead.expect(t, []int32{5}, []string{"k5"})
	if got := committed(t, s, "ahead"); got != 5 {
		t.Errorf("start offset %d, want 5", got)
	}
}

// A returning consumer resumes where it stopped, unless it asks for an offset.
func TestReturningConsumer(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	create(t, s, "other")
	for _, k := range []string{"k0", "k1", "k2"} {
		produce(t, s, "orders", k, "v")
	}

	first := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
	first.expect(t, []int32{0, 1}, []string{"k0", "k1"})
	ack(s, "alice", 1)
	ack(s, "alice", 2)
	wantCode(t, "disconnect", s.handle(protocol.Request{Op: protocol.OpDisconnect, Client: "alice"}), protocol.ExitOK)
	produce(t, s, "orders", "k3", "v")

	// Same topic, no --offset: resume at the last committed offset, 2.
	again := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
	again.expect(t, []int32{2, 3}, []string{"k2", "k3"})
	s.handle(protocol.Request{Op: protocol.OpDisconnect, Client: "alice"})

	// Same topic with --offset 0: replay from the beginning.
	replay := subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice", HasOffset: true, Offset: 0})
	replay.expect(t, []int32{0}, []string{"k0"})
	s.handle(protocol.Request{Op: protocol.OpDisconnect, Client: "alice"})

	// Another topic: the old offset means nothing there, start at 0.
	produce(t, s, "other", "o0", "v")
	elsewhere := subscribe(t, s, protocol.Request{Topic: "other", Client: "alice"})
	elsewhere.expect(t, []int32{0}, []string{"o0"})
}

func TestDisconnect(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "orders")
	subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
	path := s.endpoint + ":consumer:alice"
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("consumer FIFO missing while connected: %v", err)
	}
	if s.connected != 1 {
		t.Fatalf("connected = %d, want 1", s.connected)
	}

	wantCode(t, "disconnect", s.handle(protocol.Request{Op: protocol.OpDisconnect, Client: "alice"}), protocol.ExitOK)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Error("consumer FIFO still exists after disconnect")
	}
	if s.connected != 0 {
		t.Errorf("connected = %d, want 0", s.connected)
	}

	wantCode(t, "disconnect twice", s.handle(protocol.Request{Op: protocol.OpDisconnect, Client: "alice"}), protocol.ExitTopic)
	wantCode(t, "disconnect unknown", s.handle(protocol.Request{Op: protocol.OpDisconnect, Client: "nobody"}), protocol.ExitTopic)

	// The record is kept: info still works, an ack while disconnected is ignored.
	ack(s, "alice", 7)
	if got := committed(t, s, "alice"); got != 0 {
		t.Errorf("ack while disconnected moved the offset to %d", got)
	}
	// The name is free again.
	subscribe(t, s, protocol.Request{Topic: "orders", Client: "alice"})
}

func TestInfo(t *testing.T) {
	s := newFullServer(t)
	create(t, s, "user_events")
	wantCode(t, "info unknown", s.handle(protocol.Request{Op: protocol.OpInfo, Client: "client0"}), protocol.ExitTopic)

	c := subscribe(t, s, protocol.Request{Topic: "user_events", Client: "client0", Prefix: []byte("user.create")})
	for _, k := range []string{"user.create", "user.update", "user.delete", "user.create"} {
		produce(t, s, "user_events", k, "v")
	}
	// The client acknowledges the two messages it receives: offsets 0 and 3.
	for _, want := range []int32{0, 3} {
		d := c.next(t)
		if d.Offset != want {
			t.Fatalf("got offset %d, want %d", d.Offset, want)
		}
		ack(s, "client0", d.Offset+1)
	}

	// The example of the subject (VIII.3): offset 4 after the two commits.
	resp := s.handle(protocol.Request{Op: protocol.OpInfo, Client: "client0"})
	wantCode(t, "info", resp, protocol.ExitOK)
	want := protocol.ClientInfo{
		Client: "client0",
		Topic:  "user_events",
		Offset: 4,
		Prefix: "user.create",
		IPC:    s.endpoint + ":consumer:client0",
	}
	if *resp.Info != want {
		t.Errorf("got %+v, want %+v", *resp.Info, want)
	}
}
