package protocol

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

// Topic and client names: ^[a-zA-Z0-9_.-]{1,32}$ (subject VI.5).
func TestValidName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"one letter", "a", true},
		{"every allowed kind of char", "a.b-c_D9", true},
		{"subject example", "user_events", true},
		{"only dots", "...", true},
		{"exactly 32 chars", strings.Repeat("x", 32), true},
		{"33 chars", strings.Repeat("x", 33), false},
		{"empty", "", false},
		{"space", "bad name", false},
		{"slash", "a/b", false},
		{"colon (used in FIFO paths)", "a:b", false},
		{"exclamation mark", "hi!", false},
		{"accent", "é", false},
		{"trailing newline", "abc\n", false},
	}
	for _, c := range cases {
		if got := ValidName(c.in); got != c.want {
			t.Errorf("%s: ValidName(%q) = %v, want %v", c.name, c.in, got, c.want)
		}
	}
}

// The server PID makes the endpoint unique (subject VI.7).
func TestServerPath(t *testing.T) {
	if got := ServerPath(1337); got != "/tmp/treenity.server.1337" {
		t.Errorf("ServerPath(1337) = %q", got)
	}
	if ServerPath(1) == ServerPath(2) {
		t.Error("two PIDs give the same endpoint")
	}
}

// Each client gets its own reply FIFO, next to the server endpoint.
func TestReplyPath(t *testing.T) {
	got := ReplyPath("/tmp/treenity.server.1337", 42)
	if got != "/tmp/treenity.server.1337:reply:42" {
		t.Errorf("ReplyPath = %q", got)
	}
	if ReplyPath("/tmp/s", 1) == ReplyPath("/tmp/s", 2) {
		t.Error("two clients share the same reply FIFO")
	}
}

// Exit codes of subject VIII.4.
func TestExitCodes(t *testing.T) {
	got := []int{ExitOK, ExitGeneral, ExitTopic, ExitIPC}
	for want, code := range got {
		if code != want {
			t.Errorf("exit codes = %v, want [0 1 2 3]", got)
			break
		}
	}
}

// Limits of subject VI.4: 1024 bytes for key+body, offset is an int32.
func TestLimits(t *testing.T) {
	if MaxPayload != 1024 {
		t.Errorf("MaxPayload = %d, want 1024", MaxPayload)
	}
	if MaxOffset != math.MaxInt32 {
		t.Errorf("MaxOffset = %d, want %d", MaxOffset, math.MaxInt32)
	}
}

// The info output is a JSON object with client, topic, offset, prefix, ipc (VIII.1, VIII.3).
func TestClientInfoJSON(t *testing.T) {
	info := ClientInfo{
		Client: "client0",
		Topic:  "user_events",
		Offset: 4,
		Prefix: "user.create",
		IPC:    "/tmp/treenity.server.1337:consumer:client0",
	}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"client":"client0","topic":"user_events","offset":4,"prefix":"user.create","ipc":"/tmp/treenity.server.1337:consumer:client0"}`
	if string(data) != want {
		t.Errorf("got  %s\nwant %s", data, want)
	}

	// An empty prefix is still printed: the five fields are always there.
	data, _ = json.Marshal(ClientInfo{Client: "c"})
	if !strings.Contains(string(data), `"prefix":""`) {
		t.Errorf("empty prefix missing from %s", data)
	}
}

// Every message travels as one JSON line: a Request with all fields must stay
// one line, and survive the round trip unchanged.
func TestRequestRoundTrip(t *testing.T) {
	req := Request{
		Op: OpSubscribe, Reply: "/tmp/r", Topic: "t", Client: "c",
		Key: []byte("k\n"), Value: []byte{0, 255}, HasOffset: true, Prefix: []byte("p"), Offset: 7,
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsRune(string(data), '\n') {
		t.Fatalf("request is not one line: %s", data)
	}
	var back Request
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.Op != req.Op || back.Reply != req.Reply || back.Topic != req.Topic || back.Client != req.Client ||
		string(back.Key) != string(req.Key) || string(back.Value) != string(req.Value) ||
		back.HasOffset != req.HasOffset || string(back.Prefix) != string(req.Prefix) || back.Offset != req.Offset {
		t.Errorf("got %+v, want %+v", back, req)
	}
}
