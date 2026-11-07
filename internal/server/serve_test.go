package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"treenity/internal/fifo"
	"treenity/internal/protocol"
)

// liveServer runs the real New and Serve, and talks to them through the FIFOs,
// like a client. The tests using it must not run in parallel: New names the
// main FIFO after the PID, which is the same for the whole test binary.
type liveServer struct {
	s       *Server
	exit    chan int
	reply   string
	pipe    *os.File
	replies *bufio.Reader
}

func startServer(t *testing.T) *liveServer {
	t.Helper()
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	ls := &liveServer{s: s, exit: make(chan int, 1), reply: filepath.Join(t.TempDir(), "reply")}
	if err := fifo.Create(ls.reply); err != nil {
		t.Fatal(err)
	}
	if ls.pipe, err = fifo.Listen(ls.reply); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ls.pipe.Close() })
	ls.replies = bufio.NewReader(ls.pipe)
	go func() { ls.exit <- s.Serve() }()
	return ls
}

// send writes one request on the main FIFO, without waiting for an answer.
func (ls *liveServer) send(t *testing.T, req protocol.Request) {
	t.Helper()
	data, _ := json.Marshal(req)
	if err := fifo.Send(ls.s.endpoint, data); err != nil {
		t.Fatalf("send %s: %v", req.Op, err)
	}
}

// ask sends a request and reads the answer on the reply FIFO.
func (ls *liveServer) ask(t *testing.T, req protocol.Request) protocol.Response {
	t.Helper()
	req.Reply = ls.reply
	ls.send(t, req)
	ls.pipe.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := ls.replies.ReadBytes('\n')
	if err != nil {
		t.Fatalf("no answer to %s: %v", req.Op, err)
	}
	var resp protocol.Response
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatalf("bad answer %q: %v", line, err)
	}
	return resp
}

// signal does what the signal goroutine does on SIGINT or SIGTERM.
func (ls *liveServer) signal() {
	ls.s.requests.SetReadDeadline(time.Now())
}

// wait returns the exit code of Serve, and how long it took.
func (ls *liveServer) wait(t *testing.T, max time.Duration) (int, time.Duration) {
	t.Helper()
	start := time.Now()
	select {
	case code := <-ls.exit:
		return code, time.Since(start)
	case <-time.After(max):
		t.Fatalf("Serve did not return within %v", max)
		return 0, 0
	}
}

func TestServeCommands(t *testing.T) {
	ls := startServer(t)
	endpoint := ls.s.endpoint
	if endpoint != protocol.ServerPath(os.Getpid()) || !fifo.IsFIFO(endpoint) {
		t.Fatalf("main FIFO %q missing or misnamed", endpoint)
	}

	wantCode(t, "create", ls.ask(t, protocol.Request{Op: protocol.OpCreate, Topic: "orders"}), protocol.ExitOK)
	wantCode(t, "create again", ls.ask(t, protocol.Request{Op: protocol.OpCreate, Topic: "orders"}), protocol.ExitTopic)

	// A line that is not JSON is skipped; the server keeps serving.
	if err := fifo.Send(endpoint, []byte("not json")); err != nil {
		t.Fatal(err)
	}
	wantCode(t, "create after bad line", ls.ask(t, protocol.Request{Op: protocol.OpCreate, Topic: "alerts"}), protocol.ExitOK)

	// A client that vanished before the answer: the server logs it and goes on.
	ls.send(t, protocol.Request{Op: protocol.OpCreate, Topic: "gone", Reply: filepath.Join(t.TempDir(), "no-reply")})
	wantCode(t, "create after lost reply", ls.ask(t, protocol.Request{Op: protocol.OpCheck, Topic: "gone"}), protocol.ExitOK)

	resp := ls.ask(t, protocol.Request{Op: protocol.OpList})
	if fmt.Sprint(resp.Topics) != "[alerts gone orders]" {
		t.Errorf("list = %v, want [alerts gone orders]", resp.Topics)
	}
	wantCode(t, "info unknown", ls.ask(t, protocol.Request{Op: protocol.OpInfo, Client: "nobody"}), protocol.ExitTopic)

	// No subscriber connected: the server stops at once and removes its FIFO.
	ls.signal()
	code, took := ls.wait(t, 2*time.Second)
	if code != protocol.ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if took > time.Second {
		t.Errorf("stopping with no subscriber took %v", took)
	}
	if _, err := os.Stat(endpoint); !errors.Is(err, os.ErrNotExist) {
		t.Error("main FIFO still exists after stop")
	}
}

// Graceful shutdown: shutdown delivery, then the server waits for the disconnect.
func TestServeShutdownWaitsForSubscribers(t *testing.T) {
	ls := startServer(t)
	wantCode(t, "create", ls.ask(t, protocol.Request{Op: protocol.OpCreate, Topic: "orders"}), protocol.ExitOK)
	resp := ls.ask(t, protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "alice"})
	wantCode(t, "subscribe", resp, protocol.ExitOK)
	alice := openConsumer(t, resp.IPC)

	ls.signal()
	if d := alice.next(t); d.Type != protocol.DeliveryShutdown {
		t.Fatalf("got %+v, want a shutdown delivery", d)
	}
	select {
	case <-ls.exit:
		t.Fatal("server stopped before the subscriber disconnected")
	case <-time.After(200 * time.Millisecond):
	}

	// While it waits, the server refuses new work.
	wantCode(t, "create while stopping", ls.ask(t, protocol.Request{Op: protocol.OpCreate, Topic: "late"}), protocol.ExitIPC)
	wantCode(t, "subscribe while stopping",
		ls.ask(t, protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "bob"}), protocol.ExitIPC)

	// The client answers shutdown with disconnect: the server stops right away.
	wantCode(t, "disconnect", ls.ask(t, protocol.Request{Op: protocol.OpDisconnect, Client: "alice"}), protocol.ExitOK)
	code, took := ls.wait(t, 2*time.Second)
	if code != protocol.ExitOK || took > time.Second {
		t.Errorf("exit code %d after %v, want 0 at once", code, took)
	}
	for _, path := range []string{ls.s.endpoint, resp.IPC} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s still exists after stop", path)
		}
	}
}

// A subscriber that never answers: the server exits after the 5-second grace time.
func TestServeShutdownTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the 5-second grace time")
	}
	ls := startServer(t)
	wantCode(t, "create", ls.ask(t, protocol.Request{Op: protocol.OpCreate, Topic: "orders"}), protocol.ExitOK)
	resp := ls.ask(t, protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "mute"})
	wantCode(t, "subscribe", resp, protocol.ExitOK)
	openConsumer(t, resp.IPC) // opened, but never read and never disconnected

	start := time.Now()
	ls.signal()
	code, _ := ls.wait(t, graceTime+3*time.Second)
	took := time.Since(start)
	if code != protocol.ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if took < graceTime-100*time.Millisecond {
		t.Errorf("stopped after %v, before the grace time", took)
	}
	if _, err := os.Stat(resp.IPC); !errors.Is(err, os.ErrNotExist) {
		t.Error("consumer FIFO of the mute subscriber still exists")
	}
}

// Many producers write to the main FIFO at the same time. Every message is
// delivered once, and the order of each producer is kept.
func TestServeConcurrentProducers(t *testing.T) {
	ls := startServer(t)
	wantCode(t, "create", ls.ask(t, protocol.Request{Op: protocol.OpCreate, Topic: "orders"}), protocol.ExitOK)
	resp := ls.ask(t, protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "all"})
	wantCode(t, "subscribe", resp, protocol.ExitOK)
	c := openConsumer(t, resp.IPC)

	const producers, each = 5, 200
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				data, _ := json.Marshal(protocol.Request{
					Op: protocol.OpProduce, Topic: "orders",
					Key: []byte(fmt.Sprintf("p%d", p)), Value: []byte(fmt.Sprint(i)),
				})
				if err := fifo.Send(ls.s.endpoint, data); err != nil {
					t.Errorf("producer %d: %v", p, err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	next := make(map[string]int)
	for i := int32(0); i < producers*each; i++ {
		d := c.next(t)
		if d.Offset != i {
			t.Fatalf("got offset %d, want %d", d.Offset, i)
		}
		key, want := string(d.Key), fmt.Sprint(next[string(d.Key)])
		if string(d.Value) != want {
			t.Fatalf("%s: got message %s, want %s", key, d.Value, want)
		}
		next[key]++
	}
	for p := 0; p < producers; p++ {
		if got := next[fmt.Sprintf("p%d", p)]; got != each {
			t.Errorf("producer %d: %d messages delivered, want %d", p, got, each)
		}
	}

	ls.send(t, protocol.Request{Op: protocol.OpDisconnect, Client: "all"})
	ls.signal()
	ls.wait(t, 2*time.Second)
}

// A consumer killed without a goodbye (SIGKILL) is noticed through the PID in
// its reply path: the server disconnects it, so its name is free again.
func TestServeDetectsVanishedConsumer(t *testing.T) {
	ls := startServer(t)
	wantCode(t, "create", ls.ask(t, protocol.Request{Op: protocol.OpCreate, Topic: "orders"}), protocol.ExitOK)

	// A real process plays the client: its PID ends its reply path, as ReplyPath builds it.
	proc := exec.Command("sleep", "30")
	if err := proc.Start(); err != nil {
		t.Fatal(err)
	}
	reply := protocol.ReplyPath(ls.s.endpoint, proc.Process.Pid)
	if err := fifo.Create(reply); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fifo.Remove(reply) })
	pipe, err := fifo.Listen(reply)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pipe.Close() })

	ls.send(t, protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "ghost", Reply: reply})
	pipe.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := bufio.NewReader(pipe).ReadBytes('\n')
	if err != nil {
		t.Fatalf("no answer to subscribe: %v", err)
	}
	var resp protocol.Response
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatal(err)
	}
	wantCode(t, "subscribe ghost", resp, protocol.ExitOK)
	openConsumer(t, resp.IPC)

	// Alive: the name is taken.
	wantCode(t, "same name while alive",
		ls.ask(t, protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "ghost"}), protocol.ExitTopic)

	// The process dies without sending disconnect.
	proc.Process.Kill()
	proc.Wait()

	// Within a few retry ticks (100 ms each) the server has let it go.
	for deadline := time.Now().Add(2 * time.Second); ; {
		resp := ls.ask(t, protocol.Request{Op: protocol.OpSubscribe, Topic: "orders", Client: "ghost"})
		if resp.Code == protocol.ExitOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ghost still connected 2s after its process died: %s", resp.Error)
		}
		time.Sleep(50 * time.Millisecond)
	}

	ls.send(t, protocol.Request{Op: protocol.OpDisconnect, Client: "ghost"})
	ls.signal()
	if code, took := ls.wait(t, 2*time.Second); code != protocol.ExitOK || took > time.Second {
		t.Errorf("exit code %d after %v, want 0 at once", code, took)
	}
}
