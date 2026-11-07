package fifo

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"treenity/internal/protocol"
)

func TestCreateMakesPrivateFIFO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	if err := Create(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("mode %v is not a named pipe", info.Mode())
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("permissions %o, want 600", perm)
	}
}

// A file left by a crash must not stop a new FIFO from being created.
func TestCreateReplacesLeftover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Create(path); err != nil {
		t.Fatalf("Create over a leftover file: %v", err)
	}
	if !IsFIFO(path) {
		t.Fatal("leftover file was not replaced by a FIFO")
	}
}

func TestIsFIFO(t *testing.T) {
	dir := t.TempDir()
	pipe := filepath.Join(dir, "pipe")
	regular := filepath.Join(dir, "file")
	if err := Create(pipe); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(regular, nil, 0600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		path string
		want bool
	}{
		{pipe, true},
		{regular, false},
		{dir, false},
		{filepath.Join(dir, "missing"), false},
	}
	for _, c := range cases {
		if got := IsFIFO(c.path); got != c.want {
			t.Errorf("IsFIFO(%s) = %v, want %v", filepath.Base(c.path), got, c.want)
		}
	}
}

func TestRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	if err := Create(path); err != nil {
		t.Fatal(err)
	}
	if err := Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("file still exists after Remove")
	}
	// Removing a file that is already gone is not an error.
	if err := Remove(path); err != nil {
		t.Errorf("second Remove: %v", err)
	}
}

// Listen opens in read-write mode: it must not block, even with no writer.
func TestListenDoesNotBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	if err := Create(path); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		pipe, err := Listen(path)
		if err == nil {
			pipe.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Listen blocked")
	}
}

func TestListenMissingFile(t *testing.T) {
	if _, err := Listen(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("Listen on a missing path should fail")
	}
}

// Send writes one line ending with '\n' that the listener reads back.
func TestSendAndRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	if err := Create(path); err != nil {
		t.Fatal(err)
	}
	pipe, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pipe.Close()

	for _, msg := range []string{`{"code":0}`, `{"code":2,"error":"x"}`} {
		if err := Send(path, []byte(msg)); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	pipe.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(pipe)
	for _, want := range []string{"{\"code\":0}\n", "{\"code\":2,\"error\":\"x\"}\n"} {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line != want {
			t.Errorf("read %q, want %q", line, want)
		}
	}
}

// With nobody reading, Send fails at once instead of blocking the server.
func TestSendWithoutReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	if err := Create(path); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := Send(path, []byte("hello"))
	if !errors.Is(err, syscall.ENXIO) {
		t.Fatalf("Send without reader: got %v, want ENXIO", err)
	}
	if time.Since(start) > time.Second {
		t.Error("Send without reader took too long")
	}
}

func TestSendMissingPath(t *testing.T) {
	if err := Send(filepath.Join(t.TempDir(), "missing"), []byte("x")); err == nil {
		t.Fatal("Send to a missing path should fail")
	}
}

// A line, '\n' included, must fit in one atomic pipe write (4096 bytes).
func TestSendLineLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	if err := Create(path); err != nil {
		t.Fatal(err)
	}
	pipe, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pipe.Close()

	if err := Send(path, []byte(strings.Repeat("a", protocol.MaxLine-1))); err != nil {
		t.Errorf("4095 bytes + newline should fit: %v", err)
	}
	if err := Send(path, []byte(strings.Repeat("a", protocol.MaxLine))); err == nil {
		t.Error("4096 bytes + newline should be refused")
	}
}
