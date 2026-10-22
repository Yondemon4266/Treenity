package client

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"treenity/internal/protocol"
)

// countOpenFDs counts the number of open file descriptors by the current process (Linux).
func countOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("reading /proc/self/fd not supported on this OS")
	}
	return len(entries)
}

// assertClean verifies that the reply FIFO was successfully deleted and no FDs leaked.
func assertClean(t *testing.T, endpoint string, fdsBefore int) {
	t.Helper()

	replyPath := protocol.ReplyPath(endpoint, os.Getpid())
	if _, err := os.Stat(replyPath); !os.IsNotExist(err) {
		// Clean up to prevent polluting subsequent tests, but fail the test
		os.Remove(replyPath)
		t.Errorf("DISK LEAK: reply FIFO %s was not deleted!", replyPath)
	}

	fdsAfter := countOpenFDs(t)
	if fdsAfter != fdsBefore {
		t.Errorf("FILE DESCRIPTOR (FD) LEAK: %d FDs open before, %d after", fdsBefore, fdsAfter)
	}
}

func TestErrorPathsAndCleanup(t *testing.T) {
	// 1. Fail on syscall.Mkfifo (read-only directory: 0500)
	t.Run("Fail at Mkfifo (read-only directory)", func(t *testing.T) {
		dir := t.TempDir()
		endpoint := filepath.Join(dir, "server.fifo")
		if err := syscall.Mkfifo(endpoint, 0600); err != nil {
			t.Fatal(err)
		}
		// Disallow creating new files in dir
		os.Chmod(dir, 0500)
		defer os.Chmod(dir, 0700)

		fdsBefore := countOpenFDs(t)
		c, code := connect(endpoint)
		if c != nil || code != protocol.ExitGeneral {
			t.Errorf("got (%v, %d), want (nil, %d)", c, code, protocol.ExitGeneral)
		}
		assertClean(t, endpoint, fdsBefore)
	})

	// 2. Fail on os.OpenFile(replyPath) using Umask(0777)
	// Ensures replyPath is properly deleted if its opening fails!
	t.Run("Fail at OpenFile replyPath (Umask 0777)", func(t *testing.T) {
		endpoint := filepath.Join(t.TempDir(), "server.fifo")
		syscall.Mkfifo(endpoint, 0600)

		oldMask := syscall.Umask(0777) // FIFO will be created with 0000 permissions
		defer syscall.Umask(oldMask)

		fdsBefore := countOpenFDs(t)
		c, code := connect(endpoint)
		if c != nil || code != protocol.ExitGeneral {
			t.Errorf("got (%v, %d), want (nil, %d)", c, code, protocol.ExitGeneral)
		}
		assertClean(t, endpoint, fdsBefore)
	})

	// 3. Fail on os.OpenFile(endpoint) (Server down -> ENXIO)
	// Ensures replyPipe is closed AND replyPath deleted!
	t.Run("Fail at OpenFile toServer (no reader ENXIO)", func(t *testing.T) {
		endpoint := filepath.Join(t.TempDir(), "server.fifo")
		syscall.Mkfifo(endpoint, 0600)
		// No reader open on endpoint

		fdsBefore := countOpenFDs(t)
		c, code := connect(endpoint)
		if c != nil || code != protocol.ExitGeneral {
			t.Errorf("got (%v, %d), want (nil, %d)", c, code, protocol.ExitGeneral)
		}
		assertClean(t, endpoint, fdsBefore)
	})

	// 4. Fail on len(data) > protocol.MaxLine in send()
	t.Run("Fail at MaxLine limit in send", func(t *testing.T) {
		endpoint := filepath.Join(t.TempDir(), "server.fifo")
		syscall.Mkfifo(endpoint, 0600)
		srvPipe, _ := os.OpenFile(endpoint, os.O_RDWR, 0)
		defer srvPipe.Close()

		fdsBefore := countOpenFDs(t)
		c, code := connect(endpoint)
		if code != protocol.ExitOK {
			t.Fatalf("connect failed with code %d", code)
		}

		// Request exceeding 4096 bytes
		hugeTopic := strings.Repeat("a", protocol.MaxLine+100)
		_, askCode := c.ask(protocol.Request{Op: protocol.OpCreate, Topic: hugeTopic})
		c.close()

		if askCode != protocol.ExitIPC {
			t.Errorf("got code %d, want %d (ExitIPC)", askCode, protocol.ExitIPC)
		}
		assertClean(t, endpoint, fdsBefore)
	})

	// 5. Fail on toServer.Write(data) (Server closing connection -> Broken Pipe / EPIPE)
	t.Run("Fail at Write (Broken Pipe EPIPE)", func(t *testing.T) {
		endpoint := filepath.Join(t.TempDir(), "server.fifo")
		syscall.Mkfifo(endpoint, 0600)

		fdsBefore := countOpenFDs(t)
		// Server opened in read-only mode
		srvReader, err := os.OpenFile(endpoint, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			t.Fatal(err)
		}

		c, code := connect(endpoint)
		if code != protocol.ExitOK {
			srvReader.Close()
			t.Fatalf("connect failed with code %d", code)
		}

		// Server crashes / closes its descriptor BEFORE the client writes
		srvReader.Close()

		_, askCode := c.ask(protocol.Request{Op: protocol.OpCreate, Topic: "test"})
		c.close()

		if askCode != protocol.ExitIPC {
			t.Errorf("got code %d, want %d (ExitIPC)", askCode, protocol.ExitIPC)
		}
		assertClean(t, endpoint, fdsBefore)
	})
}
