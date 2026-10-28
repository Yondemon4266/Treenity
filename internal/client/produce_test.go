package client

import (
	"os"
	"testing"
	"time"

	"treenity/internal/protocol"
)

func TestProduceWithLiveServer(t *testing.T) {
	endpoint := os.Getenv("TREENITY_IPC")
	if endpoint == "" {
		t.Skip("TREENITY_IPC not set: run with TREENITY_IPC=/tmp/treenity.server.xxx go test -v ./...")
	}

	tests := []struct {
		name     string
		args     []string
		wantCode int
	}{
		// 1. ARGUMENT ERRORS (ExitGeneral)
		{
			name:     "Missing topic name for produce",
			args:     []string{endpoint, "produce"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Extra unexpected argument for produce",
			args:     []string{endpoint, "produce", "topic1", "extra_arg"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Invalid flag for produce",
			args:     []string{endpoint, "produce", "topic1", "--offset", "10"},
			wantCode: protocol.ExitGeneral,
		},

		// 2. NAME FORMAT VALIDATION ERRORS (ExitGeneral)
		{
			name:     "Invalid topic name format",
			args:     []string{endpoint, "produce", "bad topic!"},
			wantCode: protocol.ExitGeneral,
		},

		// 3. IPC ENDPOINT ERRORS (ExitGeneral)
		{
			name:     "Non-existent IPC endpoint for produce",
			args:     []string{"/tmp/non_existent_fifo_999999", "produce", "topic1"},
			wantCode: protocol.ExitGeneral,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCode := Run(tt.args)
			if gotCode != tt.wantCode {
				t.Errorf("Run(%v) = code %d, want %d", tt.args, gotCode, tt.wantCode)
			}
		})
	}

	// 5. LIVE PRODUCE TEST (TEXT MODE) WITH MOCKED STDIN
	t.Run("Valid produce text mode with simulated stdin", func(t *testing.T) {
		topicName := "test_produce_live"

		// creates topic before
		if code := Run([]string{endpoint, "create", topicName}); code != protocol.ExitOK {
			t.Fatalf("Failed to create topic, got code %d", code)
		}

		// save old stdin to put it back the same at the end
		oldStdin := os.Stdin
		defer func() { os.Stdin = oldStdin }()

		// creating a pipe, it will read from 'r' and write to 'w'
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}
		os.Stdin = r

		// simulating user that enters data, then does CTRL+D (EOF)
		go func() {
			time.Sleep(50 * time.Millisecond)
			_, _ = w.Write([]byte("key1:value1\n"))
			_, _ = w.Write([]byte("key2:value2\n"))
			_ = w.Close() // simulates CTRL+D, EOF
		}()

		// Run will read for our false STDIN, run the tests, and exit to EOF
		gotCode := Run([]string{endpoint, "produce", topicName})
		if gotCode != protocol.ExitOK {
			t.Errorf("Produce live test exited with code %d, want %d", gotCode, protocol.ExitOK)
		}
	})

	// 6. LIVE PRODUCE TEST (RAW MODE) WITH MOCKED STDIN
	t.Run("Valid produce raw mode with simulated stdin", func(t *testing.T) {
		topicName := "test_produce_raw"
		if code := Run([]string{endpoint, "create", topicName}); code != protocol.ExitOK {
			t.Fatalf("Failed to create topic, got code %d", code)
		}

		oldStdin := os.Stdin
		defer func() { os.Stdin = oldStdin }()
		r, w, _ := os.Pipe()
		os.Stdin = r

		go func() {
			time.Sleep(50 * time.Millisecond)

			msg := protocol.Delivery{
				Key:   []byte("test"),
				Value: []byte("ok"),
			}
			if err := protocol.WriteRawMessage(w, msg, false); err != nil {
				t.Errorf("Failed to mock raw message: %v", err)
			}
			_ = w.Close()
		}()

		gotCode := Run([]string{endpoint, "produce", topicName, "--raw"})
		if gotCode != protocol.ExitOK {
			t.Errorf("Produce live raw test exited with code %d, want %d", gotCode, protocol.ExitOK)
		}
	})
}
