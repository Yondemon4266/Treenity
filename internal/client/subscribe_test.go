package client

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"treenity/internal/protocol"
)

func TestSubscribeWithLiveServer(t *testing.T) {
	endpoint := os.Getenv("TREENITY_IPC")
	if endpoint == "" {
		t.Skip("TREENITY_IPC not set: run with TREENITY_IPC=/tmp/treenity.server.xxx go test -v ./...")
	}

	tests := []struct {
		name     string
		args     []string
		wantCode int
	}{
		// 1. ARGUMENT ERRORS (ExitGeneral) - subscribe expects <topic_name> <subscriber_name> [options]
		{
			name:     "Missing topic and subscriber name",
			args:     []string{endpoint, "subscribe"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Missing subscriber name",
			args:     []string{endpoint, "subscribe", "topic1"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Extra unexpected positional argument",
			args:     []string{endpoint, "subscribe", "topic1", "client1", "extra_arg"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Unknown flag for subscribe",
			args:     []string{endpoint, "subscribe", "topic1", "client1", "--unknown"},
			wantCode: protocol.ExitGeneral,
		},

		// 2. OPTIONS VALIDATION ERRORS (ExitGeneral)
		{
			name:     "Missing value for --prefix",
			args:     []string{endpoint, "subscribe", "topic1", "client1", "--prefix"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Missing value for --offset",
			args:     []string{endpoint, "subscribe", "topic1", "client1", "--offset"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Negative value for --offset",
			args:     []string{endpoint, "subscribe", "topic1", "client1", "--offset", "-1"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Offset exceeding MaxInt32",
			args:     []string{endpoint, "subscribe", "topic1", "client1", "--offset", "2147483648"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Non-numeric value for --offset",
			args:     []string{endpoint, "subscribe", "topic1", "client1", "--offset", "abc"},
			wantCode: protocol.ExitGeneral,
		},

		// 3. NAME FORMAT VALIDATION ERRORS (ExitGeneral)
		{
			name:     "Invalid topic name format",
			args:     []string{endpoint, "subscribe", "bad topic!", "client1"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Invalid subscriber name format",
			args:     []string{endpoint, "subscribe", "topic1", "bad client!"},
			wantCode: protocol.ExitGeneral,
		},

		// 4. IPC ENDPOINT ERRORS (ExitGeneral)
		{
			name:     "Non-existent IPC endpoint for subscribe",
			args:     []string{"/tmp/non_existent_fifo_999999", "subscribe", "topic1", "client1"},
			wantCode: protocol.ExitGeneral,
		},

		// 5. SERVER PROTOCOL ERRORS
		{
			name:     "Subscribe to non-existent topic returns error",
			args:     []string{endpoint, "subscribe", "non_existent_topic_xyz", "client1"},
			wantCode: protocol.ExitTopic, // topic error
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

	// 7. LIVE SUBSCRIPTION & GRACEFUL SHUTDOWN (SIGINT)
	t.Run("Valid subscribe and graceful exit on SIGINT", func(t *testing.T) {
		topicName := fmt.Sprintf("sub_test_%d", time.Now().UnixNano()%100000)
		clientName := fmt.Sprintf("cli_%d", time.Now().UnixNano()%100000)

		// Asks the server to create the topic
		if code := Run([]string{endpoint, "create", topicName}); code != protocol.ExitOK {
			t.Fatalf("Failed to create test topic %s, got code %d", topicName, code)
		}

		// starts a goroutine as a subscriber to the topic as clientName to 'user.' prefix at offset 0
		// once done receives the exit code of Run, the goroutine ends.
		done := make(chan int, 1)
		go func() {
			done <- Run([]string{endpoint, "subscribe", topicName, clientName, "--prefix", "user.", "--offset", "0"})
		}()

		// let the time to the client to subscribe and to listen to the specific fifo
		time.Sleep(150 * time.Millisecond)

		// Sends SIGINT signal to the current processus to trigger clean shutdown
		_ = syscall.Kill(os.Getpid(), syscall.SIGINT)

		select {
		case gotCode := <-done:
			if gotCode != protocol.ExitOK {
				t.Errorf("Subscribe exited with code %d on SIGINT, want %d", gotCode, protocol.ExitOK)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Subscribe did not exit in time after receiving SIGINT")
		}
	})
}
