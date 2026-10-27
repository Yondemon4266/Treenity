package client

import (
	"fmt"
	"os"
	"testing"
	"time"

	"treenity/internal/protocol"
)

func TestCreateWithLiveServer(t *testing.T) {
	endpoint := os.Getenv("TREENITY_IPC")
	if endpoint == "" {
		t.Skip("TREENITY_IPC not set: run with TREENITY_IPC=/tmp/treenity.server.xxx go test -v ./...")
	}

	// Unique topic name to prevent failures on subsequent test runs due to existing topics
	uniqueTopic := fmt.Sprintf("topic_%d", time.Now().UnixNano()%1000000)

	tests := []struct {
		name     string
		args     []string
		wantCode int
	}{
		// 1. SUCCESS ON LIVE SERVER
		{
			name:     "Valid topic creation",
			args:     []string{endpoint, "create", uniqueTopic},
			wantCode: protocol.ExitOK,
		},

		// 2. ERROR RETURNED BY SERVER (Duplicate)
		{
			name:     "Duplicate topic creation (already exists on server)",
			args:     []string{endpoint, "create", uniqueTopic},
			wantCode: protocol.ExitTopic,
		},

		// 3. ARGUMENT AND FORMAT ERRORS (ExitGeneral)
		{
			name:     "Missing topic name",
			args:     []string{endpoint, "create"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Extra unexpected argument",
			args:     []string{endpoint, "create", "valid_topic", "extra_arg"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Invalid topic name with spaces",
			args:     []string{endpoint, "create", "bad topic"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Invalid topic name with slash",
			args:     []string{endpoint, "create", "bad/topic"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Empty topic name",
			args:     []string{endpoint, "create", ""},
			wantCode: protocol.ExitGeneral,
		},

		// 4. IPC ENDPOINT ERRORS (ExitGeneral)
		{
			name:     "Non-existent IPC endpoint",
			args:     []string{"/tmp/non_existent_fifo_999999", "create", "topic1"},
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

}
