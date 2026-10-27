package client

import (
	"os"
	"testing"

	"treenity/internal/protocol"
)

func TestInfoWithLiveServer(t *testing.T) {
	endpoint := os.Getenv("TREENITY_IPC")
	if endpoint == "" {
		t.Skip("TREENITY_IPC not set: run with TREENITY_IPC=/tmp/treenity.server.xxx go test -v ./...")
	}

	tests := []struct {
		name     string
		args     []string
		wantCode int
	}{
		// 1. ARGUMENT ERRORS (ExitGeneral) - info needs only the name of a client
		{
			name:     "Missing subscriber name for info",
			args:     []string{endpoint, "info"},
			wantCode: protocol.ExitGeneral,
		},
		{
			name:     "Extra unexpected argument for info",
			args:     []string{endpoint, "info", "client1", "extra_arg"},
			wantCode: protocol.ExitGeneral,
		},

		// 2. INVALID CLIENT NAME FORMAT (ExitGeneral)
		{
			name:     "Invalid subscriber name format for info",
			args:     []string{endpoint, "info", "bad client name!"},
			wantCode: protocol.ExitGeneral,
		},

		// 3. IPC ENDPOINT ERRORS (ExitGeneral)
		{
			name:     "Non-existent IPC endpoint for info",
			args:     []string{"/tmp/non_existent_fifo_999999", "info", "client1"},
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
