package client

import (
	"os"
	"path/filepath"
	"testing"

	"treenity/internal/protocol"
)

func TestListWithLiveServer(t *testing.T) {
	endpoint := os.Getenv("TREENITY_IPC")
	if endpoint == "" {
		t.Skip("TREENITY_IPC not set: run with TREENITY_IPC=/tmp/treenity.server.xxx go test -v ./...")
	}

	tests := []struct {
		name     string
		args     []string
		wantCode int
	}{
		// 1. SUCCESS ON LIVE SERVER (listing topics)
		{
			name:     "Valid list command execution",
			args:     []string{endpoint, "list"},
			wantCode: protocol.ExitOK,
		},

		// 2. ARGUMENT ERRORS (ExitGeneral) - list takes no extra arguments
		{
			name:     "Extra unexpected argument for list",
			args:     []string{endpoint, "list", "extra_topic_arg"},
			wantCode: protocol.ExitGeneral,
		},

		// 3. IPC ENDPOINT ERRORS (ExitGeneral)
		{
			name:     "Non-existent IPC endpoint for list",
			args:     []string{"/tmp/non_existent_fifo_999999", "list"},
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

	// 4. TEST WITH A REGULAR FILE INSTEAD OF A FIFO
	t.Run("Regular file instead of FIFO returns ExitGeneral for list", func(t *testing.T) {
		regularFile := filepath.Join(t.TempDir(), "regular_file")
		_ = os.WriteFile(regularFile, []byte("test"), 0600)

		gotCode := Run([]string{regularFile, "list"})
		if gotCode != protocol.ExitGeneral {
			t.Errorf("Run with regular file = code %d, want %d", gotCode, protocol.ExitGeneral)
		}
	})
}
