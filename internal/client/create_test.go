package client

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"treenity/internal/protocol"
)

func TestCreateWithLiveServer(t *testing.T) {
	endpoint := os.Getenv("TREENITY_IPC")
	if endpoint == "" {
		t.Skip("TREENITY_IPC non défini : lancez avec TREENITY_IPC=/tmp/treenity.server.xxx go test -v ./...")
	}

	// Nom unique pour éviter qu'un second lancement de 'go test' échoue à cause d'un topic déjà créé
	uniqueTopic := fmt.Sprintf("topic_%d", time.Now().UnixNano()%1000000)

	tests := []struct {
		name     string
		args     []string
		wantCode int
	}{
		// 1. SUCCÈS SUR LE VRAI SERVEUR
		{
			name:     "Valid topic creation",
			args:     []string{endpoint, "create", uniqueTopic},
			wantCode: protocol.ExitOK,
		},

		// 2. ERREUR RENVOYÉE PAR LE SERVEUR (Doublon)
		{
			name:     "Duplicate topic creation (already exists on server)",
			args:     []string{endpoint, "create", uniqueTopic},
			wantCode: protocol.ExitTopic,
		},

		// 3. ERREURS D'ARGUMENTS ET DE FORMAT (ExitGeneral)
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

		// 4. ERREURS D'ENDPOINT IPC (ExitGeneral)
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

	// 5. TEST AVEC UN FICHIER NORMAL AU LIEU D'UN FIFO
	t.Run("Regular file instead of FIFO returns ExitGeneral", func(t *testing.T) {
		regularFile := filepath.Join(t.TempDir(), "regular_file")
		_ = os.WriteFile(regularFile, []byte("test"), 0600)

		gotCode := Run([]string{regularFile, "create", "topic1"})
		if gotCode != protocol.ExitGeneral {
			t.Errorf("Run with regular file = code %d, want %d", gotCode, protocol.ExitGeneral)
		}
	})
}
