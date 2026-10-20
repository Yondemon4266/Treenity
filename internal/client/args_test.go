package client

import (
	"testing"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		// --- SUCCESS CASES ---
		{
			name:    "Valid List Command",
			args:    []string{"/tmp/server.123", "list"},
			wantErr: false,
		},
		{
			name:    "Valid Create Command",
			args:    []string{"/tmp/server.123", "create", "user_events"},
			wantErr: false,
		},
		{
			name:    "Valid Produce Command with --raw",
			args:    []string{"/tmp/server.123", "produce", "user_events", "--raw"},
			wantErr: false,
		},
		{
			name:    "Valid Subscribe Command with all options",
			args:    []string{"/tmp/server.123", "subscribe", "topic1", "client1", "--prefix", "user.", "--offset", "10", "--raw"},
			wantErr: false,
		},
		{
			name:    "Valid Info Command",
			args:    []string{"/tmp/server.123", "info", "client0"},
			wantErr: false,
		},

		// --- ERRORS GLOBAL ARGUMENTS ---
		{
			name:    "Missing All Arguments",
			args:    []string{},
			wantErr: true,
		},
		{
			name:    "Missing Command",
			args:    []string{"/tmp/server.123"},
			wantErr: true,
		},
		{
			name:    "Unknown Command",
			args:    []string{"/tmp/server.123", "unknown_cmd"},
			wantErr: true,
		},

		// --- ERRORS OF NAME VALIDATION (Regex) ---
		{
			name:    "Invalid Topic Name (Spaces)",
			args:    []string{"/tmp/server.123", "create", "bad topic"},
			wantErr: true,
		},
		{
			name:    "Invalid Topic Name (Special chars)",
			args:    []string{"/tmp/server.123", "create", "topic/slash"},
			wantErr: true,
		},
		{
			name:    "Invalid Client Name (Too long)",
			args:    []string{"/tmp/server.123", "info", "this_is_a_very_long_client_name_exceeding_thirty_two_chars"},
			wantErr: true,
		},
		{
			name:    "Empty Topic Name",
			args:    []string{"/tmp/server.123", "create", ""},
			wantErr: true,
		},

		// --- ERRORS ON LIST ---
		{
			name:    "List with unexpected argument",
			args:    []string{"/tmp/server.123", "list", "extra_arg"},
			wantErr: true,
		},

		// --- ERRORS ON SUBSCRIBE & OPTIONS ---
		{
			name:    "Subscribe missing subscriber name",
			args:    []string{"/tmp/server.123", "subscribe", "topic1"},
			wantErr: true,
		},
		{
			name:    "Subscribe with missing value for --prefix",
			args:    []string{"/tmp/server.123", "subscribe", "topic1", "client1", "--prefix"},
			wantErr: true,
		},
		{
			name:    "Subscribe with missing value for --offset",
			args:    []string{"/tmp/server.123", "subscribe", "topic1", "client1", "--offset"},
			wantErr: true,
		},
		{
			name:    "Subscribe with negative offset",
			args:    []string{"/tmp/server.123", "subscribe", "topic1", "client1", "--offset", "-1"},
			wantErr: true,
		},
		{
			name:    "Subscribe with offset exceeding MaxOffset",
			args:    []string{"/tmp/server.123", "subscribe", "topic1", "client1", "--offset", "9999999999"},
			wantErr: true,
		},
		{
			name:    "Subscribe with non-numeric offset",
			args:    []string{"/tmp/server.123", "subscribe", "topic1", "client1", "--offset", "abc"},
			wantErr: true,
		},
		{
			name:    "Subscribe with unknown flag",
			args:    []string{"/tmp/server.123", "subscribe", "topic1", "client1", "--unknown"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseArgs(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseArgs(%v) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			}
		})
	}
}
