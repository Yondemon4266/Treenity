// Package protocol is the contract between the server and the client.
//
// Team rule: any change to this file goes in its own commit
// (e.g. "feat(protocol): add produce op") and the other teammate is told.
//
// Format: every Request / Response / Delivery is one JSON line ending with '\n'.
package protocol

import (
	"fmt"
	"regexp"
)

// ---- 1. Limits (subject VI.4) ----
const (
	MaxPayload = 1024 // key + value max, in bytes
	MaxLine    = 4096
	MaxOffset  = 1<<31 - 1 // int32 max limit for offset (incrementing 32-bit integer)
)

// ---- 2. Exit codes (subject VIII.4) ----
// Precedence when several errors apply: 1 > 3 > 2
const (
	ExitOK      = 0
	ExitGeneral = 1 // bad arguments, invalid name, invalid IPC identifier
	ExitTopic   = 2 // topic / client missing or duplicated
	ExitIPC     = 3 // communication with the server lost
)

// ---- 3. Operations (subject VIII.1) ----
// The first five are client commands; check, ack and disconnect are used internally.
const (
	OpCreate     = "create"
	OpList       = "list"
	OpInfo       = "info"
	OpProduce    = "produce"
	OpSubscribe  = "subscribe"
	OpCheck      = "check"
	OpAck        = "ack"
	OpDisconnect = "disconnect"
)

// ---- 4. Delivery types (server -> consumer) ----
const (
	DeliveryMessage  = "message"  // a real message
	DeliveryShutdown = "shutdown" // the server is stopping: the consumer must exit with 0 (subject VI.8)
)

// Request: client -> server (written to the server's main FIFO).
type Request struct {
	Op        string `json:"op"`
	Reply     string `json:"reply,omitempty"` // path of the client's reply FIFO
	Topic     string `json:"topic,omitempty"`
	Client    string `json:"client,omitempty"`     // subscriber name
	Key       []byte `json:"key,omitempty"`        // produce
	Value     []byte `json:"value,omitempty"`      // produce
	HasOffset bool   `json:"has_offset,omitempty"` // subscribe: whether an offset was given
	Prefix    []byte `json:"prefix,omitempty"`     // subscribe: key prefix filter
	Offset    int32  `json:"offset,omitempty"`     // subscribe: start offset; ack: committed offset

}

// Response: server -> client (written to the client's reply FIFO).
type Response struct {
	Code   int         `json:"code"`             // 0 = OK, otherwise the client's exit code
	Error  string      `json:"error,omitempty"`  // the client prints it on stderr
	Topics []string    `json:"topics,omitempty"` // sorted list of the topics created on the server
	Info   *ClientInfo `json:"info,omitempty"`   // set only when an info request succeeds
	IPC    string      `json:"ipc,omitempty"`    // after subscribe: path of this consumer's dedicated FIFO
}

// ClientInfo: output of the info command. Fields required by the subject (VIII.1 / VIII.3).
type ClientInfo struct {
	Client string `json:"client"`
	Topic  string `json:"topic"`
	Offset int32  `json:"offset"`
	Prefix string `json:"prefix"`
	IPC    string `json:"ipc"`
}

// Delivery: server -> consumer. A message = key + value + offset (subject VI.4).
type Delivery struct {
	Type   string `json:"type"` // DeliveryMessage or DeliveryShutdown
	Offset int32  `json:"offset"`
	Key    []byte `json:"key,omitempty"`
	Value  []byte `json:"value,omitempty"`
}

// Topic and client names: ^[a-zA-Z0-9_.-]{1,32}$ (subject VI.5)
var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

func ValidName(name string) bool {
	return namePattern.MatchString(name)
}

// ---- FIFO paths ----
// Server main FIFO: /tmp/treenity.server.<pid_server>
// Reply FIFO:       /tmp/treenity.server.<pid_server>:reply:<pid_client>

func ServerPath(serverPID int) string {
	return fmt.Sprintf("/tmp/treenity.server.%d", serverPID)
}

func ReplyPath(endpoint string, clientPID int) string {
	return fmt.Sprintf("%s:reply:%d", endpoint, clientPID)
}
