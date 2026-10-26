package client

import (
	"errors"
	"fmt"
	"strconv"

	"treenity/internal/protocol"
)

const usage = `usage:
  ./client <ipc_identifier> create <topic_name>
  ./client <ipc_identifier> list
  ./client <ipc_identifier> produce <topic_name> [--raw]
  ./client <ipc_identifier> subscribe <topic_name> <subscriber_name> [--prefix <prefix>] [--offset <offset>] [--raw]
  ./client <ipc_identifier> info <subscriber_name>`

// options holds the parsed command-line arguments.
type options struct {
	endpoint   string // <ipc_identifier>
	command    string // create / list / produce / subscribe / info
	topic      string
	clientName string // subscriber clientName

	prefix    string
	offset    int
	hasOffset bool // whether the user gave --offset
	raw       bool
}

// parseArgs parses the arguments. Any argument error makes the program exit with code 1.
func parseArgs(args []string) (options, error) {
	var opts options
	if len(args) < 2 {
		return opts, errors.New("not enough arguments")
	}
	opts.endpoint = args[0]
	opts.command = args[1]
	rest := args[2:]

	var positionalCount int
	switch opts.command {
	case "list":
		positionalCount = 0
	case "create", "produce", "info":
		positionalCount = 1
	case "subscribe":
		positionalCount = 2
	default:
		return opts, fmt.Errorf("unknown command %q", opts.command)
	}

	// if the number of arguments after endpoint and main command are under mandatory need
	// then return missing arguments
	if len(rest) < positionalCount {
		return opts, fmt.Errorf("%s: missing arguments", opts.command)
	}

	positional := rest[:positionalCount]
	flags := rest[positionalCount:]

	switch opts.command {
	case "create", "produce":
		opts.topic = positional[0]
	case "info":
		opts.clientName = positional[0]
	case "subscribe":
		opts.topic = positional[0]
		opts.clientName = positional[1]
	}

	// Step 2: parse the remaining --xxx options (in any order)
	for i := 0; i < len(flags); i++ {
		switch flags[i] {
		case "--raw":
			if opts.command != "produce" && opts.command != "subscribe" {
				return opts, errors.New("--raw is only for produce and subscribe")
			}
			opts.raw = true

		case "--prefix":
			if opts.command != "subscribe" || i+1 >= len(flags) {
				return opts, errors.New("--prefix needs a value and is only for subscribe")
			}
			i++ // move to the option's value
			opts.prefix = flags[i]
			if len(opts.prefix) > protocol.MaxPayload {
				return opts, errors.New("prefix is too long")
			}

		case "--offset":
			if opts.command != "subscribe" || i+1 >= len(flags) {
				return opts, errors.New("--offset needs a value and is only for subscribe")
			}
			i++
			n, err := strconv.Atoi(flags[i])
			if err != nil || n < 0 || n > protocol.MaxOffset {
				return opts, fmt.Errorf("invalid offset %q", flags[i])
			}
			opts.offset = n
			opts.hasOffset = true

		default:
			return opts, fmt.Errorf("unexpected argument %q", flags[i])
		}
	}

	// Step 3: check name formats
	if opts.command == "create" || opts.command == "produce" || opts.command == "subscribe" {
		if !protocol.ValidName(opts.topic) {

			return opts, fmt.Errorf("invalid topic clientName %q", opts.topic)
		}
	}
	if opts.command == "info" || opts.command == "subscribe" {
		if !protocol.ValidName(opts.clientName) {
			return opts, fmt.Errorf("invalid client clientName %q", opts.clientName)
		}
	}
	return opts, nil
}
