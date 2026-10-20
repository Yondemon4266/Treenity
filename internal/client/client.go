// Package client implements every subcommand of ./client.
//
// Files:
//
//	client.go     entry point Run: parse args -> connect -> dispatch to a command (this file)
//	args.go       command-line argument parsing
//	conn.go       the connection to the server: send requests, wait for replies
//	commands.go   the three simple commands: create / list / info
//	produce.go    producer: read stdin, send messages one by one
//	subscribe.go  consumer: receive messages, print them, ack them
//	raw.go        reading / writing the --raw binary format
//
// Convention: every command function returns an int, which is the program's exit code.
// On error, the function itself prints the reason on stderr.
package client

import (
	"fmt"
	"os"

	"treenity/internal/protocol"
)

// Run is the whole client program. args does not include the program name. Returns the exit code.
func Run(args []string) int {
	opts, err := parseArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		fmt.Fprintln(os.Stderr, usage)
		return protocol.ExitGeneral
	}
	fmt.Println(opts)

	return protocol.ExitOK // parseArgs guarantees we never get here
}

// printError prints an error message on stderr (stdout is only for the output required by the subject).
func printError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
}
