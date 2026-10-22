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
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
	"time"

	"treenity/internal/fifo"
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

	if !fifo.IsFIFO(opts.endpoint) {
		printError("invalid IPC identifier: %s", opts.endpoint)
		return protocol.ExitGeneral
	}
	fmt.Printf("%+v\n", opts)

	switch opts.command {
	case "create":
		// create the specific return FIFO for the server to reply
		replyPath := protocol.ReplyPath(opts.endpoint, os.Getpid())
		err := syscall.Mkfifo(replyPath, 0600)
		if err != nil {
			return protocol.ExitGeneral
		}
		defer os.Remove(replyPath)
		// opens the reply FIFO file in READ-WRITE so it doesn't block the thread
		replyPipe, err := os.OpenFile(replyPath, os.O_RDWR, 0)
		if err != nil {
			printError("cannot open reply FIFO: %v", err)
			return protocol.ExitGeneral
		}
		defer replyPipe.Close()

		// opens the main FIFO communication in WRITE ONLY and NONBLOCK => fails directly if no reader
		toServer, err := os.OpenFile(opts.endpoint, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			printError("cannot connect to server: %v", err)
			return protocol.ExitGeneral
		}
		defer toServer.Close()

		// prepares the 'create' request for the server to treat it.
		req := protocol.Request{Op: protocol.OpCreate, Topic: opts.topic, Reply: replyPath}
		// converts it to bytes because processes can communicate only with bytes stream
		data, e := json.Marshal(req)
		if e != nil {
			return protocol.ExitGeneral
		}
		data = append(data, '\n')
		if len(data) > protocol.MaxLine {
			printError("request too large")
			return protocol.ExitGeneral
		}
		// writes the request transformed into bytes to the main IPC
		_, err = toServer.Write(data)
		if err != nil {
			return protocol.ExitIPC
		}

		reader := bufio.NewReader(replyPipe)
		// reads for 5 seconds, if no response from the server, return exitIPC
		replyPipe.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := reader.ReadBytes('\n')
		if err != nil {
			printError("no answer from server: %v", err)
			return protocol.ExitIPC
		}

		// transform response into protocol.Response
		var resp protocol.Response
		err = json.Unmarshal(line, &resp)
		if err != nil {
			printError("invalid server response: %v", err)
			return protocol.ExitIPC
		}
		if resp.Code != protocol.ExitOK {
			printError("%s", resp.Error)
			return resp.Code
		}
		return protocol.ExitOK
	}

	return protocol.ExitOK // parseArgs guarantees we never get here
}

// printError prints an error message on stderr (stdout is only for the output required by the subject).
func printError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
}
