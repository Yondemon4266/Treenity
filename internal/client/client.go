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

	c, code := connect(opts.endpoint)
	if code != protocol.ExitOK {
		return code
	}

	defer c.close()
	switch opts.command {
	case protocol.OpCreate:
		return runCreate(c, opts.topic)
	case protocol.OpList:
		return runList(c)
	case protocol.OpInfo:
		return runInfo(c, opts.clientName)
	case protocol.OpProduce:
		return runProducer(c, opts.topic, opts.raw)
	}

	return protocol.ExitGeneral // parseArgs guarantees we never get here
}

// printError prints an error message on stderr (stdout is only for the output required by the subject).
func printError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
}
