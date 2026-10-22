package client

import (
	"fmt"
	"treenity/internal/protocol"
)

func runCreate(c *conn, topic string) int {
	_, code := c.ask(protocol.Request{Op: protocol.OpCreate, Topic: topic})
	if code != protocol.ExitOK {
		return code
	}
	fmt.Println("topic created")
	return protocol.ExitOK
}
