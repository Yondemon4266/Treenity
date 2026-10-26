package client

import (
	"encoding/json"
	"fmt"
	"strings"
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

func runList(c *conn) int {
	resp, code := c.ask(protocol.Request{Op: protocol.OpList})
	if code != protocol.ExitOK {
		return code
	}
	if len(resp.Topics) == 0 {
		fmt.Println("no topic created yet")
	} else {
		formatted_topics := strings.Join(resp.Topics, ",")
		fmt.Println(formatted_topics)
	}
	return protocol.ExitOK
}

func runInfo(c *conn, client string) int {
	resp, code := c.ask(protocol.Request{Op: protocol.OpInfo, Client: client})
	if code != protocol.ExitOK {
		return code
	}

	jsonData, err := json.Marshal(resp)
	if err != nil {
		printError("failed to marshal json response: %v", err)
		return protocol.ExitGeneral
	}
	fmt.Println(string(jsonData))

	return protocol.ExitOK
}
