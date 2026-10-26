package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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

func runProducer(c *conn, topic string, raw bool) int {
	if _, code := c.ask(protocol.Request{Op: protocol.OpCheck, Topic: topic}); code != protocol.ExitOK {
		return code
	}
	reader := bufio.NewReader(os.Stdin)
	scanner := bufio.NewScanner(os.Stdin)

	for {
		var key, value []byte

		if raw {
			// Reading message in binary
			msg, err := protocol.ReadRawMessage(reader, false)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break // normal end stdin
				}
				printError("invalid raw message: %v", err)
				return protocol.ExitGeneral
			}
			key, value = msg.Key, msg.Value
		} else {
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					printError("stdin read error: %v", err)
					return protocol.ExitGeneral
				}
				break // normal end stdin
			}
			var err error
			key, value, err = protocol.ParseTextMessage(scanner.Text())
			if err != nil {
				printError("%v", err)
				return protocol.ExitGeneral
			}
		}

		req := protocol.Request{
			Op:    protocol.OpProduce,
			Topic: topic,
			Key:   key,
			Value: value,
		}
		if _, code := c.ask(req); code != protocol.ExitOK {
			return code
		}
	}

	return protocol.ExitOK
}
