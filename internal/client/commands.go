package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
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

func runSubscribe(c *conn, opts options) int {
	// 1. Demande d'abonnement au serveur
	req := protocol.Request{
		Op:        protocol.OpSubscribe,
		Topic:     opts.topic,
		Client:    opts.clientName,
		Prefix:    []byte(opts.prefix),
		Offset:    opts.offset,
		HasOffset: opts.hasOffset,
	}
	resp, code := c.ask(req)
	if code != protocol.ExitOK {
		return code
	}

	// Open the FIFO given by the server in response of subscribe
	subPipe, err := os.Open(resp.IPC)
	if err != nil {
		printError("cannot open consumer FIFO: %v", err)
		return protocol.ExitIPC
	}
	defer subPipe.Close()

	fmt.Printf("subscribed to %s\n", opts.topic)

	// SIGINT and SIGTERM handling is mandatory when listening for responses to end the process when needed
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	// Reading the FIFO in a Goroutine to not block the handling of Signals
	type readResult struct {
		delivery protocol.Delivery
		err      error
	}
	msgChan := make(chan readResult)

	go func() {
		reader := bufio.NewReader(subPipe)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				msgChan <- readResult{err: err}
				return
			}
			var d protocol.Delivery
			if err := json.Unmarshal(line, &d); err != nil {
				msgChan <- readResult{err: err}
				return
			}
			msgChan <- readResult{delivery: d}
		}
	}()

	// main loop to listen to the new messages arriving in msgChan (delivery or shutdown) or signals of disconnection
	// if signal of disconnection or shutdown message from server, we inform server that we disconnect with OpDisconnect
	for {
		select {
		// case of SIGINT, SIGTERM, we inform server that we disconnect cleanly
		case <-sigChan:
			_, _ = c.ask(protocol.Request{Op: protocol.OpDisconnect, Client: opts.clientName})
			return protocol.ExitOK

		// case of error in the message of the server
		case res := <-msgChan:
			if res.err != nil {
				printError("server connection lost: %v", res.err)
				return protocol.ExitIPC
			}

			msg := res.delivery

			if msg.Type == protocol.DeliveryShutdown {
				_, _ = c.ask(protocol.Request{Op: protocol.OpDisconnect, Client: opts.clientName})
				return protocol.ExitOK
			}

			// writes in the binary protocol format in stdout
			if opts.raw {
				if err := protocol.WriteRawMessage(os.Stdout, msg, true); err != nil {
					printError("failed to write raw message: %v", err)
					return protocol.ExitGeneral
				}
			} else {
				fmt.Print(protocol.FormatTextMessage(msg.Key, msg.Value))
			}

			// Send Acknowledgement that we received message at offset and send Offset + 1
			if err := c.send(protocol.Request{
				Op:     protocol.OpAck,
				Client: opts.clientName,
				Offset: int32(msg.Offset + 1),
			}); err != nil {
				printError("failed to send ack: %v", err)
				return protocol.ExitIPC
			}
		}
	}
}
