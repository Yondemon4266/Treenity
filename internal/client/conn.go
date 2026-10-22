package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"syscall"
	"time"
	"treenity/internal/fifo"
	"treenity/internal/protocol"
)

type conn struct {
	endpoint string
	toServer *os.File // write end of the main FIFO

	replyPath   string
	replyPipe   *os.File
	replyReader *bufio.Reader
}

func connect(endpoint string) (*conn, int) {
	// check if the endpoint exists and if it is a FIFO
	if !fifo.IsFIFO(endpoint) {
		printError("invalid IPC identifier: %s", endpoint)
		return nil, protocol.ExitGeneral
	}
	// create the specific return FIFO for the server to reply
	replyPath := protocol.ReplyPath(endpoint, os.Getpid())
	err := syscall.Mkfifo(replyPath, 0600)
	if err != nil {
		return nil, protocol.ExitGeneral
	}
	// opens the reply FIFO file in READ-WRITE so it doesn't block the thread
	replyPipe, err := os.OpenFile(replyPath, os.O_RDWR, 0)
	if err != nil {
		os.Remove(replyPath)
		printError("cannot open reply FIFO: %v", err)
		return nil, protocol.ExitGeneral
	}

	// opens the main FIFO communication in WRITE ONLY and NONBLOCK => fails directly if no reader
	toServer, err := os.OpenFile(endpoint, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		os.Remove(replyPath)
		replyPipe.Close()
		printError("cannot connect to server: %v", err)
		return nil, protocol.ExitGeneral
	}

	c := &conn{
		endpoint:    endpoint,
		toServer:    toServer,
		replyPath:   replyPath,
		replyPipe:   replyPipe,
		replyReader: bufio.NewReader(replyPipe),
	}
	return c, protocol.ExitOK
}

func (c *conn) send(req protocol.Request) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	// Many clients write to the main FIFO at the same time. Only writes of at most
	// 4096 bytes are guaranteed not to interleave with other clients' data.
	if len(data) > protocol.MaxLine {
		return errors.New("request too large")
	}
	_, err = c.toServer.Write(data)
	return err
}

func (c *conn) ask(req protocol.Request) (protocol.Response, int) {
	var resp protocol.Response
	req.Reply = c.replyPath

	if err := c.send(req); err != nil {
		printError("lost connection to server: %v", err)
		return resp, protocol.ExitIPC
	}

	// reads for 5 seconds, if no response from the server, return exitIPC
	c.replyPipe.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := c.replyReader.ReadBytes('\n')
	if err != nil {
		printError("no answer from server: %v", err)
		return resp, protocol.ExitIPC
	}

	// transform response into protocol.Response struct
	err = json.Unmarshal(line, &resp)
	if err != nil {
		printError("invalid server response: %v", err)
		return resp, protocol.ExitIPC
	}
	if resp.Code != protocol.ExitOK {
		printError("%s", resp.Error)
		return resp, resp.Code
	}

	return resp, protocol.ExitOK
}

func (c *conn) close() {
	c.toServer.Close()
	c.replyPipe.Close()
	os.Remove(c.replyPath)
}
