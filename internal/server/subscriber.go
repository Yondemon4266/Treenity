package server

import "os"

// subscriber is the server-side record of a consumer.
type subscriber struct {
	name   string
	topic  *topic
	prefix []byte
	offset int32
	path   string
	pipe   *os.File
	active bool
}
