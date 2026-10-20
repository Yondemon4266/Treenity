package main

import (
	"os"

	"treenity/internal/client"
)

func main() {
	os.Exit(client.Run(os.Args[1:]))
}
