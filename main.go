package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	// Start a TCP listener on Redis default port)
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Accept a single client connection
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	// Close connection once finished
	defer conn.Close()

	// Infinite loop: receive commands from client and respond
	for {
		buffer := make([]byte, 1024)

		// Read message from client
		_, err = conn.Read(buffer)
		if err != nil {
			// Client disconnected normally
			if err == io.EOF {
				break
			}

			fmt.Println("Error reading from client: ", err.Error())
			os.Exit(1)
		}

		// Respond to client
		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error writing to client: ", err.Error())
			os.Exit(1)
		}
	}
}
