package main

import (
	"fmt"
	"net"
)

func main() {
	// Create new server
	const PORT string = ":6379"
	listener, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Listening on port ", PORT)

	// Listen for connections
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(value)

		// Respond to client
		conn.Write([]byte("+PONG\r\n"))
	}
}
