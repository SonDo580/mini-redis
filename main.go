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
		// Read message from client
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(value)

		// Respond to client
		writer := NewWriter(conn)
		writer.Write(Value{typ: "string", str: "OK"})
	}
}
