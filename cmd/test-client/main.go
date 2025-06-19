package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	// Connect to server
	conn, err := net.Dial("tcp", "localhost:5433")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to QuantaDB server")

	// Read startup message response
	reader := bufio.NewReader(conn)
	buf := make([]byte, 1024)
	n, err := reader.Read(buf)
	if err != nil {
		fmt.Printf("Failed to read: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Received %d bytes\n", n)

	// For now, just test the connection
	fmt.Println("Connection test successful!")
}
