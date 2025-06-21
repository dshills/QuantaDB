package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	// Simple TCP connection test
	fmt.Println("Testing basic TCP connection to QuantaDB...")
	
	conn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()
	
	fmt.Println("TCP connection successful!")
	
	// Give it a moment
	time.Sleep(100 * time.Millisecond)
	
	// Try to read any initial response
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("Read error: %v\n", err)
	} else {
		fmt.Printf("Received %d bytes\n", n)
	}
}