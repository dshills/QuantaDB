package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:5433")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Send SSL request
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, int32(8))          // length
	binary.Write(buf, binary.BigEndian, int32(80877103))   // SSL request code
	
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		fmt.Printf("Failed to send SSL request: %v\n", err)
		os.Exit(1)
	}

	// Read SSL response
	sslResp := make([]byte, 1)
	_, err = conn.Read(sslResp)
	if err != nil {
		fmt.Printf("Failed to read SSL response: %v\n", err)
		os.Exit(1)
	}

	if sslResp[0] == 'N' {
		fmt.Println("Server declined SSL")
	} else {
		fmt.Printf("Unexpected SSL response: %c\n", sslResp[0])
	}

	// Send startup message
	startupBuf := new(bytes.Buffer)
	
	// Build parameters
	params := map[string]string{
		"user":     "user",
		"database": "database",
	}
	
	paramBuf := new(bytes.Buffer)
	binary.Write(paramBuf, binary.BigEndian, int32(196608)) // protocol version
	
	for k, v := range params {
		paramBuf.WriteString(k)
		paramBuf.WriteByte(0)
		paramBuf.WriteString(v)
		paramBuf.WriteByte(0)
	}
	paramBuf.WriteByte(0) // final terminator
	
	// Write length then params
	binary.Write(startupBuf, binary.BigEndian, int32(paramBuf.Len()+4))
	startupBuf.Write(paramBuf.Bytes())
	
	_, err = conn.Write(startupBuf.Bytes())
	if err != nil {
		fmt.Printf("Failed to send startup: %v\n", err)
		os.Exit(1)
	}

	// Read response
	resp := make([]byte, 1024)
	n, err := conn.Read(resp)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Received %d bytes\n", n)
	fmt.Printf("Response: %v\n", resp[:n])
}