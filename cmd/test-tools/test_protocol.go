//go:build ignore

package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func main() {
	fmt.Println("Testing PostgreSQL protocol handshake...")

	// Connect to QuantaDB
	conn, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to server")

	// Send SSL request
	sslRequest := make([]byte, 8)
	binary.BigEndian.PutUint32(sslRequest[0:4], 8)        // Length
	binary.BigEndian.PutUint32(sslRequest[4:8], 80877103) // SSL request code

	fmt.Println("Sending SSL request...")
	if _, err := conn.Write(sslRequest); err != nil {
		fmt.Printf("Failed to send SSL request: %v\n", err)
		return
	}

	// Read SSL response
	sslResponse := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(sslResponse); err != nil {
		fmt.Printf("Failed to read SSL response: %v\n", err)
		return
	}

	fmt.Printf("SSL response: %c\n", sslResponse[0])

	if sslResponse[0] != 'N' {
		fmt.Println("Unexpected SSL response")
		return
	}

	// Now send the actual startup message
	// Build startup parameters
	params := map[string]string{
		"user":             "postgres",
		"database":         "postgres",
		"application_name": "test_protocol",
	}

	// Calculate message size
	msgSize := 4 + 4 // length + version
	for k, v := range params {
		msgSize += len(k) + 1 + len(v) + 1
	}
	msgSize += 1 // final null terminator

	// Build message
	msg := make([]byte, msgSize)
	pos := 0

	// Length (includes itself)
	binary.BigEndian.PutUint32(msg[pos:], uint32(msgSize))
	pos += 4

	// Protocol version
	binary.BigEndian.PutUint32(msg[pos:], 196608) // 3.0
	pos += 4

	// Parameters
	for k, v := range params {
		copy(msg[pos:], k)
		pos += len(k)
		msg[pos] = 0
		pos++

		copy(msg[pos:], v)
		pos += len(v)
		msg[pos] = 0
		pos++
	}

	// Final null terminator
	msg[pos] = 0

	fmt.Println("Sending startup message...")
	if _, err := conn.Write(msg); err != nil {
		fmt.Printf("Failed to send startup message: %v\n", err)
		return
	}

	// Read authentication response
	fmt.Println("Waiting for authentication response...")

	// Keep reading messages until we get ReadyForQuery
	for {
		msgType := make([]byte, 1)
		if _, err := conn.Read(msgType); err != nil {
			fmt.Printf("Failed to read message type: %v\n", err)
			return
		}

		// Read message length
		lengthBuf := make([]byte, 4)
		if _, err := conn.Read(lengthBuf); err != nil {
			fmt.Printf("Failed to read message length: %v\n", err)
			return
		}

		msgLen := binary.BigEndian.Uint32(lengthBuf) - 4
		fmt.Printf("Message: Type=%c Length=%d\n", msgType[0], msgLen)

		// Read message data
		if msgLen > 0 {
			data := make([]byte, msgLen)
			if _, err := conn.Read(data); err != nil {
				fmt.Printf("Failed to read message data: %v\n", err)
				return
			}

			// Print some data for debugging
			if msgType[0] == 'S' { // ParameterStatus
				// Find null terminators
				var parts []string
				start := 0
				for i := 0; i < len(data); i++ {
					if data[i] == 0 {
						parts = append(parts, string(data[start:i]))
						start = i + 1
					}
				}
				if len(parts) >= 2 {
					fmt.Printf("  ParameterStatus: %s = %s\n", parts[0], parts[1])
				}
			}
		}

		// Check if we got ReadyForQuery
		if msgType[0] == 'Z' {
			fmt.Println("Received ReadyForQuery - server is ready!")
			break
		}
	}

	// Now send a simple query
	fmt.Println("\nSending simple query: SELECT 1")
	query := "SELECT 1"
	queryMsg := make([]byte, 5+len(query)+1)
	queryMsg[0] = 'Q' // Query message
	binary.BigEndian.PutUint32(queryMsg[1:5], uint32(4+len(query)+1))
	copy(queryMsg[5:], query)
	queryMsg[5+len(query)] = 0 // Null terminator

	if _, err := conn.Write(queryMsg); err != nil {
		fmt.Printf("Failed to send query: %v\n", err)
		return
	}

	// Read query response
	fmt.Println("Reading query response...")
	for {
		msgType := make([]byte, 1)
		if _, err := conn.Read(msgType); err != nil {
			fmt.Printf("Failed to read response type: %v\n", err)
			return
		}

		lengthBuf := make([]byte, 4)
		if _, err := conn.Read(lengthBuf); err != nil {
			fmt.Printf("Failed to read response length: %v\n", err)
			return
		}

		msgLen := binary.BigEndian.Uint32(lengthBuf) - 4
		fmt.Printf("Response: Type=%c Length=%d\n", msgType[0], msgLen)

		// Read message data
		if msgLen > 0 {
			data := make([]byte, msgLen)
			if _, err := conn.Read(data); err != nil {
				fmt.Printf("Failed to read response data: %v\n", err)
				return
			}
		}

		// Stop when we get ReadyForQuery
		if msgType[0] == 'Z' {
			fmt.Println("Query completed successfully!")
			break
		}
	}
}
