package main

import (
	"database/sql"
	"encoding/hex"
	"log"
	"net"
	"sync"

	_ "github.com/lib/pq"
)

type LoggingProxy struct {
	clientConn net.Conn
	serverConn net.Conn
	mu         sync.Mutex
}

func (p *LoggingProxy) Start() {
	go p.clientToServer()
	go p.serverToClient()
}

func (p *LoggingProxy) clientToServer() {
	buf := make([]byte, 4096)
	for {
		n, err := p.clientConn.Read(buf)
		if err != nil {
			log.Printf("Client read error: %v", err)
			return
		}

		log.Printf("CLIENT->SERVER (%d bytes):\n%s", n, hex.Dump(buf[:n]))

		_, err = p.serverConn.Write(buf[:n])
		if err != nil {
			log.Printf("Server write error: %v", err)
			return
		}
	}
}

func (p *LoggingProxy) serverToClient() {
	buf := make([]byte, 4096)
	for {
		n, err := p.serverConn.Read(buf)
		if err != nil {
			log.Printf("Server read error: %v", err)
			return
		}

		log.Printf("SERVER->CLIENT (%d bytes):\n%s", n, hex.Dump(buf[:n]))

		_, err = p.clientConn.Write(buf[:n])
		if err != nil {
			log.Printf("Client write error: %v", err)
			return
		}
	}
}

func main() {
	// Start proxy listener
	listener, err := net.Listen("tcp", ":5433")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("Proxy listening on :5433")

	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				log.Printf("Accept error: %v", err)
				continue
			}

			// Connect to real server
			server, err := net.Dial("tcp", "localhost:5432")
			if err != nil {
				log.Printf("Server connect error: %v", err)
				client.Close()
				continue
			}

			proxy := &LoggingProxy{
				clientConn: client,
				serverConn: server,
			}
			proxy.Start()
		}
	}()

	// Now connect via lib/pq through the proxy
	connStr := "host=localhost port=5433 user=postgres dbname=postgres sslmode=disable"
	log.Printf("Connecting with: %s", connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	log.Println("Pinging...")
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}

	log.Println("Success!")
}
