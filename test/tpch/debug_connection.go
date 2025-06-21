package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	
	_ "github.com/lib/pq"
)

func main() {
	// Enable debug output
	os.Setenv("PGLOGLEVEL", "2")
	
	// Build connection string
	connStr := "host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
	fmt.Printf("Connecting with: %s\n", connStr)
	
	// Try to connect
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()
	
	fmt.Println("sql.Open succeeded, now pinging...")
	
	// Try to ping
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}
	
	fmt.Println("Ping successful!")
	
	// Try a simple query
	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	
	fmt.Printf("Query result: %d\n", result)
}