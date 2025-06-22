//go:build ignore

package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	connStr := "host=localhost port=5433 user=postgres dbname=quantadb sslmode=disable"
	fmt.Printf("Connecting to: %s\n", connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Open error:", err)
	}
	defer db.Close()

	fmt.Println("sql.Open succeeded")

	// Try a simple query first
	rows, err := db.Query("SELECT 1")
	if err != nil {
		log.Fatal("Query error:", err)
	}
	rows.Close()
	fmt.Println("Query succeeded")

	// Now try Ping
	err = db.Ping()
	if err != nil {
		log.Fatal("Ping error:", err)
	}

	fmt.Println("Ping succeeded!")
}
