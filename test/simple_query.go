//go:build ignore

package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Simple Test ===")

	// Test 1: Simple COUNT
	fmt.Println("1. Simple COUNT:")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&count)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Success: %d rows\n", count)
	}

	// Test 2: Simple SELECT with single column
	fmt.Println("2. Simple SELECT one column:")
	rows, err := db.Query("SELECT c_mktsegment FROM customer LIMIT 5")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
		return
	}

	count = 0
	for rows.Next() {
		var segment string
		err = rows.Scan(&segment)
		if err != nil {
			fmt.Printf("  Scan error: %v\n", err)
			break
		}
		fmt.Printf("  %s\n", segment)
		count++
	}
	rows.Close()
	fmt.Printf("  Total: %d rows\n", count)
}
