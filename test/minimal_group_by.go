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

	fmt.Println("=== Minimal GROUP BY Test ===")

	// Test the exact failing query
	fmt.Println("Testing: SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment")

	rows, err := db.Query("SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}

	fmt.Println("Query succeeded, processing results...")

	for rows.Next() {
		var segment string
		var count int

		err = rows.Scan(&segment, &count)
		if err != nil {
			fmt.Printf("Scan failed: %v\n", err)
			break
		}

		fmt.Printf("  %s: %d\n", segment, count)
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("Row iteration error: %v\n", err)
	}

	rows.Close()
	fmt.Println("Done.")
}
