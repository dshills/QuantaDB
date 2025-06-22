package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
)

func main() {
	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Debugging GROUP BY Crash ===")

	// Test 1: Simple COUNT (known to work)
	fmt.Println("\n1. Testing simple COUNT (should work):")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&count)
	if err != nil {
		fmt.Printf("  ❌ Error: %v\n", err)
	} else {
		fmt.Printf("  ✅ Success: %d rows\n", count)
	}

	// Test 2: GROUP BY crash test
	fmt.Println("\n2. Testing GROUP BY (expected to crash):")
	rows, err := db.Query("SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  ❌ Error: %v\n", err)
		return
	}

	fmt.Println("  Processing results...")
	rowCount := 0
	for rows.Next() {
		var segment string
		var cnt int
		err = rows.Scan(&segment, &cnt)
		if err != nil {
			fmt.Printf("  ❌ Scan error: %v\n", err)
			break
		}
		fmt.Printf("    %s: %d\n", segment, cnt)
		rowCount++
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		fmt.Printf("  ❌ Row iteration error: %v\n", err)
	} else {
		fmt.Printf("  ✅ Success: %d groups\n", rowCount)
	}
}
