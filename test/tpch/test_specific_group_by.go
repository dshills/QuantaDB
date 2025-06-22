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

	fmt.Println("=== Specific GROUP BY Test ===")

	// Test the exact query that fails in TPC-H
	fmt.Println("Testing specific SELECT with GROUP BY:")
	fmt.Println("Query: SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment ORDER BY cnt DESC")

	rows, err := db.Query("SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment ORDER BY cnt DESC")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
		return
	}
	defer rows.Close()

	fmt.Println("  Results:")
	count := 0
	for rows.Next() {
		var segment string
		var segmentCount int
		err = rows.Scan(&segment, &segmentCount)
		if err != nil {
			fmt.Printf("  Scan error: %v\n", err)
			break
		}
		fmt.Printf("    %s: %d rows\n", segment, segmentCount)
		count++
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("  Row iteration error: %v\n", err)
		return
	}

	fmt.Printf("  Total groups: %d\n", count)
	fmt.Println("Done!")
}
