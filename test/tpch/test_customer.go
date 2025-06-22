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

	fmt.Println("=== Customer Table Test ===")

	// Test simple count
	fmt.Println("1. Simple COUNT:")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&count)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Success: %d rows\n", count)
	}

	fmt.Println("2. GROUP BY test:")
	rows, err := db.Query("SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
		return
	}
	defer rows.Close()

	fmt.Println("  Results:")
	for rows.Next() {
		var segment string
		var segmentCount int
		err = rows.Scan(&segment, &segmentCount)
		if err != nil {
			fmt.Printf("  Scan error: %v\n", err)
			break
		}
		fmt.Printf("    %s: %d\n", segment, segmentCount)
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("  Row iteration error: %v\n", err)
	}
}
