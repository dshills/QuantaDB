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

	// Test different queries on orders table
	queries := []string{
		"SELECT * FROM orders",
		"SELECT o_orderkey FROM orders",
		"SELECT 1 FROM orders",
	}

	for i, query := range queries {
		fmt.Printf("\nTest %d: %s\n", i+1, query)
		rows, err := db.Query(query)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		count := 0
		for rows.Next() {
			count++
			if count == 1 {
				fmt.Println("  Found at least one row")
			}
		}
		rows.Close()
		fmt.Printf("  Total rows: %d\n", count)
	}

	// Try count on a table we know has data
	fmt.Println("\nTesting COUNT on customer table (known to have 1500 rows):")
	var custCount int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&custCount)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Count: %d\n", custCount)
	}
}
