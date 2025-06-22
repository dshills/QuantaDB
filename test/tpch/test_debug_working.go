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

	fmt.Println("=== Debug Working Query ===")

	// Test the query that worked before
	fmt.Println("Test: SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")

	rows, err := db.Query("SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
		return
	}
	defer rows.Close()

	// Check how many columns are actually returned
	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("  Error getting columns: %v\n", err)
		return
	}

	fmt.Printf("  Number of columns returned: %d\n", len(columns))
	fmt.Printf("  Column names: %v\n", columns)

	fmt.Println("Done!")
}
