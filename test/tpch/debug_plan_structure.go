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

	fmt.Println("=== Debug Plan Structure ===")

	// Test working query that returns both columns but no explicit SELECT
	fmt.Println("\nTest 1: SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")
	rows, err := db.Query("SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		defer rows.Close()
		cols, _ := rows.Columns()
		fmt.Printf("  SUCCESS - Columns: %v\n", cols)
		count := 0
		for rows.Next() {
			count++
		}
		fmt.Printf("  Rows: %d\n", count)
	}

	// Test 2: Explicit column selection causing the error
	fmt.Println("\nTest 2: SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")
	rows2, err := db.Query("SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		defer rows2.Close()
		cols, _ := rows2.Columns()
		fmt.Printf("  SUCCESS - Columns: %v\n", cols)
	}

	// Test 3: Try different column order
	fmt.Println("\nTest 3: SELECT COUNT(*) as cnt, c_mktsegment FROM customer GROUP BY c_mktsegment")
	rows3, err := db.Query("SELECT COUNT(*) as cnt, c_mktsegment FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		defer rows3.Close()
		cols, _ := rows3.Columns()
		fmt.Printf("  SUCCESS - Columns: %v\n", cols)
	}

	fmt.Println("\n=== End Test ===")
}
