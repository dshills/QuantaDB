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

	fmt.Println("=== Test Projection Column Resolution Fix ===")

	// Test 1: Working query (only aggregates)
	fmt.Println("\nTest 1: SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")
	rows, err := db.Query("SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		defer rows.Close()
		fmt.Println("  SUCCESS: Query executed without errors")

		columns, _ := rows.Columns()
		fmt.Printf("  Columns: %v\n", columns)

		count := 0
		for rows.Next() {
			var cnt int
			rows.Scan(&cnt)
			count++
		}
		fmt.Printf("  Returned %d rows\n", count)
	}

	// Test 2: Failing query (explicit column in projection)
	fmt.Println("\nTest 2: SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")
	rows2, err := db.Query("SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		defer rows2.Close()
		fmt.Println("  SUCCESS: Query executed without errors")

		columns, _ := rows2.Columns()
		fmt.Printf("  Columns: %v\n", columns)

		count := 0
		for rows2.Next() {
			var segment string
			var cnt int
			rows2.Scan(&segment, &cnt)
			fmt.Printf("  Row %d: %s = %d\n", count+1, segment, cnt)
			count++
		}
		fmt.Printf("  Total rows: %d\n", count)
	}

	fmt.Println("\n=== End Test ===")
}
