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

	fmt.Println("=== Debug GROUP BY Test ===")

	// Test 1: Simple COUNT with GROUP BY - this worked before
	fmt.Println("Test 1: Simple COUNT(*) with GROUP BY (no explicit column in SELECT):")
	fmt.Println("Query: SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")

	rows, err := db.Query("SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Println("  Success!")
		count := 0
		for rows.Next() {
			var segmentCount int
			err = rows.Scan(&segmentCount)
			if err != nil {
				fmt.Printf("  Scan error: %v\n", err)
				break
			}
			fmt.Printf("    Group %d: %d rows\n", count+1, segmentCount)
			count++
		}
		rows.Close()
		fmt.Printf("  Total groups: %d\n", count)
	}

	fmt.Println()

	// Test 2: Explicit projection of just the aggregate
	fmt.Println("Test 2: Explicit projection of aggregate only:")
	fmt.Println("Query: SELECT COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")

	rows, err = db.Query("SELECT COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Println("  Success!")
		count := 0
		for rows.Next() {
			var segmentCount int
			err = rows.Scan(&segmentCount)
			if err != nil {
				fmt.Printf("  Scan error: %v\n", err)
				break
			}
			fmt.Printf("    Group %d: %d rows\n", count+1, segmentCount)
			count++
		}
		rows.Close()
		fmt.Printf("  Total groups: %d\n", count)
	}

	fmt.Println()

	// Test 3: Explicit projection of GROUP BY column only
	fmt.Println("Test 3: Explicit projection of GROUP BY column only:")
	fmt.Println("Query: SELECT c_mktsegment FROM customer GROUP BY c_mktsegment")

	rows, err = db.Query("SELECT c_mktsegment FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Println("  Success!")
		count := 0
		for rows.Next() {
			var segment string
			err = rows.Scan(&segment)
			if err != nil {
				fmt.Printf("  Scan error: %v\n", err)
				break
			}
			fmt.Printf("    Group %d: %s\n", count+1, segment)
			count++
		}
		rows.Close()
		fmt.Printf("  Total groups: %d\n", count)
	}

	fmt.Println("Done!")
}
