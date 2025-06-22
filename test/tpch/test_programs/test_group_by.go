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

	fmt.Println("Testing GROUP BY queries...")
	fmt.Println("==========================")

	// Test 1: Simple COUNT without GROUP BY (known to work)
	fmt.Println("\n1. Simple COUNT (no GROUP BY):")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&count)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		fmt.Printf("   Success: %d rows\n", count)
	}

	// Test 2: Simple SELECT without GROUP BY
	fmt.Println("\n2. Simple SELECT with DISTINCT:")
	rows, err := db.Query("SELECT DISTINCT c_mktsegment FROM customer")
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		var segments []string
		for rows.Next() {
			var seg string
			if err := rows.Scan(&seg); err != nil {
				fmt.Printf("   Scan error: %v\n", err)
				break
			}
			segments = append(segments, seg)
		}
		rows.Close()
		fmt.Printf("   Success: Found %d distinct segments: %v\n", len(segments), segments)
	}

	// Test 3: Simple GROUP BY without COUNT
	fmt.Println("\n3. Simple GROUP BY (no aggregation):")
	rows, err = db.Query("SELECT c_mktsegment FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		var segments []string
		for rows.Next() {
			var seg string
			if err := rows.Scan(&seg); err != nil {
				fmt.Printf("   Scan error: %v\n", err)
				break
			}
			segments = append(segments, seg)
		}
		rows.Close()
		fmt.Printf("   Success: Found %d segments: %v\n", len(segments), segments)
	}

	// Test 4: GROUP BY with COUNT
	fmt.Println("\n4. GROUP BY with COUNT:")
	rows, err = db.Query("SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		var results []string
		for rows.Next() {
			var seg string
			var cnt int
			if err := rows.Scan(&seg, &cnt); err != nil {
				fmt.Printf("   Scan error: %v\n", err)
				break
			}
			results = append(results, fmt.Sprintf("%s:%d", seg, cnt))
		}
		rows.Close()
		fmt.Printf("   Success: %v\n", results)
	}
}
