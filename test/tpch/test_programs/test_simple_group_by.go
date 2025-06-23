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

	fmt.Println("=== Simple GROUP BY Test ===")

	// Test simpler GROUP BY without complex column names
	fmt.Println("Testing simple COUNT with GROUP BY:")
	fmt.Println("Query: SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")

	rows, err := db.Query("SELECT COUNT(*) FROM customer GROUP BY c_mktsegment")
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
