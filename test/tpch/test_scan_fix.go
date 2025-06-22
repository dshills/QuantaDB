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

	fmt.Println("=== Fixed Scan Test ===")

	// Test with proper scan arguments
	fmt.Println("Test: SELECT COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")

	rows, err := db.Query("SELECT COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")
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

	// Scan with the right number of columns
	if len(columns) == 1 {
		fmt.Println("  Results (1 column):")
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
		fmt.Printf("  Total groups: %d\n", count)
	} else if len(columns) == 2 {
		fmt.Println("  Results (2 columns):")
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
		fmt.Printf("  Total groups: %d\n", count)
	}

	fmt.Println("Done!")
}
