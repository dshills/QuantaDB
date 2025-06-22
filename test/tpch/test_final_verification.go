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

	fmt.Println("=== Final Verification of GROUP BY Projection Fix ===")

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "Basic GROUP BY with COUNT(*)",
			query: "SELECT COUNT(*) FROM customer GROUP BY c_mktsegment",
		},
		{
			name:  "Explicit column selection (original failing case)",
			query: "SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment",
		},
		{
			name:  "Different column order",
			query: "SELECT COUNT(*) as cnt, c_mktsegment FROM customer GROUP BY c_mktsegment",
		},
		{
			name:  "Multiple aggregates",
			query: "SELECT c_mktsegment, COUNT(*) as cnt, COUNT(c_custkey) as key_cnt FROM customer GROUP BY c_mktsegment",
		},
		{
			name:  "With ORDER BY",
			query: "SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment ORDER BY cnt DESC",
		},
	}

	for i, test := range tests {
		fmt.Printf("\nTest %d: %s\n", i+1, test.name)
		fmt.Printf("Query: %s\n", test.query)

		rows, err := db.Query(test.query)
		if err != nil {
			fmt.Printf("  ❌ ERROR: %v\n", err)
			continue
		}
		defer rows.Close()

		// Get column info
		columns, err := rows.Columns()
		if err != nil {
			fmt.Printf("  ❌ ERROR getting columns: %v\n", err)
			continue
		}

		fmt.Printf("  ✅ SUCCESS - Columns: %v\n", columns)

		// Count and display rows
		rowCount := 0
		for rows.Next() {
			rowCount++
			if rowCount <= 3 { // Show first 3 rows
				// Create interface slice for scanning
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for j := range values {
					valuePtrs[j] = &values[j]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					fmt.Printf("    Error scanning row: %v\n", err)
					continue
				}

				fmt.Printf("    Row %d: ", rowCount)
				for j, val := range values {
					if j > 0 {
						fmt.Printf(", ")
					}
					fmt.Printf("%v", val)
				}
				fmt.Printf("\n")
			}
		}

		if rowCount > 3 {
			fmt.Printf("    ... (%d total rows)\n", rowCount)
		} else {
			fmt.Printf("  Total rows: %d\n", rowCount)
		}
	}

	fmt.Println("\n=== All tests completed! ===")
}
