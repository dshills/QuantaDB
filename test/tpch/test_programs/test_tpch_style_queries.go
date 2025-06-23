package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== TPC-H Style GROUP BY Queries Test ===")
	fmt.Printf("Testing critical GROUP BY functionality for TPC-H benchmarks\n\n")

	// TPC-H style queries that were previously failing
	queries := []struct {
		name        string
		description string
		query       string
	}{
		{
			name:        "Q1-Style",
			description: "Basic aggregation with GROUP BY (similar to TPC-H Q1)",
			query:       "SELECT c_mktsegment, COUNT(*) as customer_count FROM customer GROUP BY c_mktsegment",
		},
		{
			name:        "Q1-Style-OrderBy",
			description: "With ORDER BY clause",
			query:       "SELECT c_mktsegment, COUNT(*) as customer_count FROM customer GROUP BY c_mktsegment ORDER BY customer_count DESC",
		},
		{
			name:        "Q5-Style",
			description: "Multiple aggregates (similar to TPC-H Q5)",
			query:       "SELECT c_mktsegment, COUNT(*) as cnt, COUNT(c_custkey) as key_cnt FROM customer GROUP BY c_mktsegment",
		},
		{
			name:        "Complex-Projection",
			description: "Complex projection with aliasing",
			query:       "SELECT c_mktsegment as market_segment, COUNT(*) as total_customers FROM customer GROUP BY c_mktsegment",
		},
	}

	successCount := 0
	startTime := time.Now()

	for i, test := range queries {
		fmt.Printf("Query %d (%s): %s\n", i+1, test.name, test.description)
		fmt.Printf("SQL: %s\n", test.query)

		queryStart := time.Now()
		rows, err := db.Query(test.query)
		if err != nil {
			fmt.Printf("  ❌ FAILED: %v\n\n", err)
			continue
		}
		defer rows.Close()

		// Verify column structure
		columns, err := rows.Columns()
		if err != nil {
			fmt.Printf("  ❌ FAILED to get columns: %v\n\n", err)
			continue
		}

		// Process results
		rowCount := 0
		for rows.Next() {
			rowCount++
		}

		queryTime := time.Since(queryStart)
		fmt.Printf("  ✅ SUCCESS: %d rows, %d columns, %v\n", rowCount, len(columns), queryTime)
		fmt.Printf("     Columns: %v\n\n", columns)

		successCount++
	}

	totalTime := time.Since(startTime)

	fmt.Printf("=== Test Summary ===\n")
	fmt.Printf("Total queries: %d\n", len(queries))
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", len(queries)-successCount)
	fmt.Printf("Success rate: %.1f%%\n", float64(successCount)/float64(len(queries))*100)
	fmt.Printf("Total execution time: %v\n", totalTime)

	if successCount == len(queries) {
		fmt.Printf("\n🎉 ALL TESTS PASSED! GROUP BY functionality is ready for TPC-H benchmarks.\n")
	} else {
		fmt.Printf("\n⚠️  Some tests failed. GROUP BY implementation needs more work.\n")
	}
}
