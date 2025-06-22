//go:build ignore

package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	// Connect to database
	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to database successfully!")

	// Test queries
	queries := []struct {
		name string
		sql  string
	}{
		{"Count customers", "SELECT COUNT(*) FROM customer"},
		{"Count orders", "SELECT COUNT(*) FROM orders"},
		{"Simple aggregation", "SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment"},
		{"Join test", "SELECT COUNT(*) FROM customer c, orders o WHERE c.c_custkey = o.o_custkey"},
	}

	for _, q := range queries {
		fmt.Printf("\n%s:\n", q.name)
		start := time.Now()

		rows, err := db.Query(q.sql)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		// Get column names
		cols, err := rows.Columns()
		if err != nil {
			fmt.Printf("  Error getting columns: %v\n", err)
			rows.Close()
			continue
		}

		// Create slice for values
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Print results
		rowCount := 0
		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				fmt.Printf("  Error scanning: %v\n", err)
				break
			}

			if rowCount < 10 { // Only print first 10 rows
				fmt.Printf("  ")
				for i, v := range values {
					if i > 0 {
						fmt.Printf(", ")
					}
					fmt.Printf("%v", v)
				}
				fmt.Println()
			}
			rowCount++
		}

		if err := rows.Err(); err != nil {
			fmt.Printf("  Error iterating: %v\n", err)
		}

		rows.Close()
		elapsed := time.Since(start)
		fmt.Printf("  Rows: %d, Time: %v\n", rowCount, elapsed)
	}
}
