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
	fmt.Println("=== QuantaDB Simple TPC-H Benchmark ===")
	fmt.Println()

	// Simple queries that should work
	queries := []struct {
		name string
		sql  string
	}{
		{
			name: "Q1-Simple: Count all customers",
			sql:  "SELECT COUNT(*) FROM customer",
		},
		{
			name: "Q2-Simple: Customers by market segment",
			sql:  "SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment ORDER BY cnt DESC",
		},
		{
			name: "Q3-Simplified: Top revenue orders",
			sql: `SELECT l_orderkey, 
			             SUM(l_extendedprice * (1 - l_discount)) as revenue
			      FROM lineitem
			      WHERE l_shipdate > date '1995-03-15'
			      GROUP BY l_orderkey
			      ORDER BY revenue DESC`,
		},
		{
			name: "Q4-Simple: Order counts by priority",
			sql: `SELECT o_orderpriority, COUNT(*) as order_count
			      FROM orders
			      WHERE o_orderdate >= date '1993-07-01'
			        AND o_orderdate < date '1993-10-01'
			      GROUP BY o_orderpriority
			      ORDER BY o_orderpriority`,
		},
		{
			name: "Q5-Join: Customer-Order join count",
			sql: `SELECT COUNT(*)
			      FROM customer, orders
			      WHERE c_custkey = o_custkey`,
		},
	}

	fmt.Println("Running queries...")
	fmt.Println()

	totalTime := time.Duration(0)

	for _, q := range queries {
		fmt.Printf("%s\n", q.name)
		start := time.Now()

		// Execute query
		rows, err := db.Query(q.sql)
		if err != nil {
			fmt.Printf("  ❌ Error: %v\n", err)
			fmt.Println()
			continue
		}

		// Count rows
		rowCount := 0
		for rows.Next() {
			rowCount++
			// For debugging, print first row
			if rowCount == 1 {
				cols, _ := rows.Columns()
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err == nil {
					fmt.Printf("  First row: ")
					for i, v := range values {
						if i > 0 {
							fmt.Printf(", ")
						}
						fmt.Printf("%v", v)
					}
					fmt.Println()
				}
			}
		}
		rows.Close()

		elapsed := time.Since(start)
		totalTime += elapsed

		fmt.Printf("  ✓ Rows: %d, Time: %v\n", rowCount, elapsed)
		fmt.Println()
	}

	fmt.Printf("Total benchmark time: %v\n", totalTime)
}
