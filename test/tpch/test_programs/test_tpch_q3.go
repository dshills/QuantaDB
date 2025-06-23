package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// TPC-H Q3: Shipping Priority Query
const q3 = `
SELECT
    l.l_orderkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM
    customer c,
    orders o,
    lineitem l
WHERE
    c.c_mktsegment = 'BUILDING'
    AND c.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND o.o_orderdate < DATE '1995-03-15'
    AND l.l_shipdate > DATE '1995-03-15'
GROUP BY
    l.l_orderkey,
    o.o_orderdate,
    o.o_shippriority
ORDER BY
    revenue DESC,
    o.o_orderdate
LIMIT 10;
`

func main() {
	// Connect to database
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping:", err)
	}

	fmt.Println("Connected to QuantaDB")
	fmt.Println("Running TPC-H Q3...")
	fmt.Println()

	// Execute query
	start := time.Now()
	rows, err := db.Query(q3)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	// Print results
	fmt.Printf("%-10s %-15s %-12s %-8s\n", "OrderKey", "Revenue", "OrderDate", "Priority")
	fmt.Println(strings.Repeat("-", 50))

	count := 0
	for rows.Next() {
		var orderKey int
		var revenue float64
		var orderDate time.Time
		var shipPriority int

		if err := rows.Scan(&orderKey, &revenue, &orderDate, &shipPriority); err != nil {
			log.Fatal("Scan failed:", err)
		}

		fmt.Printf("%-10d %-15.2f %-12s %-8d\n",
			orderKey, revenue, orderDate.Format("2006-01-02"), shipPriority)
		count++
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Row iteration error:", err)
	}

	elapsed := time.Since(start)
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("Returned %d rows in %v\n", count, elapsed)
}

