package main

import (
	"database/sql"
	"fmt"
	"log"
	
	_ "github.com/lib/pq"
)

func main() {
	// Connect to database
	connStr := "user=postgres password= dbname=quantadb host=localhost port=5432 sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Test simple correlated EXISTS query
	fmt.Println("Testing simple correlated EXISTS query...")
	query := `
		SELECT c_custkey, c_phone, c_acctbal
		FROM customer c
		WHERE NOT EXISTS (
			SELECT 1
			FROM orders o
			WHERE o.o_custkey = c.c_custkey
		)
		ORDER BY c_custkey
		LIMIT 10
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	fmt.Println("Customers without orders:")
	count := 0
	for rows.Next() {
		var custkey int
		var phone string
		var acctbal float64
		if err := rows.Scan(&custkey, &phone, &acctbal); err != nil {
			log.Fatal("Scan failed:", err)
		}
		fmt.Printf("  Customer %d: phone=%s, balance=%.2f\n", custkey, phone, acctbal)
		count++
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Row iteration failed:", err)
	}

	fmt.Printf("\nTotal customers without orders: %d\n", count)

	// Test correlated scalar subquery
	fmt.Println("\nTesting correlated scalar subquery...")
	query2 := `
		SELECT l_partkey, l_quantity
		FROM lineitem
		WHERE l_quantity < (
			SELECT 0.2 * AVG(l2.l_quantity)
			FROM lineitem l2
			WHERE l2.l_partkey = lineitem.l_partkey
		)
		LIMIT 5
	`

	rows2, err := db.Query(query2)
	if err != nil {
		fmt.Printf("Correlated scalar subquery failed (expected): %v\n", err)
	} else {
		defer rows2.Close()
		fmt.Println("Items with quantity below 20% of average for their part:")
		for rows2.Next() {
			var partkey int
			var quantity float64
			if err := rows2.Scan(&partkey, &quantity); err != nil {
				log.Fatal("Scan failed:", err)
			}
			fmt.Printf("  Part %d: quantity=%.2f\n", partkey, quantity)
		}
	}
}