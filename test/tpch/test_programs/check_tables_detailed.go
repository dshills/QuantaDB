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

	// First check if tables exist
	fmt.Println("Checking if tables exist:")
	fmt.Println("========================")

	tables := []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"}
	for _, table := range tables {
		_, err := db.Exec(fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", table))
		if err != nil {
			fmt.Printf("%-12s: DOES NOT EXIST - %v\n", table, err)
		} else {
			fmt.Printf("%-12s: EXISTS\n", table)
		}
	}

	fmt.Println("\nTrying simple count on orders table:")
	fmt.Println("====================================")

	// Try different approaches for orders table
	row := db.QueryRow("SELECT COUNT(*) FROM orders")
	var count sql.NullInt64
	err = row.Scan(&count)
	if err != nil {
		fmt.Printf("Count with NullInt64 failed: %v\n", err)
	} else {
		fmt.Printf("Count with NullInt64: valid=%v, value=%d\n", count.Valid, count.Int64)
	}

	// Try without scanning
	rows, err := db.Query("SELECT COUNT(*) FROM orders")
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
	} else {
		defer rows.Close()
		if rows.Next() {
			var c int
			err = rows.Scan(&c)
			if err != nil {
				fmt.Printf("Scan failed: %v\n", err)
			} else {
				fmt.Printf("Count: %d\n", c)
			}
		} else {
			fmt.Println("No rows returned")
		}
	}
}
