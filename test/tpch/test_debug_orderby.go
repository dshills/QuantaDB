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

	fmt.Println("=== Debug ORDER BY Issue ===")

	// Test query without ORDER BY
	fmt.Println("\nTest 1: Without ORDER BY")
	rows, err := db.Query("SELECT c_mktsegment, COUNT(*) as cnt FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		rows.Close()
		fmt.Println("  SUCCESS: Query executed without errors")
	}

	// Test simple query with ORDER BY to see if it's ORDER BY causing the issue
	fmt.Println("\nTest 2: Simple query with ORDER BY")
	rows2, err := db.Query("SELECT c_custkey FROM customer ORDER BY c_custkey LIMIT 2")
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		defer rows2.Close()
		fmt.Println("  SUCCESS: Simple ORDER BY works")
		for rows2.Next() {
			var key int
			rows2.Scan(&key)
			fmt.Printf("  custkey: %d\n", key)
		}
	}

	fmt.Println("\n=== End Test ===")
}
