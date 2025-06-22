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

	fmt.Println("=== List Tables Test ===")

	// Try to list tables in various ways
	fmt.Println("1. Simple query on customer:")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&count)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Success: %d rows in customer\n", count)
	}

	fmt.Println("2. Query with explicit schema:")
	err = db.QueryRow("SELECT COUNT(*) FROM public.customer").Scan(&count)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Success: %d rows in public.customer\n", count)
	}
}
