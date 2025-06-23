package main

import (
	"database/sql"
	"fmt"
	"log"
	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tables := []string{"region", "nation", "supplier", "customer", "part", "orders", "lineitem", "partsupp"}
	
	fmt.Println("TPC-H Table Status:")
	for _, table := range tables {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			fmt.Printf("  %s: NOT FOUND or ERROR: %v\n", table, err)
		} else {
			fmt.Printf("  %s: %d rows\n", table, count)
		}
	}
}