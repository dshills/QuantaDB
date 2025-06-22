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

	tables := []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"}

	fmt.Println("Checking TPC-H table row counts:")
	fmt.Println("================================")

	for _, table := range tables {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			fmt.Printf("%-12s: ERROR - %v\n", table, err)
		} else {
			fmt.Printf("%-12s: %d rows\n", table, count)
		}
	}
}
