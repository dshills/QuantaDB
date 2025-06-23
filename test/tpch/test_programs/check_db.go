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

	// Show tables
	fmt.Println("Tables in database:")
	rows, err := db.Query("SELECT tablename FROM pg_tables WHERE schemaname='public'")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			fmt.Printf("Scan error: %v\n", err)
			continue
		}
		// Count rows
		var count int
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			fmt.Printf("  %s: error counting rows: %v\n", table, err)
		} else {
			fmt.Printf("  %s: %d rows\n", table, count)
		}
	}
}