package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
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

	// Read Q22
	queryBytes, err := ioutil.ReadFile("queries/q22.sql")
	if err != nil {
		log.Fatal("Failed to read query:", err)
	}
	query := string(queryBytes)

	fmt.Println("Running Q22...")
	fmt.Println("Query:", query)
	
	// Execute query
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	// Count rows
	count := 0
	for rows.Next() {
		count++
	}

	if err = rows.Err(); err != nil {
		log.Fatal("Row iteration failed:", err)
	}

	fmt.Printf("Q22 returned %d rows\n", count)
}