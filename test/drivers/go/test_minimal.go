package main

import (
	"database/sql"
	"fmt"
	"log"
	_ "github.com/lib/pq"
)

func main() {
	connStr := "host=localhost port=5433 user=postgres dbname=quantadb sslmode=disable"
	fmt.Printf("Connecting to: %s\n", connStr)
	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Open error:", err)
	}
	defer db.Close()
	
	fmt.Println("sql.Open succeeded")
	
	// Execute a simple query
	rows, err := db.Query("SELECT 1")
	if err != nil {
		log.Fatal("Query error:", err)
	}
	defer rows.Close()
	
	fmt.Println("Query succeeded!")
	
	if rows.Next() {
		var val int
		if err := rows.Scan(&val); err != nil {
			log.Fatal("Scan error:", err)
		}
		fmt.Printf("Result: %d\n", val)
	}
}