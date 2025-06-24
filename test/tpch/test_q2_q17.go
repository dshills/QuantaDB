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

	// Test Q2
	fmt.Println("Testing Q2 (Minimum Cost Supplier)...")
	q2Bytes, err := ioutil.ReadFile("queries/q2.sql")
	if err != nil {
		log.Fatal("Failed to read Q2:", err)
	}
	
	rows, err := db.Query(string(q2Bytes))
	if err != nil {
		fmt.Printf("Q2 failed: %v\n\n", err)
	} else {
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		fmt.Printf("Q2 returned %d rows\n\n", count)
	}

	// Test Q17
	fmt.Println("Testing Q17 (Small-Quantity-Order Revenue)...")
	q17Bytes, err := ioutil.ReadFile("queries/q17.sql")
	if err != nil {
		log.Fatal("Failed to read Q17:", err)
	}
	
	rows2, err := db.Query(string(q17Bytes))
	if err != nil {
		fmt.Printf("Q17 failed: %v\n\n", err)
	} else {
		defer rows2.Close()
		count := 0
		for rows2.Next() {
			count++
		}
		fmt.Printf("Q17 returned %d rows\n\n", count)
	}

	// Test Q22
	fmt.Println("Testing Q22 (Global Sales Opportunity)...")
	q22Bytes, err := ioutil.ReadFile("queries/q22.sql")
	if err != nil {
		log.Fatal("Failed to read Q22:", err)
	}
	
	rows3, err := db.Query(string(q22Bytes))
	if err != nil {
		fmt.Printf("Q22 failed: %v\n\n", err)
	} else {
		defer rows3.Close()
		count := 0
		for rows3.Next() {
			count++
		}
		fmt.Printf("Q22 returned %d rows\n\n", count)
	}
}