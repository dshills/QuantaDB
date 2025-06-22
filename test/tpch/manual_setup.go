package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Manual Setup ===")

	// Create customer table
	createCustomer := `CREATE TABLE customer (
        c_custkey INTEGER NOT NULL,
        c_name TEXT NOT NULL,
        c_address TEXT NOT NULL,
        c_nationkey INTEGER NOT NULL,
        c_phone TEXT NOT NULL,
        c_acctbal FLOAT NOT NULL,
        c_mktsegment TEXT NOT NULL,
        c_comment TEXT
    )`

	fmt.Println("Creating customer table...")
	_, err = db.Exec(createCustomer)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Println("  Success!")
	}

	// Insert some test data
	insertData := `INSERT INTO customer VALUES 
        (1, 'Customer One', '123 Main St', 1, '555-1234', 1000.50, 'AUTOMOBILE', 'Regular customer'),
        (2, 'Customer Two', '456 Oak Ave', 2, '555-5678', 2500.75, 'HOUSEHOLD', 'Premium customer'),
        (3, 'Customer Three', '789 Pine Rd', 1, '555-9012', 750.25, 'FURNITURE', 'New customer'),
        (4, 'Customer Four', '321 Elm St', 3, '555-3456', 1800.00, 'BUILDING', 'VIP customer'),
        (5, 'Customer Five', '654 Birch Dr', 2, '555-7890', 3200.00, 'AUTOMOBILE', 'Long-term customer')`

	fmt.Println("Inserting test data...")
	_, err = db.Exec(insertData)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Println("  Success!")
	}

	// Test simple count
	fmt.Println("Testing simple count...")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&count)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Success: %d rows\n", count)
	}
}
