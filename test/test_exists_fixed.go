package main

import (
	"database/sql"
	"fmt"
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

	// Clean up and create test tables
	db.Exec(`DROP TABLE IF EXISTS test_users`)
	db.Exec(`DROP TABLE IF EXISTS test_orders`)

	fmt.Println("Creating test tables...")
	_, err = db.Exec(`CREATE TABLE test_users (id INTEGER, name TEXT)`)
	if err != nil {
		log.Fatal("Create test_users failed:", err)
	}
	
	_, err = db.Exec(`CREATE TABLE test_orders (id INTEGER, user_id INTEGER)`)
	if err != nil {
		log.Fatal("Create test_orders failed:", err)
	}

	// Insert test data
	fmt.Println("Inserting test data...")
	// Users: Alice (1), Bob (2), Charlie (3)
	db.Exec(`INSERT INTO test_users VALUES (1, 'Alice')`)
	db.Exec(`INSERT INTO test_users VALUES (2, 'Bob')`)
	db.Exec(`INSERT INTO test_users VALUES (3, 'Charlie')`)

	// Orders: Only Alice (1) and Bob (2) have orders
	db.Exec(`INSERT INTO test_orders VALUES (1, 1)`)  // Alice's order
	db.Exec(`INSERT INTO test_orders VALUES (2, 2)`)  // Bob's order

	// Test 1: EXISTS in SELECT
	fmt.Println("\nTest 1: EXISTS in SELECT clause")
	query1 := `
		SELECT u.id, u.name,
		       EXISTS (SELECT 1 FROM test_orders o WHERE o.user_id = u.id) as has_orders
		FROM test_users u
	`
	
	rows, err := db.Query(query1)
	if err != nil {
		fmt.Printf("EXISTS in SELECT failed: %v\n", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var id int
			var name string
			var hasOrders bool
			rows.Scan(&id, &name, &hasOrders)
			fmt.Printf("  %d: %s - has_orders=%v\n", id, name, hasOrders)
		}
	}

	// Test 2: EXISTS in WHERE
	fmt.Println("\nTest 2: EXISTS in WHERE clause (users with orders)")
	query2 := `
		SELECT u.id, u.name
		FROM test_users u
		WHERE EXISTS (
			SELECT 1 
			FROM test_orders o 
			WHERE o.user_id = u.id
		)
	`
	
	rows2, err := db.Query(query2)
	if err != nil {
		fmt.Printf("EXISTS in WHERE failed: %v\n", err)
	} else {
		defer rows2.Close()
		count := 0
		for rows2.Next() {
			var id int
			var name string
			rows2.Scan(&id, &name)
			fmt.Printf("  %d: %s\n", id, name)
			count++
		}
		fmt.Printf("Total users with orders: %d\n", count)
	}

	// Test 3: NOT EXISTS
	fmt.Println("\nTest 3: NOT EXISTS (users without orders)")
	query3 := `
		SELECT u.id, u.name
		FROM test_users u
		WHERE NOT EXISTS (
			SELECT 1
			FROM test_orders o
			WHERE o.user_id = u.id
		)
	`
	
	rows3, err := db.Query(query3)
	if err != nil {
		fmt.Printf("NOT EXISTS failed: %v\n", err)
	} else {
		defer rows3.Close()
		count := 0
		for rows3.Next() {
			var id int
			var name string
			rows3.Scan(&id, &name)
			fmt.Printf("  %d: %s\n", id, name)
			count++
		}
		fmt.Printf("Total users without orders: %d\n", count)
	}

	// Verify the results
	fmt.Println("\nExpected results:")
	fmt.Println("- Alice (1) and Bob (2) should have orders")
	fmt.Println("- Charlie (3) should NOT have orders")

	// Clean up
	db.Exec(`DROP TABLE test_orders`)
	db.Exec(`DROP TABLE test_users`)
}