//go:build ignore

package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	// Connect to QuantaDB
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to QuantaDB successfully!")
	fmt.Println("Running simple SQL tests...")
	fmt.Println()

	// Test 1: CREATE TABLE
	fmt.Println("1. Creating table...")
	_, err = db.Exec(`CREATE TABLE simple_test (id INTEGER PRIMARY KEY, name VARCHAR(50))`)
	if err != nil {
		fmt.Printf("   ERROR: %v\n", err)
	} else {
		fmt.Println("   SUCCESS")
	}

	// Test 2: INSERT without parameters
	fmt.Println("\n2. Inserting data...")
	_, err = db.Exec(`INSERT INTO simple_test (id, name) VALUES (1, 'Test')`)
	if err != nil {
		fmt.Printf("   ERROR: %v\n", err)
	} else {
		fmt.Println("   SUCCESS")
	}

	// Test 3: SELECT
	fmt.Println("\n3. Selecting data...")
	rows, err := db.Query(`SELECT id, name FROM simple_test`)
	if err != nil {
		fmt.Printf("   ERROR: %v\n", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				fmt.Printf("   SCAN ERROR: %v\n", err)
			} else {
				fmt.Printf("   Found: id=%d, name=%s\n", id, name)
			}
		}
		fmt.Println("   SUCCESS")
	}

	// Test 4: DROP TABLE
	fmt.Println("\n4. Dropping table...")
	_, err = db.Exec(`DROP TABLE simple_test`)
	if err != nil {
		fmt.Printf("   ERROR: %v\n", err)
	} else {
		fmt.Println("   SUCCESS")
	}

	fmt.Println("\nAll tests completed!")
}
