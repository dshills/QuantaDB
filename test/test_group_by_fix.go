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
	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	fmt.Println("=== Testing GROUP BY Fix ===")

	// Test 1: Simple aggregate without GROUP BY (baseline)
	fmt.Println("\n1. Testing simple COUNT(*) - Baseline:")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&count)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Success: %d rows\n", count)
	}

	// Test 2: Empty table GROUP BY
	fmt.Println("\n2. Testing GROUP BY on empty table:")
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_empty (id INTEGER, category TEXT)")
	if err != nil {
		fmt.Printf("   ❌ Create table error: %v\n", err)
	} else {
		rows, err := db.Query("SELECT category, COUNT(*) FROM test_empty GROUP BY category")
		if err != nil {
			fmt.Printf("   ❌ Query error: %v\n", err)
		} else {
			rowCount := 0
			for rows.Next() {
				rowCount++
			}
			rows.Close()
			fmt.Printf("   ✅ Success: %d groups (expected 0)\n", rowCount)
		}
	}

	// Test 3: Simple GROUP BY with data
	fmt.Println("\n3. Testing GROUP BY with test data:")
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_group (id INTEGER, category TEXT)")
	if err != nil {
		fmt.Printf("   ❌ Create table error: %v\n", err)
		return
	}

	// Clear and insert test data
	_, err = db.Exec("DELETE FROM test_group")
	if err != nil {
		fmt.Printf("   ❌ Clear table error: %v\n", err)
	}

	testData := []struct {
		id       int
		category string
	}{
		{1, "A"},
		{2, "B"},
		{3, "A"},
		{4, "C"},
		{5, "B"},
	}

	for _, td := range testData {
		_, err = db.Exec("INSERT INTO test_group (id, category) VALUES ($1, $2)", td.id, td.category)
		if err != nil {
			fmt.Printf("   ❌ Insert error: %v\n", err)
			return
		}
	}

	// Now test GROUP BY
	rows, err := db.Query("SELECT category, COUNT(*) FROM test_group GROUP BY category ORDER BY category")
	if err != nil {
		fmt.Printf("   ❌ GROUP BY query error: %v\n", err)
		return
	}
	defer rows.Close()

	fmt.Println("   Results:")
	for rows.Next() {
		var category string
		var cnt int
		err = rows.Scan(&category, &cnt)
		if err != nil {
			fmt.Printf("   ❌ Scan error: %v\n", err)
			break
		}
		fmt.Printf("     %s: %d\n", category, cnt)
	}

	if err = rows.Err(); err != nil {
		fmt.Printf("   ❌ Row iteration error: %v\n", err)
	} else {
		fmt.Printf("   ✅ GROUP BY executed successfully!\n")
	}

	// Test 4: The original failing query (if customer table exists)
	fmt.Println("\n4. Testing original failing query:")
	rows, err = db.Query("SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment")
	if err != nil {
		fmt.Printf("   ❌ Query error: %v\n", err)
		return
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		var segment string
		var cnt int
		err = rows.Scan(&segment, &cnt)
		if err != nil {
			fmt.Printf("   ❌ Scan error: %v\n", err)
			break
		}
		fmt.Printf("     %s: %d\n", segment, cnt)
		rowCount++
	}

	if err = rows.Err(); err != nil {
		fmt.Printf("   ❌ Row iteration error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Success: %d market segments\n", rowCount)
	}
}