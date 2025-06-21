package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
	
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
	
	fmt.Println("QuantaDB Basic Benchmark")
	fmt.Println("========================")
	fmt.Printf("Timestamp: %s\n\n", time.Now().Format(time.RFC3339))
	
	// Create test table
	fmt.Println("Creating test table...")
	_, err = db.Exec(`
		CREATE TABLE benchmark_test (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			value INTEGER,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		log.Printf("Create table error: %v", err)
	}
	
	// Test 1: INSERT performance
	fmt.Println("\nTest 1: INSERT Performance")
	fmt.Println("--------------------------")
	start := time.Now()
	
	for i := 1; i <= 1000; i++ {
		_, err := db.Exec(
			"INSERT INTO benchmark_test (id, name, value, created_at) VALUES ($1, $2, $3, $4)",
			i, fmt.Sprintf("Test %d", i), i*10, time.Now(),
		)
		if err != nil {
			log.Printf("Insert error: %v", err)
			break
		}
	}
	
	insertTime := time.Since(start)
	fmt.Printf("Inserted 1000 rows in %v\n", insertTime)
	fmt.Printf("Rate: %.2f rows/sec\n", 1000.0/insertTime.Seconds())
	
	// Test 2: SELECT performance
	fmt.Println("\nTest 2: SELECT Performance")
	fmt.Println("--------------------------")
	
	// Simple scan
	start = time.Now()
	rows, err := db.Query("SELECT COUNT(*) FROM benchmark_test")
	if err != nil {
		log.Printf("Select error: %v", err)
	} else {
		var count int
		if rows.Next() {
			rows.Scan(&count)
			fmt.Printf("Count: %d rows in %v\n", count, time.Since(start))
		}
		rows.Close()
	}
	
	// Filter scan
	start = time.Now()
	rows, err = db.Query("SELECT * FROM benchmark_test WHERE value > 500")
	if err != nil {
		log.Printf("Filter error: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		fmt.Printf("Filtered %d rows in %v\n", count, time.Since(start))
	}
	
	// Test 3: UPDATE performance
	fmt.Println("\nTest 3: UPDATE Performance")
	fmt.Println("--------------------------")
	start = time.Now()
	
	result, err := db.Exec("UPDATE benchmark_test SET value = value * 2 WHERE id <= 100")
	if err != nil {
		log.Printf("Update error: %v", err)
	} else {
		affected, _ := result.RowsAffected()
		fmt.Printf("Updated %d rows in %v\n", affected, time.Since(start))
	}
	
	// Test 4: DELETE performance
	fmt.Println("\nTest 4: DELETE Performance")
	fmt.Println("--------------------------")
	start = time.Now()
	
	result, err = db.Exec("DELETE FROM benchmark_test WHERE id > 900")
	if err != nil {
		log.Printf("Delete error: %v", err)
	} else {
		affected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d rows in %v\n", affected, time.Since(start))
	}
	
	// Cleanup
	fmt.Println("\nCleaning up...")
	_, err = db.Exec("DROP TABLE benchmark_test")
	if err != nil {
		log.Printf("Drop table error: %v", err)
	}
	
	fmt.Println("\nBenchmark complete!")
}