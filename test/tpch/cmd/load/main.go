package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

func main() {
	var (
		host       = flag.String("host", "localhost", "Database host")
		port       = flag.Int("port", 5432, "Database port")
		schemaFile = flag.String("schema", "../../schema.sql", "Schema file")
		dataFile   = flag.String("data", "../../test_data.sql", "Data file")
	)
	flag.Parse()

	// Connect to QuantaDB
	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", *host, *port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to ping:", err)
	}

	fmt.Println("Connected to QuantaDB!")

	// Load schema
	fmt.Println("Loading schema...")
	if err := executeFile(db, *schemaFile); err != nil {
		log.Printf("Warning: Schema load failed: %v", err)
		// Continue anyway - tables might already exist
	}

	// Load test data
	fmt.Println("Loading test data...")
	if err := executeFile(db, *dataFile); err != nil {
		log.Printf("Warning: Data load failed: %v", err)
	}

	// Run some test queries
	fmt.Println("\nRunning test queries...")
	testQueries(db)
}

func executeFile(db *sql.DB, filename string) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Split by semicolon and execute each statement
	statements := strings.Split(string(content), ";")
	successCount := 0
	failCount := 0

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}

		_, err := db.Exec(stmt)
		if err != nil {
			// Log but continue
			fmt.Printf("  Failed: %s\n  Error: %v\n",
				strings.Split(stmt, "\n")[0], err)
			failCount++
		} else {
			successCount++
		}
	}

	fmt.Printf("  Executed %d statements (%d success, %d failed)\n",
		successCount+failCount, successCount, failCount)

	if failCount > 0 && successCount == 0 {
		return fmt.Errorf("all statements failed")
	}
	return nil
}

func testQueries(db *sql.DB) {
	// Test 1: Simple count
	fmt.Println("\n1. Testing simple count:")
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM region").Scan(&count)
	if err != nil {
		fmt.Printf("   FAILED: %v\n", err)
	} else {
		fmt.Printf("   Region count: %d\n", count)
	}

	// Test 2: Simple join
	fmt.Println("\n2. Testing simple join:")
	rows, err := db.Query(`
		SELECT r.r_name, COUNT(*) as nation_count
		FROM region r, nation n
		WHERE r.r_regionkey = n.n_regionkey
		GROUP BY r.r_name
		ORDER BY nation_count DESC
	`)
	if err != nil {
		fmt.Printf("   FAILED: %v\n", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var regionName string
			var nationCount int
			if err := rows.Scan(&regionName, &nationCount); err != nil {
				fmt.Printf("   Scan error: %v\n", err)
			} else {
				fmt.Printf("   %s: %d nations\n", regionName, nationCount)
			}
		}
	}

	// Test 3: Customer market segments
	fmt.Println("\n3. Testing market segments:")
	err = db.QueryRow("SELECT COUNT(*) FROM customer WHERE c_mktsegment = 'BUILDING'").Scan(&count)
	if err != nil {
		fmt.Printf("   FAILED: %v\n", err)
	} else {
		fmt.Printf("   BUILDING customers: %d\n", count)
	}
}
