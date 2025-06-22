package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	var (
		host     = flag.String("host", "localhost", "Database host")
		port     = flag.Int("port", 5432, "Database port")
		sqlFile  = flag.String("file", "", "SQL file to load")
	)
	flag.Parse()

	if *sqlFile == "" {
		log.Fatal("Please specify a SQL file with -file")
	}

	// Connect to database
	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=quantadb sslmode=disable", *host, *port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Read SQL file
	content, err := ioutil.ReadFile(*sqlFile)
	if err != nil {
		log.Fatal("Failed to read file:", err)
	}

	// Split by semicolon and execute each statement
	statements := strings.Split(string(content), ";")
	totalStmts := 0
	successCount := 0
	
	start := time.Now()
	
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		
		totalStmts++
		_, err := db.Exec(stmt)
		if err != nil {
			fmt.Printf("Error: %v\nStatement: %.100s...\n", err, stmt)
		} else {
			successCount++
		}
	}
	
	elapsed := time.Since(start)
	fmt.Printf("\nLoaded %d/%d statements successfully in %v\n", successCount, totalStmts, elapsed)
}