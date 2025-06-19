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
	
	err = db.Ping()
	if err != nil {
		log.Fatal("Ping error:", err)
	}
	
	fmt.Println("Ping succeeded!")
}