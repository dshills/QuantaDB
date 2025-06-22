package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
)

func main() {
	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=quantadb sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Setting up customer table ===")

	// Create customer table
	_, err = db.Exec(`CREATE TABLE customer (
		c_custkey INTEGER NOT NULL,
		c_name TEXT NOT NULL,
		c_address TEXT NOT NULL,
		c_nationkey INTEGER NOT NULL,
		c_phone TEXT NOT NULL,
		c_acctbal DECIMAL(15,2) NOT NULL,
		c_mktsegment TEXT NOT NULL,
		c_comment TEXT NOT NULL
	)`)
	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		return
	}
	fmt.Println("Customer table created.")

	// Insert sample data
	customers := []struct {
		key, nationkey                            int
		name, address, phone, mktsegment, comment string
		acctbal                                   float64
	}{
		{1, 15, "Customer#000000001", "IVhzIApeRb ot,c,E", "25-989-741-2988", "BUILDING", "to the even, regular platelets. regular, ironic epitaphs nag e", 711.56},
		{2, 13, "Customer#000000002", "XSTf4,NCwDVaWNe6tEgvwfmRchLXak", "23-768-687-3665", "AUTOMOBILE", "l accounts. blithely ironic theodolites integrate boldly: caref", 121.65},
		{3, 1, "Customer#000000003", "MG9kdTD2WBHm", "11-719-748-3364", "AUTOMOBILE", " deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov", 7498.12},
		{4, 4, "Customer#000000004", "XxVSJsLAGtn", "14-128-190-5944", "MACHINERY", " requests. final, regular ideas sleep final accou", 2866.83},
		{5, 3, "Customer#000000005", "KvpyuHCplrB84WgAiGV6sYpZq7Tj", "13-750-942-6364", "HOUSEHOLD", "n accounts will have to unwind. foxes cajole accor", 794.47},
	}

	for _, c := range customers {
		_, err = db.Exec(`INSERT INTO customer VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			c.key, c.name, c.address, c.nationkey, c.phone, c.acctbal, c.mktsegment, c.comment)
		if err != nil {
			fmt.Printf("Error inserting customer %d: %v\n", c.key, err)
		}
	}

	fmt.Println("Sample customer data inserted.")

	// Test a simple query
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM customer").Scan(&count)
	if err != nil {
		fmt.Printf("Error counting customers: %v\n", err)
		return
	}

	fmt.Printf("Customer table ready with %d rows.\n", count)
}
