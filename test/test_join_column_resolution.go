//go:build ignore

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

func main() {
	fmt.Println("=== Testing JOIN Column Resolution Fix ===")

	// Create a test catalog
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Create storage backend
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		log.Fatal("Failed to create disk manager:", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	txnManager := txn.NewManager(eng, nil)
	storageBackend := executor.NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test tables
	fmt.Println("\n1. Creating test tables...")

	customersSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "customers",
		Columns: []catalog.ColumnDef{
			{Name: "c_custkey", DataType: types.Integer},
			{Name: "c_name", DataType: types.Text},
		},
	}
	customersTable, err := cat.CreateTable(customersSchema)
	if err != nil {
		log.Fatal("Failed to create customers table:", err)
	}
	fmt.Println("   ✅ Created customers table")

	// Create table in storage backend
	err = storageBackend.CreateTable(customersTable)
	if err != nil {
		log.Fatal("Failed to create customers table in storage:", err)
	}

	ordersSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "orders",
		Columns: []catalog.ColumnDef{
			{Name: "o_orderkey", DataType: types.Integer},
			{Name: "o_custkey", DataType: types.Integer},
		},
	}
	ordersTable, err := cat.CreateTable(ordersSchema)
	if err != nil {
		log.Fatal("Failed to create orders table:", err)
	}
	fmt.Println("   ✅ Created orders table")

	// Create table in storage backend
	err = storageBackend.CreateTable(ordersTable)
	if err != nil {
		log.Fatal("Failed to create orders table in storage:", err)
	}

	// Test queries
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple JOIN with qualified columns",
			query: "SELECT c.c_name FROM customers c JOIN orders o ON c.c_custkey = o.o_custkey",
		},
		{
			name:  "JOIN with WHERE clause using qualified names",
			query: "SELECT * FROM customers c, orders o WHERE c.c_custkey = o.o_custkey",
		},
		{
			name:  "Multiple column SELECT with JOIN",
			query: "SELECT c.c_name, o.o_orderkey FROM customers c JOIN orders o ON c.c_custkey = o.o_custkey",
		},
		{
			name:  "Complex WHERE with qualified columns",
			query: "SELECT c.c_custkey, c.c_name, o.o_orderkey FROM customers c, orders o WHERE c.c_custkey = o.o_custkey AND c.c_custkey > 0",
		},
	}

	for i, test := range queries {
		fmt.Printf("\n%d. Testing: %s\n", i+2, test.name)
		fmt.Printf("   Query: %s\n", test.query)

		// Parse the query
		p := parser.NewParser(test.query)
		stmt, err := p.Parse()
		if err != nil {
			fmt.Printf("   ❌ Parse error: %v\n", err)
			continue
		}

		// Plan the query
		plnr := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := plnr.Plan(stmt)
		if err != nil {
			fmt.Printf("   ❌ Planning error: %v\n", err)
			continue
		}

		// Create executor with storage backend
		exec := executor.NewBasicExecutor(cat, eng)
		exec.SetStorageBackend(storageBackend)

		// Start a transaction
		txnObj, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			fmt.Printf("   ❌ Failed to begin transaction: %v\n", err)
			continue
		}

		// Create execution context
		ctx := &executor.ExecContext{
			Catalog:    cat,
			Engine:     eng,
			Stats:      &executor.ExecStats{},
			TxnManager: txnManager,
			Txn:        txnObj,
		}

		// Execute the query
		result, err := exec.Execute(plan, ctx)
		if err != nil {
			fmt.Printf("   ❌ Execution error: %v\n", err)
			continue
		}

		// Check schema
		schema := result.Schema()
		if schema == nil {
			fmt.Printf("   ❌ No schema returned\n")
			continue
		}

		fmt.Printf("   ✅ Query executed successfully!\n")
		fmt.Printf("   Schema columns:\n")
		for _, col := range schema.Columns {
			fmt.Printf("      - %s (table: %s, alias: %s)\n", col.Name, col.TableName, col.TableAlias)
		}

		result.Close()
	}

	fmt.Println("\n=== Column Resolution Test Complete ===")
}
