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
	fmt.Println("=== Testing Aggregate Expressions in Projection ===")

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

	// Create a test table
	fmt.Println("\n1. Creating test table...")

	salesSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "sales",
		Columns: []catalog.ColumnDef{
			{Name: "product", DataType: types.Text},
			{Name: "amount", DataType: types.Integer},
			{Name: "quantity", DataType: types.Integer},
		},
	}
	salesTable, err := cat.CreateTable(salesSchema)
	if err != nil {
		log.Fatal("Failed to create sales table:", err)
	}
	fmt.Println("   ✅ Created sales table")

	// Create table in storage backend
	err = storageBackend.CreateTable(salesTable)
	if err != nil {
		log.Fatal("Failed to create sales table in storage:", err)
	}

	// Insert test data
	fmt.Println("\n2. Inserting test data...")
	testData := []struct {
		product  string
		amount   int32
		quantity int32
	}{
		{"A", 100, 10},
		{"B", 200, 20},
		{"A", 150, 15},
		{"B", 300, 30},
	}

	for _, row := range testData {
		r := &executor.Row{
			Values: []types.Value{
				types.NewTextValue(row.product),
				types.NewIntegerValue(row.amount),
				types.NewIntegerValue(row.quantity),
			},
		}
		_, err := storageBackend.InsertRow(salesTable.ID, r)
		if err != nil {
			log.Fatal("Failed to insert row:", err)
		}
	}
	fmt.Println("   ✅ Inserted 4 rows")

	// Test queries with aggregate expressions
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple SUM",
			query: "SELECT SUM(amount) FROM sales",
		},
		{
			name:  "Multiple aggregates",
			query: "SELECT SUM(amount), SUM(quantity) FROM sales",
		},
		{
			name:  "Aggregate arithmetic",
			query: "SELECT SUM(amount) / SUM(quantity) FROM sales",
		},
		{
			name:  "Complex aggregate expression",
			query: "SELECT SUM(amount) * 100.0 / SUM(quantity) FROM sales",
		},
		{
			name:  "GROUP BY with aggregate arithmetic",
			query: "SELECT product, SUM(amount) / SUM(quantity) as avg_price FROM sales GROUP BY product",
		},
	}

	for i, test := range queries {
		fmt.Printf("\n%d. Testing: %s\n", i+3, test.name)
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

		// Check results
		schema := result.Schema()
		if schema == nil {
			fmt.Printf("   ❌ No schema returned\n")
			result.Close()
			continue
		}

		fmt.Printf("   ✅ Query executed successfully!\n")
		fmt.Printf("   Schema columns: %d\n", len(schema.Columns))
		for j, col := range schema.Columns {
			fmt.Printf("     Column %d: %s (type: %s)\n", j, col.Name, col.Type)
		}

		// Read first row
		row, err := result.Next()
		if err != nil {
			fmt.Printf("   ❌ Error reading result: %v\n", err)
		} else if row != nil {
			fmt.Printf("   Result: ")
			for j, val := range row.Values {
				if j > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%v", val.Data)
			}
			fmt.Printf("\n")
		}

		result.Close()
		txnManager.CommitTransaction(txnObj)
	}

	fmt.Println("\n=== Aggregate Expression Test Complete ===")
}
