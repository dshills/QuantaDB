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
	fmt.Println("=== Comprehensive GROUP BY Test ===")

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

	// Create a simple test table
	fmt.Println("\n1. Creating test table...")

	salesSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "sales",
		Columns: []catalog.ColumnDef{
			{Name: "product", DataType: types.Text, IsNullable: false},
			{Name: "region", DataType: types.Text, IsNullable: false},
			{Name: "amount", DataType: types.Integer, IsNullable: false},
			{Name: "quantity", DataType: types.Integer, IsNullable: false},
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
	
	// Start a transaction for inserts
	insertTxn, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		log.Fatal("Failed to begin insert transaction:", err)
	}
	
	// Set transaction ID for storage backend
	storageBackend.SetTransactionID(uint64(insertTxn.ID()))
	
	testData := []struct {
		product  string
		region   string
		amount   int32
		quantity int32
	}{
		{"Widget", "North", 100, 10},
		{"Widget", "South", 150, 15},
		{"Widget", "North", 200, 20},
		{"Gadget", "North", 300, 30},
		{"Gadget", "South", 250, 25},
		{"Gadget", "East", 400, 40},
		{"Gizmo", "North", 500, 50},
		{"Gizmo", "East", 600, 60},
	}

	for _, row := range testData {
		r := &executor.Row{
			Values: []types.Value{
				types.NewValue(row.product),
				types.NewValue(row.region),
				types.NewValue(row.amount),
				types.NewValue(row.quantity),
			},
		}
		_, err := storageBackend.InsertRow(salesTable.ID, r)
		if err != nil {
			log.Fatal("Failed to insert row:", err)
		}
	}
	
	// Commit insert transaction
	txnManager.CommitTransaction(insertTxn)
	fmt.Printf("   ✅ Inserted %d rows\n", len(testData))

	// Test various GROUP BY queries
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple GROUP BY with COUNT",
			query: "SELECT product, COUNT(*) FROM sales GROUP BY product",
		},
		{
			name:  "GROUP BY with SUM",
			query: "SELECT product, SUM(amount) FROM sales GROUP BY product",
		},
		{
			name:  "GROUP BY with multiple aggregates",
			query: "SELECT product, COUNT(*), SUM(amount), AVG(amount) FROM sales GROUP BY product",
		},
		{
			name:  "GROUP BY multiple columns",
			query: "SELECT product, region, SUM(amount) FROM sales GROUP BY product, region",
		},
		{
			name:  "GROUP BY with HAVING",
			query: "SELECT product, SUM(amount) FROM sales GROUP BY product HAVING SUM(amount) > 500",
		},
		{
			name:  "GROUP BY with complex expression",
			query: "SELECT product, SUM(amount) / SUM(quantity) as avg_price FROM sales GROUP BY product",
		},
		{
			name:  "GROUP BY without aggregates",
			query: "SELECT DISTINCT product FROM sales GROUP BY product",
		},
		{
			name:  "Empty GROUP BY result",
			query: "SELECT product, SUM(amount) FROM sales WHERE product = 'NonExistent' GROUP BY product",
		},
	}

	// Create executor with storage backend
	exec := executor.NewBasicExecutor(cat, eng)
	exec.SetStorageBackend(storageBackend)

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

		// Create planner and build plan
		pl := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := pl.Plan(stmt)
		if err != nil {
			fmt.Printf("   ❌ Planning error: %v\n", err)
			continue
		}

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
			SnapshotTS: int64(txnObj.ReadTimestamp()),
		}

		// Execute the plan
		result, err := exec.Execute(plan, ctx)
		if err != nil {
			fmt.Printf("   ❌ Execution error: %v\n", err)
			txnManager.AbortTransaction(txnObj)
			continue
		}

		schema := result.Schema()
		if schema == nil {
			fmt.Printf("   ❌ Failed to get schema: nil schema\n")
			result.Close()
			txnManager.AbortTransaction(txnObj)
			continue
		}

		fmt.Printf("   ✅ Query executed successfully!\n")
		fmt.Printf("   Schema columns: %d\n", len(schema.Columns))
		for j, col := range schema.Columns {
			fmt.Printf("     Column %d: %s (type: %s)\n", j, col.Name, col.Type)
		}

		// Read all results
		fmt.Println("   Results:")
		rowCount := 0
		for {
			row, err := result.Next()
			if err != nil {
				fmt.Printf("   ❌ Error reading result: %v\n", err)
				break
			}
			if row == nil {
				break
			}
			fmt.Printf("   ")
			for j, val := range row.Values {
				if j > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%v", val.Data)
			}
			fmt.Printf("\n")
			rowCount++
		}
		fmt.Printf("   Total rows: %d\n", rowCount)

		result.Close()
		txnManager.CommitTransaction(txnObj)
	}

	fmt.Println("\n=== Comprehensive GROUP BY Test Complete ===")
}