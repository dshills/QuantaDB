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
	fmt.Println("=== Testing Aggregate NULL Semantics ===")

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

	testSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "value", DataType: types.Integer, IsNullable: true},
		},
	}
	testTable, err := cat.CreateTable(testSchema)
	if err != nil {
		log.Fatal("Failed to create test table:", err)
	}
	fmt.Println("   ✅ Created test table")

	// Create table in storage backend
	err = storageBackend.CreateTable(testTable)
	if err != nil {
		log.Fatal("Failed to create test table in storage:", err)
	}

	// Insert test data with transaction
	fmt.Println("\n2. Inserting test data...")
	
	insertTxn, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		log.Fatal("Failed to begin insert transaction:", err)
	}
	
	storageBackend.SetTransactionID(uint64(insertTxn.ID()))
	
	// Insert some rows (we'll test with WHERE FALSE to get empty results)
	testData := []struct {
		id    int32
		value interface{}
	}{
		{1, int32(10)},
		{2, int32(20)},
		{3, nil}, // NULL value
		{4, int32(30)},
	}

	for _, row := range testData {
		var valueVal types.Value
		if row.value == nil {
			valueVal = types.NewNullValue()
		} else {
			valueVal = types.NewValue(row.value)
		}
		
		r := &executor.Row{
			Values: []types.Value{
				types.NewValue(row.id),
				valueVal,
			},
		}
		_, err := storageBackend.InsertRow(testTable.ID, r)
		if err != nil {
			log.Fatal("Failed to insert row:", err)
		}
	}
	
	txnManager.CommitTransaction(insertTxn)
	fmt.Printf("   ✅ Inserted %d rows\n", len(testData))

	// Test queries for NULL semantics
	queries := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "COUNT(*) with no rows (WHERE FALSE)",
			query:    "SELECT COUNT(*) FROM test WHERE 1=0",
			expected: "Should return one row with value 0",
		},
		{
			name:     "SUM with no rows (WHERE FALSE)",
			query:    "SELECT SUM(value) FROM test WHERE 1=0",
			expected: "Should return one row with NULL",
		},
		{
			name:     "AVG with no rows (WHERE FALSE)",
			query:    "SELECT AVG(value) FROM test WHERE 1=0",
			expected: "Should return one row with NULL",
		},
		{
			name:     "Multiple aggregates with no rows",
			query:    "SELECT COUNT(*), SUM(value), AVG(value) FROM test WHERE id < 0",
			expected: "Should return one row: 0, NULL, NULL",
		},
		{
			name:     "SUM with all NULL values",
			query:    "SELECT SUM(value) FROM test WHERE id = 3",
			expected: "Should return one row with NULL",
		},
		{
			name:     "COUNT with all NULL values",
			query:    "SELECT COUNT(value) FROM test WHERE id = 3",
			expected: "Should return one row with 0 (COUNT ignores NULLs)",
		},
	}

	// Create executor
	exec := executor.NewBasicExecutor(cat, eng)
	exec.SetStorageBackend(storageBackend)

	for i, test := range queries {
		fmt.Printf("\n%d. Testing: %s\n", i+3, test.name)
		fmt.Printf("   Query: %s\n", test.query)
		fmt.Printf("   Expected: %s\n", test.expected)

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

		// Read the result
		row, err := result.Next()
		if err != nil {
			fmt.Printf("   ❌ Error reading result: %v\n", err)
			result.Close()
			txnManager.AbortTransaction(txnObj)
			continue
		}

		if row == nil {
			fmt.Printf("   ❌ No rows returned (expected 1 row)\n")
		} else {
			fmt.Printf("   ✅ Got 1 row: ")
			for j, val := range row.Values {
				if j > 0 {
					fmt.Printf(", ")
				}
				if val.IsNull() {
					fmt.Printf("NULL")
				} else {
					fmt.Printf("%v", val.Data)
				}
			}
			fmt.Printf("\n")
		}

		// Verify no more rows
		row2, _ := result.Next()
		if row2 != nil {
			fmt.Printf("   ⚠️  Warning: Got more than 1 row\n")
		}

		result.Close()
		txnManager.CommitTransaction(txnObj)
	}

	fmt.Println("\n=== Aggregate NULL Semantics Test Complete ===")
}