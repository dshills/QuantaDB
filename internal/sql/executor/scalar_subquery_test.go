package executor

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/testutil"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestScalarSubqueryInWhere(t *testing.T) {
	// Create test environment
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Create transaction manager and storage backend
	txnManager := txn.NewManager(eng, nil)
	diskManager, err := storage.NewDiskManager(":memory:")
	testutil.AssertNoError(t, err)
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create executor
	exec := NewBasicExecutor(cat, eng)
	exec.SetStorageBackend(storageBackend)

	// Create planner
	plan := planner.NewBasicPlannerWithCatalog(cat)

	// Create test table schema
	ordersTableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "orders",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "customer_id", DataType: types.Integer, IsNullable: false},
			{Name: "amount", DataType: types.Integer, IsNullable: false},
		},
	}

	ordersTable, err := cat.CreateTable(ordersTableSchema)
	testutil.AssertNoError(t, err)

	err = storageBackend.CreateTable(ordersTable)
	testutil.AssertNoError(t, err)

	// Insert test data
	testData := [][]types.Value{
		{types.NewIntegerValue(1), types.NewIntegerValue(101), types.NewIntegerValue(150)},
		{types.NewIntegerValue(2), types.NewIntegerValue(102), types.NewIntegerValue(250)},
		{types.NewIntegerValue(3), types.NewIntegerValue(101), types.NewIntegerValue(300)},
	}

	for _, rowData := range testData {
		row := &Row{Values: rowData}
		_, err := storageBackend.InsertRow(ordersTable.ID, row)
		testutil.AssertNoError(t, err)
	}

	// Create transaction for query execution
	transaction, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
	testutil.AssertNoError(t, err)
	defer transaction.Rollback()

	execCtx := &ExecContext{
		Catalog:        cat,
		Engine:         eng,
		TxnManager:     txnManager,
		Txn:            transaction,
		SnapshotTS:     int64(transaction.ReadTimestamp()),
		IsolationLevel: txn.ReadCommitted,
		Stats:          &ExecStats{},
	}

	tests := []struct {
		name     string
		sql      string
		expected []struct {
			id     int32
			amount int32
		}
	}{
		{
			name: "Scalar subquery with AVG in WHERE clause",
			sql:  "SELECT id, amount FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
			expected: []struct {
				id     int32
				amount int32
			}{
				{2, 250}, // 250 > 233.33 (average)
				{3, 300}, // 300 > 233.33 (average)
			},
		},
		{
			name: "Scalar subquery with MAX in WHERE clause",
			sql:  "SELECT id, amount FROM orders WHERE amount = (SELECT MAX(amount) FROM orders)",
			expected: []struct {
				id     int32
				amount int32
			}{
				{3, 300}, // Only the maximum amount order
			},
		},
		{
			name: "Scalar subquery with MIN in WHERE clause",
			sql:  "SELECT id, amount FROM orders WHERE amount > (SELECT MIN(amount) FROM orders)",
			expected: []struct {
				id     int32
				amount int32
			}{
				{2, 250}, // 250 > 150 (minimum)
				{3, 300}, // 300 > 150 (minimum)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse SQL
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()
			testutil.AssertNoError(t, err)

			// Plan query
			queryPlan, err := plan.Plan(stmt)
			testutil.AssertNoError(t, err)

			// Execute query
			result, err := exec.Execute(queryPlan, execCtx)
			testutil.AssertNoError(t, err)
			defer result.Close()

			// Collect results
			var actualResults []struct {
				id     int32
				amount int32
			}

			for {
				row, err := result.Next()
				testutil.AssertNoError(t, err)
				if row == nil {
					break
				}

				actualResults = append(actualResults, struct {
					id     int32
					amount int32
				}{
					id:     row.Values[0].Data.(int32),
					amount: row.Values[1].Data.(int32),
				})
			}

			// Verify results
			testutil.AssertEqual(t, len(tt.expected), len(actualResults))
			for i, expected := range tt.expected {
				testutil.AssertEqual(t, expected.id, actualResults[i].id)
				testutil.AssertEqual(t, expected.amount, actualResults[i].amount)
			}
		})
	}
}

func TestScalarSubqueryError(t *testing.T) {
	// Create test environment
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Create transaction manager and storage backend
	txnManager := txn.NewManager(eng, nil)
	diskManager, err := storage.NewDiskManager(":memory:")
	testutil.AssertNoError(t, err)
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create executor
	exec := NewBasicExecutor(cat, eng)
	exec.SetStorageBackend(storageBackend)

	// Create planner
	plan := planner.NewBasicPlannerWithCatalog(cat)

	// Create test table schema
	ordersTableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "orders",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "amount", DataType: types.Integer, IsNullable: false},
		},
	}

	ordersTable, err := cat.CreateTable(ordersTableSchema)
	testutil.AssertNoError(t, err)

	err = storageBackend.CreateTable(ordersTable)
	testutil.AssertNoError(t, err)

	// Insert test data that would cause subquery to return multiple columns
	testData := [][]types.Value{
		{types.NewIntegerValue(1), types.NewIntegerValue(150)},
		{types.NewIntegerValue(2), types.NewIntegerValue(250)},
	}

	for _, rowData := range testData {
		row := &Row{Values: rowData}
		_, err := storageBackend.InsertRow(ordersTable.ID, row)
		testutil.AssertNoError(t, err)
	}

	// Create transaction for query execution
	transaction, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
	testutil.AssertNoError(t, err)
	defer transaction.Rollback()

	execCtx := &ExecContext{
		Catalog:        cat,
		Engine:         eng,
		TxnManager:     txnManager,
		Txn:            transaction,
		SnapshotTS:     int64(transaction.ReadTimestamp()),
		IsolationLevel: txn.ReadCommitted,
		Stats:          &ExecStats{},
	}

	// Test scalar subquery that returns multiple columns (should error)
	sqlWithMultipleColumns := "SELECT id FROM orders WHERE amount > (SELECT id, amount FROM orders LIMIT 1)"

	// Parse SQL
	p := parser.NewParser(sqlWithMultipleColumns)
	stmt, err := p.Parse()
	testutil.AssertNoError(t, err)

	// Plan query
	queryPlan, err := plan.Plan(stmt)
	testutil.AssertNoError(t, err)

	// Execute query - might fail at planning or execution
	result, err := exec.Execute(queryPlan, execCtx)
	if err == nil {
		// If planning succeeded, the execution should fail when we try to read results
		defer result.Close()

		// Try to read the first row - this should fail
		_, err = result.Next()
		if err == nil {
			t.Fatal("Expected error for scalar subquery returning multiple columns")
		}
	}

	// Either planning or execution should fail - we just verify some error occurred
	if err == nil {
		t.Fatal("Expected some error for scalar subquery returning multiple columns")
	}
}
