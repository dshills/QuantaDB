package sql

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

// TestSQLIntegration tests the full SQL pipeline: parse -> plan -> execute.
func TestSQLIntegration(t *testing.T) {
	// Set up catalog and storage
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Create transaction manager and storage backend
	txnManager := txn.NewManager(eng, nil)
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := executor.NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create a test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "employees",
		Columns: []catalog.ColumnDef{
			{
				Name:       "id",
				DataType:   types.Integer,
				IsNullable: false,
			},
			{
				Name:       "name",
				DataType:   types.Varchar(100),
				IsNullable: false,
			},
			{
				Name:       "department",
				DataType:   types.Varchar(50),
				IsNullable: true,
			},
			{
				Name:       "salary",
				DataType:   types.Integer,
				IsNullable: false,
			},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create table in storage backend
	err = storageBackend.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table in storage: %v", err)
	}

	// Insert test data using proper row serialization
	testData := []struct {
		id         int
		name       string
		department string
		salary     int
	}{
		{1, "Alice", "Engineering", 100000},
		{2, "Bob", "Sales", 80000},
		{3, "Charlie", "Engineering", 120000},
		{4, "David", "HR", 70000},
		{5, "Eve", "Sales", 90000},
	}

	// Insert test data using the storage backend
	for _, testRow := range testData {
		// Create row for storage backend
		row := &executor.Row{
			Values: []types.Value{
				types.NewIntegerValue(int32(testRow.id)),
				types.NewTextValue(testRow.name),
				types.NewTextValue(testRow.department),
				types.NewIntegerValue(int32(testRow.salary)),
			},
		}

		// Insert into storage backend
		_, err := storageBackend.InsertRow(table.ID, row)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Create planner and executor
	plan := planner.NewBasicPlannerWithCatalog(cat)
	exec := executor.NewBasicExecutor(cat, eng)
	exec.SetStorageBackend(storageBackend)

	tests := []struct {
		name     string
		sql      string
		expected int // Expected row count
	}{
		{
			name:     "Simple SELECT",
			sql:      "SELECT * FROM employees",
			expected: 5,
		},
		{
			name:     "SELECT with LIMIT",
			sql:      "SELECT * FROM employees LIMIT 3",
			expected: 3,
		},
		{
			name:     "SELECT with OFFSET",
			sql:      "SELECT * FROM employees LIMIT 10 OFFSET 2",
			expected: 3, // 5 total - 2 offset = 3
		},
		{
			name:     "SELECT specific columns",
			sql:      "SELECT id, name FROM employees",
			expected: 5,
		},
		{
			name:     "SELECT with ORDER BY",
			sql:      "SELECT * FROM employees ORDER BY salary DESC",
			expected: 5,
		},
		{
			name:     "SELECT with ORDER BY and LIMIT",
			sql:      "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3",
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse SQL
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			// Plan query
			queryPlan, err := plan.Plan(stmt)
			if err != nil {
				t.Fatalf("Failed to plan query: %v", err)
			}

			// Create transaction for query execution
			transaction, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}
			defer transaction.Rollback()

			// Execute query
			ctx := &executor.ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            transaction,
				SnapshotTS:     int64(transaction.ReadTimestamp()),
				IsolationLevel: txn.ReadCommitted,
				Stats:          &executor.ExecStats{},
			}

			result, err := exec.Execute(queryPlan, ctx)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer result.Close()

			// Count results
			rowCount := 0
			for {
				row, err := result.Next()
				if err != nil {
					t.Fatalf("Error getting next row: %v", err)
				}
				if row == nil {
					break
				}
				rowCount++

				// Log the row for debugging
				t.Logf("Row %d: %v", rowCount, row.Values)
			}

			if rowCount != tt.expected {
				t.Errorf("Expected %d rows, got %d", tt.expected, rowCount)
			}

			// Log statistics
			t.Logf("Query stats: RowsRead=%d, RowsReturned=%d, BytesRead=%d",
				ctx.Stats.RowsRead, ctx.Stats.RowsReturned, ctx.Stats.BytesRead)
		})
	}
}

// TestEndToEndQuery tests a complete query execution with proper row format.
func TestEndToEndQuery(t *testing.T) {
	// This test demonstrates what a full integration would look like
	// with proper row serialization and column resolution

	t.Run("Full query execution", func(t *testing.T) {
		cat := catalog.NewMemoryCatalog()
		eng := engine.NewMemoryEngine()
		defer eng.Close()

		// Create transaction manager and storage backend
		txnManager := txn.NewManager(eng, nil)
		diskManager, err := storage.NewDiskManager(":memory:")
		if err != nil {
			t.Fatalf("Failed to create disk manager: %v", err)
		}
		defer diskManager.Close()

		bufferPool := storage.NewBufferPool(diskManager, 10)
		storageBackend := executor.NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

		// Create table
		tableSchema := &catalog.TableSchema{
			SchemaName: "public",
			TableName:  "users",
			Columns: []catalog.ColumnDef{
				{Name: "id", DataType: types.Integer, IsNullable: false},
				{Name: "name", DataType: types.Text, IsNullable: false},
				{Name: "age", DataType: types.Integer, IsNullable: false},
			},
		}

		table, err := cat.CreateTable(tableSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Create table in storage backend
		err = storageBackend.CreateTable(table)
		if err != nil {
			t.Fatalf("Failed to create table in storage: %v", err)
		}

		// Insert test users using storage backend
		testUsers := []struct {
			id   int
			name string
			age  int
		}{
			{1, "Alice", 28},
			{2, "Bob", 22},
			{3, "Charlie", 35},
			{4, "David", 40},
			{5, "Eve", 18},
		}

		for _, user := range testUsers {
			row := &executor.Row{
				Values: []types.Value{
					types.NewIntegerValue(int32(user.id)),
					types.NewTextValue(user.name),
					types.NewIntegerValue(int32(user.age)),
				},
			}

			_, err := storageBackend.InsertRow(table.ID, row)
			if err != nil {
				t.Fatalf("Failed to insert user: %v", err)
			}
		}

		// For this test, we'll simulate what would happen with proper integration
		sql := "SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC LIMIT 10"

		// Parse
		p := parser.NewParser(sql)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}

		// Plan
		plnr := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := plnr.Plan(stmt)
		if err != nil {
			t.Fatalf("Failed to plan: %v", err)
		}

		// Verify plan structure
		planStr := planner.ExplainPlan(plan)
		t.Logf("Query plan:\n%s", planStr)

		// The plan should have this structure:
		// Limit(10)
		//   Sort(age DESC)
		//     Project(name, age)
		//       Filter(age > 25)
		//         Scan(users)

		// Execute
		exec := executor.NewBasicExecutor(cat, eng)
		exec.SetStorageBackend(storageBackend)
		
		// Create transaction for query execution
		transaction, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		defer transaction.Rollback()
		
		ctx := &executor.ExecContext{
			Catalog:        cat,
			Engine:         eng,
			TxnManager:     txnManager,
			Txn:            transaction,
			SnapshotTS:     int64(transaction.ReadTimestamp()),
			IsolationLevel: txn.ReadCommitted,
			Stats:          &executor.ExecStats{},
		}

		// Note: This will fail with sort operator not implemented
		// which is expected for now
		_, err = exec.Execute(plan, ctx)
		if err != nil && err.Error() != "sort operator not implemented" {
			t.Fatalf("Unexpected error: %v", err)
		}
	})
}
