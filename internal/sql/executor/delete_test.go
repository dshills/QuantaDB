package executor

import (
	"path/filepath"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
)

func TestDeleteOperator(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	
	// Create disk manager and buffer pool
	dm, err := storage.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()
	
	bufferPool := storage.NewBufferPool(dm, 10)
	cat := catalog.NewMemoryCatalog()
	storageBackend := NewDiskStorageBackend(bufferPool, cat)

	// Create a test table
	table := &catalog.Table{
		ID:         1,
		SchemaName: "public",
		TableName:  "users",
		Columns: []*catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer, IsNullable: false},
			{ID: 2, Name: "name", DataType: types.Text, IsNullable: false},
			{ID: 3, Name: "age", DataType: types.Integer, IsNullable: true},
		},
	}

	// Test DELETE without WHERE clause (deletes all rows)
	t.Run("DeleteAll", func(t *testing.T) {
		// Create table in storage
		err := storageBackend.CreateTable(table)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert test data
		testRows := []*Row{
			{Values: []types.Value{types.NewValue(int32(1)), types.NewValue("Alice"), types.NewValue(int32(25))}},
			{Values: []types.Value{types.NewValue(int32(2)), types.NewValue("Bob"), types.NewValue(int32(30))}},
			{Values: []types.Value{types.NewValue(int32(3)), types.NewValue("Charlie"), types.NewValue(int32(35))}},
		}

		for _, row := range testRows {
			_, err := storageBackend.InsertRow(table.ID, row)
			if err != nil {
				t.Fatalf("failed to insert row: %v", err)
			}
		}

		deleteOp := NewDeleteOperator(table, storageBackend, nil)
		ctx := &ExecContext{Stats: &ExecStats{}}
		
		err = deleteOp.Open(ctx)
		if err != nil {
			t.Fatalf("failed to open delete operator: %v", err)
		}

		// Get result
		result, err := deleteOp.Next()
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}

		// Should have deleted 3 rows
		if result.Values[0].Data != int64(3) {
			t.Errorf("expected 3 rows deleted, got %v", result.Values[0].Data)
		}

		deleteOp.Close()

		// Verify rows are marked as deleted by scanning
		iterator, err := storageBackend.ScanTable(table.ID)
		if err != nil {
			t.Fatalf("failed to scan table: %v", err)
		}
		defer iterator.Close()

		rowCount := 0
		for {
			row, _, err := iterator.Next()
			if err != nil {
				t.Fatalf("failed to iterate: %v", err)
			}
			if row == nil {
				break
			}
			rowCount++
		}

		// Should have no visible rows after delete
		if rowCount != 0 {
			t.Errorf("expected 0 visible rows after delete, got %d", rowCount)
		}
	})

	// Test DELETE with WHERE clause
	t.Run("DeleteWithWhere", func(t *testing.T) {
		// Create fresh storage backend for test isolation
		dm2, err := storage.NewDiskManager(filepath.Join(tmpDir, "test2.db"))
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm2.Close()
		
		bufferPool2 := storage.NewBufferPool(dm2, 10)
		storageBackend2 := NewDiskStorageBackend(bufferPool2, cat)
		
		// Create table in new backend
		table2 := &catalog.Table{
			ID:         2,
			SchemaName: "public",
			TableName:  "users2",
			Columns:    table.Columns,
		}
		
		err = storageBackend2.CreateTable(table2)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Insert test data
		testRows := []*Row{
			{Values: []types.Value{types.NewValue(int32(1)), types.NewValue("Alice"), types.NewValue(int32(25))}},
			{Values: []types.Value{types.NewValue(int32(2)), types.NewValue("Bob"), types.NewValue(int32(30))}},
			{Values: []types.Value{types.NewValue(int32(3)), types.NewValue("Charlie"), types.NewValue(int32(35))}},
		}

		for _, row := range testRows {
			_, err := storageBackend2.InsertRow(table2.ID, row)
			if err != nil {
				t.Fatalf("failed to insert row: %v", err)
			}
		}

		// WHERE age > 30
		whereClause := &parser.BinaryExpr{
			Left:     &parser.Identifier{Name: "age"},
			Operator: parser.TokenGreater,
			Right:    &parser.Literal{Value: types.NewValue(int32(30))},
		}

		deleteOp := NewDeleteOperator(table2, storageBackend2, whereClause)
		ctx := &ExecContext{Stats: &ExecStats{}}
		
		err = deleteOp.Open(ctx)
		if err != nil {
			t.Fatalf("failed to open delete operator: %v", err)
		}

		// Get result
		result, err := deleteOp.Next()
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}

		// Should have deleted 1 row (Charlie with age 35)
		if result.Values[0].Data != int64(1) {
			t.Errorf("expected 1 row deleted, got %v", result.Values[0].Data)
		}

		deleteOp.Close()

		// Verify correct rows remain by scanning
		iterator, err := storageBackend2.ScanTable(table2.ID)
		if err != nil {
			t.Fatalf("failed to scan table: %v", err)
		}
		defer iterator.Close()

		remainingRows := 0
		for {
			row, _, err := iterator.Next()
			if err != nil {
				t.Fatalf("failed to iterate: %v", err)
			}
			if row == nil {
				break
			}
			remainingRows++
			
			// Check that remaining rows have age <= 30
			age := row.Values[2].Data.(int32)
			if age > 30 {
				t.Errorf("found row with age > 30 that should have been deleted: %v", age)
			}
		}

		// Should have 2 visible rows after delete
		if remainingRows != 2 {
			t.Errorf("expected 2 visible rows after delete, got %d", remainingRows)
		}
	})
}