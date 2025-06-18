package executor

import (
	"path/filepath"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
)

func TestUpdateOperator(t *testing.T) {
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

	// Create table in storage
	err = storageBackend.CreateTable(table)
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

	// Test UPDATE without WHERE clause (updates all rows)
	t.Run("UpdateAll", func(t *testing.T) {
		assignments := []parser.Assignment{
			{Column: "age", Value: &parser.Literal{Value: types.NewValue(int32(40))}},
		}

		updateOp := NewUpdateOperator(table, storageBackend, assignments, nil)
		ctx := &ExecContext{Stats: &ExecStats{}}
		
		err = updateOp.Open(ctx)
		if err != nil {
			t.Fatalf("failed to open update operator: %v", err)
		}

		// Get result
		result, err := updateOp.Next()
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}

		// Should have updated 3 rows
		if result.Values[0].Data != int64(3) {
			t.Errorf("expected 3 rows updated, got %v", result.Values[0].Data)
		}

		updateOp.Close()

		// Verify that all rows now have age = 40
		iter, err := storageBackend.ScanTable(table.ID)
		if err != nil {
			t.Fatalf("failed to scan table: %v", err)
		}
		defer iter.Close()

		rowCount := 0
		for {
			row, _, err := iter.Next()
			if err != nil {
				break
			}
			if row == nil {
				break
			}
			rowCount++

			// Check age column (index 2)
			if row.Values[2].Data != int32(40) {
				t.Errorf("expected age to be 40, got %v", row.Values[2].Data)
			}
		}

		if rowCount != 3 {
			t.Errorf("expected 3 rows in table, got %d", rowCount)
		}
	})

	// Test UPDATE with WHERE clause
	t.Run("UpdateWithWhere", func(t *testing.T) {
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
		
		// Insert fresh test data
		for _, row := range testRows {
			_, err := storageBackend2.InsertRow(table2.ID, row)
			if err != nil {
				t.Fatalf("failed to insert row: %v", err)
			}
		}

		assignments := []parser.Assignment{
			{Column: "name", Value: &parser.Literal{Value: types.NewValue("Updated")}},
		}

		// WHERE id = 2
		whereClause := &parser.BinaryExpr{
			Left:     &parser.Identifier{Name: "id"},
			Operator: parser.TokenEqual,
			Right:    &parser.Literal{Value: types.NewValue(int32(2))},
		}

		updateOp := NewUpdateOperator(table2, storageBackend2, assignments, whereClause)
		ctx := &ExecContext{Stats: &ExecStats{}}
		
		err = updateOp.Open(ctx)
		if err != nil {
			t.Fatalf("failed to open update operator: %v", err)
		}

		// Get result
		result, err := updateOp.Next()
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}

		// Should have updated 1 row
		if result.Values[0].Data != int64(1) {
			t.Errorf("expected 1 row updated, got %v", result.Values[0].Data)
		}

		updateOp.Close()

		// Verify that only the row with id=2 was updated
		iter, err := storageBackend2.ScanTable(table2.ID)
		if err != nil {
			t.Fatalf("failed to scan table: %v", err)
		}
		defer iter.Close()

		rowCount := 0
		for {
			row, _, err := iter.Next()
			if err != nil {
				break
			}
			if row == nil {
				break
			}
			rowCount++

			id := row.Values[0].Data.(int32)
			name := row.Values[1].Data.(string)

			if id == 2 {
				// This row should be updated
				if name != "Updated" {
					t.Errorf("expected name to be 'Updated' for id=2, got %v", name)
				}
			} else {
				// Other rows should remain unchanged
				expectedName := ""
				switch id {
				case 1:
					expectedName = "Alice"
				case 3:
					expectedName = "Charlie"
				}
				if name != expectedName {
					t.Errorf("expected name to be '%s' for id=%d, got %v", expectedName, id, name)
				}
			}
		}

		if rowCount != 3 {
			t.Errorf("expected 3 rows in table, got %d", rowCount)
		}
	})
}