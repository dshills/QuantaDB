package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestDropTableOperator(t *testing.T) {
	// Create test catalog and storage
	cat := catalog.NewMemoryCatalog()
	storage := newMockStorageBackend()

	// Create a test table first
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{
				Name:     "id",
				DataType: types.Integer,
			},
			{
				Name:     "name",
				DataType: types.Varchar(50),
			},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	err = storage.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Verify table exists
	_, err = cat.GetTable("public", "test_table")
	if err != nil {
		t.Fatalf("Table should exist before drop: %v", err)
	}

	// Create DROP TABLE operator
	dropOp := NewDropTableOperator("public", "test_table", cat, storage)

	// Create execution context
	ctx := &ExecContext{
		Catalog: cat,
		Stats:   &ExecStats{},
	}

	// Test Open
	err = dropOp.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Test Next - should return success message
	row, err := dropOp.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if row == nil {
		t.Fatal("Expected result row, got nil")
	}

	if len(row.Values) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(row.Values))
	}

	result, ok := row.Values[0].Data.(string)
	if !ok {
		t.Fatalf("Expected string result, got %T", row.Values[0].Data)
	}

	expected := "Table 'public.test_table' dropped"
	if result != expected {
		t.Fatalf("Expected result '%s', got '%s'", expected, result)
	}

	// Test Next again - should return nil (EOF)
	row, err = dropOp.Next()
	if err != nil {
		t.Fatalf("Second Next failed: %v", err)
	}

	if row != nil {
		t.Fatal("Expected nil on second Next call")
	}

	// Test Close
	err = dropOp.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify table no longer exists in catalog
	_, err = cat.GetTable("public", "test_table")
	if err == nil {
		t.Fatal("Table should not exist after drop")
	}

	// Verify table storage was removed
	if _, exists := storage.tables[table.ID]; exists {
		t.Fatal("Table storage should not exist after drop")
	}

	// Verify stats were updated
	if ctx.Stats.RowsReturned != 1 {
		t.Fatalf("Expected RowsReturned=1, got %d", ctx.Stats.RowsReturned)
	}
}

func TestDropTableOperator_NonExistentTable(t *testing.T) {
	// Create test catalog and storage
	cat := catalog.NewMemoryCatalog()
	storage := newMockStorageBackend()

	// Create DROP TABLE operator for non-existent table
	dropOp := NewDropTableOperator("public", "non_existent", cat, storage)

	// Create execution context
	ctx := &ExecContext{
		Catalog: cat,
		Stats:   &ExecStats{},
	}

	// Test Open - should fail
	err := dropOp.Open(ctx)
	if err == nil {
		t.Fatal("Expected error when dropping non-existent table")
	}

	expectedErr := "table 'public.non_existent' does not exist"
	if err.Error() != expectedErr {
		t.Fatalf("Expected error '%s', got '%s'", expectedErr, err.Error())
	}
}

func TestDropTableOperator_Schema(t *testing.T) {
	cat := catalog.NewMemoryCatalog()
	storage := newMockStorageBackend()

	dropOp := NewDropTableOperator("public", "test_table", cat, storage)

	schema := dropOp.Schema()
	if schema == nil {
		t.Fatal("Schema should not be nil")
	}

	if len(schema.Columns) != 1 {
		t.Fatalf("Expected 1 column in schema, got %d", len(schema.Columns))
	}

	if schema.Columns[0].Name != "result" {
		t.Fatalf("Expected column name 'result', got '%s'", schema.Columns[0].Name)
	}

	if schema.Columns[0].Type != types.Text {
		t.Fatalf("Expected TEXT type, got %s", schema.Columns[0].Type.Name())
	}

	if schema.Columns[0].Nullable {
		t.Fatal("Result column should not be nullable")
	}
}
