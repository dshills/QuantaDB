package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestAlterTableAddColumnOperator(t *testing.T) {
	// Create test catalog and storage
	cat := catalog.NewMemoryCatalog()
	storage := newMockStorageBackend()

	// Create a test table first
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
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

	// Create column definition to add
	columnDef := &parser.ColumnDef{
		Name:     "email",
		DataType: types.Varchar(100),
		Constraints: []parser.ColumnConstraint{
			parser.NotNullConstraint{},
		},
	}

	// Create ALTER TABLE ADD COLUMN operator
	addOp := NewAlterTableAddColumnOperator("public", "users", columnDef, cat, storage)

	// Create execution context
	ctx := &ExecContext{
		Catalog: cat,
		Stats:   &ExecStats{},
	}

	// Test Open
	err = addOp.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Test Next - should return success message
	row, err := addOp.Next()
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

	expected := "Column 'email' added to table 'public.users'"
	if result != expected {
		t.Fatalf("Expected result '%s', got '%s'", expected, result)
	}

	// Test Next again - should return nil (EOF)
	row, err = addOp.Next()
	if err != nil {
		t.Fatalf("Second Next failed: %v", err)
	}

	if row != nil {
		t.Fatal("Expected nil on second Next call")
	}

	// Test Close
	err = addOp.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify column was added to catalog
	updatedTable, err := cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("Failed to get updated table: %v", err)
	}

	if len(updatedTable.Columns) != 3 {
		t.Fatalf("Expected 3 columns after adding, got %d", len(updatedTable.Columns))
	}

	// Check the new column
	emailColumn := updatedTable.Columns[2]
	if emailColumn.Name != "email" {
		t.Errorf("Expected column name 'email', got '%s'", emailColumn.Name)
	}

	if emailColumn.DataType.Name() != "VARCHAR(100)" {
		t.Errorf("Expected VARCHAR(100), got %s", emailColumn.DataType.Name())
	}

	if emailColumn.IsNullable {
		t.Error("Expected NOT NULL column")
	}

	// Verify stats were updated
	if ctx.Stats.RowsReturned != 1 {
		t.Fatalf("Expected RowsReturned=1, got %d", ctx.Stats.RowsReturned)
	}
}

func TestAlterTableDropColumnOperator(t *testing.T) {
	// Create test catalog and storage
	cat := catalog.NewMemoryCatalog()
	storage := newMockStorageBackend()

	// Create a test table with multiple columns
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{
				Name:     "id",
				DataType: types.Integer,
			},
			{
				Name:     "name",
				DataType: types.Varchar(50),
			},
			{
				Name:     "email",
				DataType: types.Varchar(100),
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

	// Create ALTER TABLE DROP COLUMN operator
	dropOp := NewAlterTableDropColumnOperator("public", "users", "email", cat, storage)

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

	result, ok := row.Values[0].Data.(string)
	if !ok {
		t.Fatalf("Expected string result, got %T", row.Values[0].Data)
	}

	expected := "Column 'email' dropped from table 'public.users'"
	if result != expected {
		t.Fatalf("Expected result '%s', got '%s'", expected, result)
	}

	// Test Close
	err = dropOp.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify column was removed from catalog
	updatedTable, err := cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("Failed to get updated table: %v", err)
	}

	if len(updatedTable.Columns) != 2 {
		t.Fatalf("Expected 2 columns after dropping, got %d", len(updatedTable.Columns))
	}

	// Verify email column is gone
	for _, col := range updatedTable.Columns {
		if col.Name == "email" {
			t.Error("Email column should have been dropped")
		}
	}
}

func TestAlterTableAddColumnOperator_DuplicateColumn(t *testing.T) {
	cat := catalog.NewMemoryCatalog()
	storage := newMockStorageBackend()

	// Create a test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
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

	// Try to add a column that already exists
	columnDef := &parser.ColumnDef{
		Name:     "name", // Duplicate column name
		DataType: types.Varchar(100),
	}

	addOp := NewAlterTableAddColumnOperator("public", "users", columnDef, cat, storage)

	ctx := &ExecContext{
		Catalog: cat,
		Stats:   &ExecStats{},
	}

	// Should fail with duplicate column error
	err = addOp.Open(ctx)
	if err == nil {
		t.Fatal("Expected error when adding duplicate column")
	}

	expectedErr := "column 'name' already exists in table 'public.users'"
	if err.Error() != expectedErr {
		t.Fatalf("Expected error '%s', got '%s'", expectedErr, err.Error())
	}
}

func TestAlterTableDropColumnOperator_LastColumn(t *testing.T) {
	cat := catalog.NewMemoryCatalog()
	storage := newMockStorageBackend()

	// Create a test table with only one column
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{
				Name:     "id",
				DataType: types.Integer,
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

	// Try to drop the last column
	dropOp := NewAlterTableDropColumnOperator("public", "users", "id", cat, storage)

	ctx := &ExecContext{
		Catalog: cat,
		Stats:   &ExecStats{},
	}

	// Should fail with "cannot drop last column" error
	err = dropOp.Open(ctx)
	if err == nil {
		t.Fatal("Expected error when dropping last column")
	}

	expectedErr := "cannot drop the last column from table 'public.users'"
	if err.Error() != expectedErr {
		t.Fatalf("Expected error '%s', got '%s'", expectedErr, err.Error())
	}
}

func TestAlterTableOperators_Schema(t *testing.T) {
	cat := catalog.NewMemoryCatalog()
	storage := newMockStorageBackend()

	columnDef := &parser.ColumnDef{
		Name:     "email",
		DataType: types.Varchar(100),
	}

	addOp := NewAlterTableAddColumnOperator("public", "users", columnDef, cat, storage)
	dropOp := NewAlterTableDropColumnOperator("public", "users", "email", cat, storage)

	// Test ADD COLUMN schema
	schema := addOp.Schema()
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

	// Test DROP COLUMN schema (should be the same)
	schema2 := dropOp.Schema()
	if len(schema2.Columns) != 1 || schema2.Columns[0].Name != "result" {
		t.Error("DROP COLUMN schema should match ADD COLUMN schema")
	}
}
