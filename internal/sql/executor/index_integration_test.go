package executor

import (
	"os"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
)

func TestCreateIndexIntegration(t *testing.T) {
	// Clean up test files
	defer os.Remove("test.db")

	// Create catalog
	cat := catalog.NewMemoryCatalog()

	// Create a test table first
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "email", DataType: types.Text, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: true},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create storage
	diskManager, err := storage.NewDiskManager("test.db")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()
	bufferPool := storage.NewBufferPool(diskManager, 128)
	diskStorage := NewDiskStorageBackend(bufferPool, cat)
	err = diskStorage.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Create index manager
	indexMgr := index.NewManager(cat)

	// Create engine and executor
	eng := engine.NewMemoryEngine()
	exec := NewBasicExecutor(cat, eng)
	exec.SetStorageBackend(diskStorage)
	exec.SetIndexManager(indexMgr)

	// Test CREATE INDEX
	sql := "CREATE INDEX idx_users_email ON users (email)"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse CREATE INDEX: %v", err)
	}

	// Plan the statement
	planner := planner.NewBasicPlannerWithCatalog(cat)
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan CREATE INDEX: %v", err)
	}

	// Execute the plan
	ctx := &ExecContext{
		Catalog: cat,
		Engine:  eng,
		Stats:   &ExecStats{},
	}

	result, err := exec.Execute(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to execute CREATE INDEX: %v", err)
	}

	// Check result
	row, err := result.Next()
	if err != nil {
		t.Fatalf("Failed to get result: %v", err)
	}

	if row == nil {
		t.Fatal("Expected result row, got nil")
	}

	if len(row.Values) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(row.Values))
	}

	resultMsg := row.Values[0].String()
	expected := "Index 'idx_users_email' created on table 'public.users'"
	if resultMsg != expected {
		t.Errorf("Expected result '%s', got '%s'", expected, resultMsg)
	}

	// Verify index was created in catalog
	updatedTable, err := cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if len(updatedTable.Indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(updatedTable.Indexes))
	}

	idx := updatedTable.Indexes[0]
	if idx.Name != "idx_users_email" {
		t.Errorf("Expected index name 'idx_users_email', got '%s'", idx.Name)
	}

	if len(idx.Columns) != 1 || idx.Columns[0].Column.Name != "email" {
		t.Errorf("Expected index on column 'email', got %v", idx.Columns)
	}

	// Close result
	if err := result.Close(); err != nil {
		t.Errorf("Failed to close result: %v", err)
	}
}

func TestCreateUniqueIndexIntegration(t *testing.T) {
	// Clean up test files
	defer os.Remove("test.db")

	// Create catalog
	cat := catalog.NewMemoryCatalog()

	// Create a test table first
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "products",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "sku", DataType: types.Text, IsNullable: false},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create storage
	diskManager, err := storage.NewDiskManager("test.db")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()
	bufferPool := storage.NewBufferPool(diskManager, 128)
	diskStorage := NewDiskStorageBackend(bufferPool, cat)
	err = diskStorage.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Create index manager
	indexMgr := index.NewManager(cat)

	// Create engine and executor
	eng := engine.NewMemoryEngine()
	exec := NewBasicExecutor(cat, eng)
	exec.SetStorageBackend(diskStorage)
	exec.SetIndexManager(indexMgr)

	// Test CREATE UNIQUE INDEX
	sql := "CREATE UNIQUE INDEX idx_products_sku ON products (sku)"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse CREATE UNIQUE INDEX: %v", err)
	}

	// Plan the statement
	planner := planner.NewBasicPlannerWithCatalog(cat)
	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan CREATE UNIQUE INDEX: %v", err)
	}

	// Execute the plan
	ctx := &ExecContext{
		Catalog: cat,
		Engine:  eng,
		Stats:   &ExecStats{},
	}

	result, err := exec.Execute(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to execute CREATE UNIQUE INDEX: %v", err)
	}

	// Verify index was created
	updatedTable, err := cat.GetTable("public", "products")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if len(updatedTable.Indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(updatedTable.Indexes))
	}

	idx := updatedTable.Indexes[0]
	if !idx.IsUnique {
		t.Error("Expected unique index, got non-unique")
	}

	// Close result
	if err := result.Close(); err != nil {
		t.Errorf("Failed to close result: %v", err)
	}
}
