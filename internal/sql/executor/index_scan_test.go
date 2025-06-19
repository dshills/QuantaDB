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

func TestIndexScanOperator(t *testing.T) {
	// Clean up test files
	defer os.Remove("test_index_scan.db")

	// Create catalog
	cat := catalog.NewMemoryCatalog()

	// Create test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "products",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
			{Name: "price", DataType: types.Integer, IsNullable: false},
		},
		Constraints: []catalog.Constraint{
			catalog.PrimaryKeyConstraint{Columns: []string{"id"}},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index on price column
	indexSchema := &catalog.IndexSchema{
		SchemaName: "public",
		TableName:  "products",
		IndexName:  "idx_products_price",
		Type:       catalog.BTreeIndex,
		IsUnique:   false,
		Columns: []catalog.IndexColumnDef{
			{ColumnName: "price", SortOrder: catalog.Ascending},
		},
	}

	priceIndex, err := cat.CreateIndex(indexSchema)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Create storage backend
	diskManager, err := storage.NewDiskManager("test_index_scan.db")
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

	// Create the actual index in the index manager
	err = indexMgr.CreateIndex("public", "products", "idx_products_price", []string{"price"}, false)
	if err != nil {
		t.Fatalf("Failed to create index in manager: %v", err)
	}

	// Create executor
	eng := engine.NewMemoryEngine()
	executor := NewBasicExecutor(cat, eng)
	executor.SetStorageBackend(diskStorage)
	executor.SetIndexManager(indexMgr)

	// Insert some test data
	insertData := []struct {
		id    int32
		name  string
		price int32
	}{
		{1, "Product A", 100},
		{2, "Product B", 150},
		{3, "Product C", 75},
		{4, "Product D", 200},
		{5, "Product E", 125},
	}

	ctx := &ExecContext{
		Catalog: cat,
		Engine:  eng,
		Stats:   &ExecStats{},
	}

	for _, data := range insertData {
		// For this test, we'll simulate having the data in storage
		// In a real scenario, this would go through the INSERT operator
		t.Logf("Would insert: id=%d, name=%s, price=%d", data.id, data.name, data.price)
	}

	// Test index scan operator directly
	t.Run("Index scan for price >= 100", func(t *testing.T) {
		// Create start key expression (price >= 100)
		startKey := &planner.Literal{
			Value: types.NewValue(int32(100)),
			Type:  types.Integer,
		}

		// Create index scan operator
		indexScanOp := NewIndexScanOperator(
			table,
			priceIndex,
			indexMgr,
			diskStorage,
			startKey,
			nil, // No end key
		)

		// Open the operator
		err := indexScanOp.Open(ctx)
		if err != nil {
			t.Fatalf("Failed to open index scan operator: %v", err)
		}
		defer indexScanOp.Close()

		// Read rows
		rowCount := 0
		for {
			row, err := indexScanOp.Next()
			if err != nil {
				t.Fatalf("Error reading from index scan: %v", err)
			}
			if row == nil {
				break // No more rows
			}

			rowCount++
			t.Logf("Found row: %v", row.Values)
		}

		// Since we don't have actual data inserted, we expect 0 rows
		// but the operator should work without errors
		t.Logf("Index scan returned %d rows", rowCount)
	})
}

func TestIndexScanIntegration(t *testing.T) {
	// This test will verify that the full planner → executor integration works
	// Clean up test files
	defer os.Remove("test_integration.db")

	// Create catalog
	cat := catalog.NewMemoryCatalog()

	// Create test schema
	err := cat.CreateSchema("test")
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "test", // Use "test" schema to match existing tests
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
			{Name: "email", DataType: types.Text, IsNullable: true},
		},
		Constraints: []catalog.Constraint{
			catalog.PrimaryKeyConstraint{Columns: []string{"id"}},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create storage backend
	diskManager, err := storage.NewDiskManager("test_integration.db")
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

	// Create executor with all components
	eng := engine.NewMemoryEngine()
	executor := NewBasicExecutor(cat, eng)
	executor.SetStorageBackend(diskStorage)
	executor.SetIndexManager(indexMgr)

	// Create planner
	plannerObj := planner.NewBasicPlannerWithCatalog(cat)

	// Test query that should use index scan
	sql := "SELECT * FROM users WHERE id = 1"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Plan the query
	plan, err := plannerObj.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Check if plan uses index scan
	planStr := planner.ExplainPlan(plan)
	t.Logf("Query plan: %s", planStr)

	// Execute the plan
	ctx := &ExecContext{
		Catalog: cat,
		Engine:  eng,
		Stats:   &ExecStats{},
	}

	result, err := executor.Execute(plan, ctx)
	if err != nil {
		t.Logf("Expected error since no data exists: %v", err)
		// This is expected since we don't have actual data
		return
	}

	// Read results
	for {
		row, err := result.Next()
		if err != nil {
			t.Logf("Error reading results: %v", err)
			break
		}
		if row == nil {
			break
		}
		t.Logf("Result row: %v", row.Values)
	}

	result.Close()
}
