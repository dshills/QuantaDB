package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
)

func TestCompletePlanCacheIntegration(t *testing.T) {
	// Create a caching planner
	config := planner.CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	cachingPlanner := planner.NewCachingPlanner(config)

	// Set up test catalog
	cat := cachingPlanner.GetCatalog()
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: true},
		},
	}
	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Parse a query
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	// Plan the query multiple times to generate cache activity
	plan1, err := cachingPlanner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	plan2, err := cachingPlanner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query second time: %v", err)
	}

	// Both plans should be the same object (cache hit)
	if plan1.String() != plan2.String() {
		t.Error("Expected cached plans to be equivalent")
	}

	// Test PopulatePlanCacheStats function
	ctx := &ExecContext{
		Planner: cachingPlanner,
	}

	PopulatePlanCacheStats(ctx, cachingPlanner)

	if ctx.PlanCacheStats == nil {
		t.Fatal("Expected plan cache stats to be populated")
	}

	if ctx.PlanCacheStats.HitCount != 1 {
		t.Errorf("Expected 1 cache hit, got %d", ctx.PlanCacheStats.HitCount)
	}

	if ctx.PlanCacheStats.MissCount != 1 {
		t.Errorf("Expected 1 cache miss, got %d", ctx.PlanCacheStats.MissCount)
	}

	// Test system view creation
	sysView := CreateSystemView("plan_cache_stats", cachingPlanner)
	if sysView == nil {
		t.Fatal("Expected system view operator, got nil")
	}

	// Test EXPLAIN ANALYZE integration
	mockStorage := &TestStorageBackend{}
	mockPlan := &TestOperator{}
	explainOp := NewExplainOperator(
		mockPlan, // Mock plan operator
		true,     // analyze = true
		true,     // verbose = true
		"text",   // format
		mockStorage,
	)

	// Populate cache stats before opening explain operator
	ctx.CollectStats = true
	PopulatePlanCacheStats(ctx, cachingPlanner)

	err = explainOp.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open explain operator: %v", err)
	}

	// Get the explain output
	row, err := explainOp.Next()
	if err != nil {
		t.Fatalf("Failed to get explain output: %v", err)
	}

	if row == nil {
		t.Fatal("Expected explain output row, got nil")
	}

	// The output should include plan cache statistics since verbose=true
	output := row.Values[0].String()
	if output == "" {
		t.Error("Expected non-empty explain output")
	}

	// Clean up
	err = explainOp.Close()
	if err != nil {
		t.Fatalf("Failed to close explain operator: %v", err)
	}
}

func TestPlanCacheStatsWithoutCachingPlanner(t *testing.T) {
	// Test with basic planner (no caching)
	basicPlanner := planner.NewBasicPlanner()

	ctx := &ExecContext{
		Planner: basicPlanner,
	}

	// This should not populate cache stats
	PopulatePlanCacheStats(ctx, basicPlanner)

	if ctx.PlanCacheStats != nil {
		t.Error("Expected no plan cache stats with basic planner")
	}

	// Test system view with non-caching planner
	sysView := CreateSystemView("plan_cache_stats", basicPlanner)
	if sysView == nil {
		t.Fatal("Expected system view operator even with basic planner")
	}

	err := sysView.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open system view: %v", err)
	}

	// Should still return a row, just with zero stats
	row, err := sysView.Next()
	if err != nil {
		t.Fatalf("Failed to read from system view: %v", err)
	}

	if row == nil {
		t.Error("Expected row from system view even with basic planner")
	}

	err = sysView.Close()
	if err != nil {
		t.Fatalf("Failed to close system view: %v", err)
	}
}

// TestOperator for testing
type TestOperator struct {
	schema *Schema
}

func (t *TestOperator) Open(ctx *ExecContext) error {
	return nil
}

func (t *TestOperator) Next() (*Row, error) {
	return nil, nil // EOF
}

func (t *TestOperator) Close() error {
	return nil
}

func (t *TestOperator) Schema() *Schema {
	if t.schema == nil {
		t.schema = &Schema{
			Columns: []Column{
				{Name: "test", Type: types.Text, Nullable: true},
			},
		}
	}
	return t.schema
}

// TestStorageBackend for testing
type TestStorageBackend struct{}

func (t *TestStorageBackend) GetBufferPoolStats() *storage.BufferPoolStats {
	return &storage.BufferPoolStats{
		PagesHit:     100,
		PagesRead:    10,
		PagesDirtied: 5,
		PagesEvicted: 2,
	}
}

// Implement other required methods for StorageBackend interface
func (t *TestStorageBackend) ScanTable(tableID int64, snapshotTS int64) (RowIterator, error) {
	return nil, nil
}

func (t *TestStorageBackend) InsertRow(tableID int64, row *Row) (RowID, error) {
	return RowID{}, nil
}

func (t *TestStorageBackend) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	return nil
}

func (t *TestStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	return nil
}

func (t *TestStorageBackend) GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error) {
	return nil, nil
}

func (t *TestStorageBackend) CreateTable(table *catalog.Table) error {
	return nil
}

func (t *TestStorageBackend) DropTable(tableID int64) error {
	return nil
}

func (t *TestStorageBackend) SetTransactionID(txnID uint64) {
	// No-op for test
}
