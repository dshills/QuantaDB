package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestPlanCacheStatsIntegration(t *testing.T) {
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

	// Parse and plan a query to populate cache
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	// Plan twice to get cache hit
	_, err = cachingPlanner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	_, err = cachingPlanner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query second time: %v", err)
	}

	// Get cache statistics
	stats := cachingPlanner.GetCacheStats()
	if stats == nil {
		t.Fatal("Expected cache stats, got nil")
	}

	// Verify we have at least one hit and one miss
	if stats.HitCount != 1 {
		t.Errorf("Expected hit count 1, got %d", stats.HitCount)
	}
	if stats.MissCount != 1 {
		t.Errorf("Expected miss count 1, got %d", stats.MissCount)
	}

	// Test system view for cache statistics
	sysView := NewSystemViewOperator("plan_cache_stats", stats)
	ctx := &ExecContext{}
	
	err = sysView.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open system view: %v", err)
	}

	// Read the first row
	row, err := sysView.Next()
	if err != nil {
		t.Fatalf("Failed to read from system view: %v", err)
	}

	if row == nil {
		t.Fatal("Expected row from system view, got nil")
	}

	// Verify schema
	schema := sysView.Schema()
	expectedColumns := []string{"cache_hits", "cache_misses", "cache_size", "cache_evictions", "hit_rate_percent", "max_size"}
	if len(schema.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(schema.Columns))
	}

	for i, expected := range expectedColumns {
		if i < len(schema.Columns) && schema.Columns[i].Name != expected {
			t.Errorf("Expected column %d to be %s, got %s", i, expected, schema.Columns[i].Name)
		}
	}

	// Verify we can close
	err = sysView.Close()
	if err != nil {
		t.Fatalf("Failed to close system view: %v", err)
	}
}

func TestSystemViewSchemas(t *testing.T) {
	// Test plan_cache_stats schema
	schema := GetSystemViewSchema("plan_cache_stats")
	if schema == nil {
		t.Fatal("Expected schema for plan_cache_stats, got nil")
	}

	expectedColumns := 6
	if len(schema.Columns) != expectedColumns {
		t.Errorf("Expected %d columns in plan_cache_stats schema, got %d", expectedColumns, len(schema.Columns))
	}

	// Test unknown view type
	schema = GetSystemViewSchema("unknown_view")
	if schema == nil {
		t.Fatal("Expected schema for unknown view, got nil")
	}

	if len(schema.Columns) != 1 || schema.Columns[0].Name != "error" {
		t.Error("Expected error schema for unknown view type")
	}
}