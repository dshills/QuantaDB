package planner

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestCachingPlanner_BasicCaching(t *testing.T) {
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	
	planner := NewCachingPlanner(config)
	
	// Set up test catalog
	cat := planner.catalog
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
	
	// Parse a test query
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	// First plan should be a cache miss
	plan1, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	stats := planner.GetCacheStats()
	if stats.MissCount != 1 {
		t.Errorf("Expected miss count 1, got %d", stats.MissCount)
	}
	if stats.HitCount != 0 {
		t.Errorf("Expected hit count 0, got %d", stats.HitCount)
	}
	
	// Second plan should be a cache hit
	plan2, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	stats = planner.GetCacheStats()
	if stats.MissCount != 1 {
		t.Errorf("Expected miss count 1, got %d", stats.MissCount)
	}
	if stats.HitCount != 1 {
		t.Errorf("Expected hit count 1, got %d", stats.HitCount)
	}
	
	// Plans should be equivalent (same type and schema)
	if plan1.String() != plan2.String() {
		t.Error("Cached plan should be equivalent to original plan")
	}
	
	// Cache should contain 1 plan
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.GetCacheSize())
	}
}

func TestCachingPlanner_ParameterizedQueries(t *testing.T) {
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	
	planner := NewCachingPlanner(config)
	
	// Set up test catalog
	cat := planner.catalog
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
	
	// Parse a simple query (we'll simulate parameters)
	sql := "SELECT * FROM users WHERE id = 1"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	paramTypes := []types.DataType{types.Integer}
	
	// First plan should be a cache miss
	_, err = planner.PlanWithParameters(stmt, paramTypes)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	// Second plan with same parameters should be a cache hit
	_, err = planner.PlanWithParameters(stmt, paramTypes)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	stats := planner.GetCacheStats()
	if stats.HitCount != 1 {
		t.Errorf("Expected hit count 1, got %d", stats.HitCount)
	}
	
	// Different parameter types should be a cache miss
	differentParamTypes := []types.DataType{types.Text}
	_, err = planner.PlanWithParameters(stmt, differentParamTypes)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	stats = planner.GetCacheStats()
	if stats.MissCount != 2 {
		t.Errorf("Expected miss count 2, got %d", stats.MissCount)
	}
}

func TestCachingPlanner_SchemaVersionInvalidation(t *testing.T) {
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	
	planner := NewCachingPlanner(config)
	
	// Set up test catalog
	cat := planner.catalog
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Parse a test query
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	// First plan should be cached
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	// Cache should have 1 entry
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.GetCacheSize())
	}
	
	// Update schema version should invalidate cache
	planner.UpdateSchemaVersion(2)
	
	// Cache should be empty
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache size 0 after schema update, got %d", planner.GetCacheSize())
	}
	
	// Next plan should be a cache miss
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	stats := planner.GetCacheStats()
	if stats.MissCount != 2 {
		t.Errorf("Expected miss count 2 after schema update, got %d", stats.MissCount)
	}
}

func TestCachingPlanner_DisabledCaching(t *testing.T) {
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       false, // Caching disabled
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	
	planner := NewCachingPlanner(config)
	
	// Set up test catalog
	cat := planner.catalog
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Parse a test query
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	// Plan multiple times
	for i := 0; i < 3; i++ {
		_, err = planner.Plan(stmt)
		if err != nil {
			t.Fatalf("Failed to plan query: %v", err)
		}
	}
	
	// Cache should remain empty
	stats := planner.GetCacheStats()
	if stats.HitCount != 0 || stats.MissCount != 0 {
		t.Error("Disabled cache should not record any hits or misses")
	}
	
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache size 0 when disabled, got %d", planner.GetCacheSize())
	}
}

func TestCachingPlanner_ExecutionStatsUpdate(t *testing.T) {
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	
	planner := NewCachingPlanner(config)
	
	// Set up test catalog
	cat := planner.catalog
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Parse a test query
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	// Plan query to cache it
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	// Update execution stats
	execTime := 100 * time.Millisecond
	planner.UpdatePlanExecutionStats(stmt, nil, execTime)
	
	// Get cached plan to verify stats
	cachedPlan := planner.GetCachedPlan(stmt, nil)
	if cachedPlan == nil {
		t.Fatal("Expected cached plan, got nil")
	}
	
	if cachedPlan.ExecutionCount != 1 {
		t.Errorf("Expected execution count 1, got %d", cachedPlan.ExecutionCount)
	}
	
	if cachedPlan.TotalExecTime != execTime {
		t.Errorf("Expected total exec time %v, got %v", execTime, cachedPlan.TotalExecTime)
	}
}

func TestCachingPlanner_CacheableStatements(t *testing.T) {
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	
	planner := NewCachingPlanner(config)
	
	tests := []struct {
		sql       string
		cacheable bool
	}{
		{"SELECT * FROM users", true},
		{"INSERT INTO users (name) VALUES ('test')", true},
		{"UPDATE users SET name = 'new' WHERE id = 1", true},
		{"DELETE FROM users WHERE id = 1", true},
		{"CREATE TABLE test (id INTEGER)", false},
		{"CREATE INDEX idx_users_name ON users (name)", false},
		{"DROP TABLE users", false},
		{"ALTER TABLE users ADD COLUMN email TEXT", false},
	}
	
	for _, tt := range tests {
		p := parser.NewParser(tt.sql)
		stmt, err := p.Parse()
		if err != nil {
			// Skip statements that don't parse (DDL not fully implemented)
			continue
		}
		
		cacheable := planner.isCacheable(stmt)
		if cacheable != tt.cacheable {
			t.Errorf("Statement %q: expected cacheable=%v, got %v", tt.sql, tt.cacheable, cacheable)
		}
	}
}

func TestCachingPlanner_ClearCache(t *testing.T) {
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	
	planner := NewCachingPlanner(config)
	
	// Set up test catalog and cache some plans
	cat := planner.catalog
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Cache multiple plans
	queries := []string{
		"SELECT * FROM users",
		"SELECT id FROM users",
		"SELECT * FROM users WHERE id = 1",
	}
	
	for _, sql := range queries {
		p := parser.NewParser(sql)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse SQL: %v", err)
		}
		
		_, err = planner.Plan(stmt)
		if err != nil {
			t.Fatalf("Failed to plan query: %v", err)
		}
	}
	
	// Verify cache has entries
	if planner.GetCacheSize() == 0 {
		t.Error("Expected cache to have entries")
	}
	
	// Clear cache
	planner.ClearCache()
	
	// Verify cache is empty
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache size 0 after clear, got %d", planner.GetCacheSize())
	}
}

func TestCachingPlanner_ToggleCaching(t *testing.T) {
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	
	planner := NewCachingPlanner(config)
	
	// Set up test catalog
	cat := planner.catalog
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	// Cache a plan
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	// Verify plan is cached
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.GetCacheSize())
	}
	
	// Disable caching
	planner.SetCacheEnabled(false)
	
	// Cache should be cleared
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache size 0 after disabling, got %d", planner.GetCacheSize())
	}
	
	// Plan again - should not be cached
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache size 0 when disabled, got %d", planner.GetCacheSize())
	}
	
	// Re-enable caching
	planner.SetCacheEnabled(true)
	
	// Plan again - should be cached now
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1 after re-enabling, got %d", planner.GetCacheSize())
	}
}