package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestInvalidatingCatalog_CreateTable(t *testing.T) {
	// Create base catalog and caching planner
	baseCatalog := catalog.NewMemoryCatalog()
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	planner := NewCachingPlannerWithCatalog(baseCatalog, config)
	
	// Create invalidating catalog
	invalidatingCatalog := NewInvalidatingCatalog(baseCatalog, planner)
	
	// Update planner to use the invalidating catalog
	planner.catalog = invalidatingCatalog
	
	// Create initial table and cache a plan
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := invalidatingCatalog.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Parse and plan a query
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
	
	// Verify plan is cached
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.GetCacheSize())
	}
	
	initialSchemaVersion := planner.schemaVersion
	
	// Create another table - should invalidate cache
	tableSchema2 := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "orders",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err = invalidatingCatalog.CreateTable(tableSchema2)
	if err != nil {
		t.Fatalf("Failed to create second table: %v", err)
	}
	
	// Schema version should have been incremented
	if planner.schemaVersion <= initialSchemaVersion {
		t.Errorf("Expected schema version to be incremented, got %d", planner.schemaVersion)
	}
	
	// Cache should be invalidated
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache to be invalidated, but size is %d", planner.GetCacheSize())
	}
}

func TestInvalidatingCatalog_DropTable(t *testing.T) {
	// Set up catalog and planner
	baseCatalog := catalog.NewMemoryCatalog()
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	planner := NewCachingPlannerWithCatalog(baseCatalog, config)
	invalidatingCatalog := NewInvalidatingCatalog(baseCatalog, planner)
	planner.catalog = invalidatingCatalog
	
	// Create table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := invalidatingCatalog.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Cache a plan
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	// Verify plan is cached
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.GetCacheSize())
	}
	
	initialSchemaVersion := planner.schemaVersion
	
	// Drop table - should invalidate cache
	err = invalidatingCatalog.DropTable("public", "users")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
	
	// Schema version should have been incremented
	if planner.schemaVersion <= initialSchemaVersion {
		t.Errorf("Expected schema version to be incremented, got %d", planner.schemaVersion)
	}
	
	// Cache should be invalidated
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache to be invalidated, but size is %d", planner.GetCacheSize())
	}
}

func TestInvalidatingCatalog_AddColumn(t *testing.T) {
	// Set up catalog and planner
	baseCatalog := catalog.NewMemoryCatalog()
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	planner := NewCachingPlannerWithCatalog(baseCatalog, config)
	invalidatingCatalog := NewInvalidatingCatalog(baseCatalog, planner)
	planner.catalog = invalidatingCatalog
	
	// Create table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := invalidatingCatalog.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Cache a plan
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	// Verify plan is cached
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.GetCacheSize())
	}
	
	initialSchemaVersion := planner.schemaVersion
	
	// Add column - should invalidate cache
	newColumn := catalog.ColumnDef{
		Name:       "name",
		DataType:   types.Text,
		IsNullable: true,
	}
	err = invalidatingCatalog.AddColumn("public", "users", newColumn)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	
	// Schema version should have been incremented
	if planner.schemaVersion <= initialSchemaVersion {
		t.Errorf("Expected schema version to be incremented, got %d", planner.schemaVersion)
	}
	
	// Cache should be invalidated
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache to be invalidated, but size is %d", planner.GetCacheSize())
	}
}

func TestInvalidatingCatalog_UpdateTableStats(t *testing.T) {
	// Set up catalog and planner
	baseCatalog := catalog.NewMemoryCatalog()
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	planner := NewCachingPlannerWithCatalog(baseCatalog, config)
	invalidatingCatalog := NewInvalidatingCatalog(baseCatalog, planner)
	planner.catalog = invalidatingCatalog
	
	// Create table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := invalidatingCatalog.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Cache a plan
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	// Verify plan is cached
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.GetCacheSize())
	}
	
	initialStatsVersion := planner.statsVersion
	
	// Update table stats - should invalidate cache via stats version
	err = invalidatingCatalog.UpdateTableStats("public", "users")
	if err != nil {
		t.Fatalf("Failed to update table stats: %v", err)
	}
	
	// Stats version should have been incremented
	if planner.statsVersion <= initialStatsVersion {
		t.Errorf("Expected stats version to be incremented, got %d", planner.statsVersion)
	}
	
	// Cache should be invalidated
	if planner.GetCacheSize() != 0 {
		t.Errorf("Expected cache to be invalidated, but size is %d", planner.GetCacheSize())
	}
}

func TestInvalidatingCatalog_NoInvalidationWhenOperationFails(t *testing.T) {
	// Set up catalog and planner
	baseCatalog := catalog.NewMemoryCatalog()
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	planner := NewCachingPlannerWithCatalog(baseCatalog, config)
	invalidatingCatalog := NewInvalidatingCatalog(baseCatalog, planner)
	planner.catalog = invalidatingCatalog
	
	// Create table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := invalidatingCatalog.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Cache a plan
	sql := "SELECT * FROM users"
	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	
	_, err = planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}
	
	// Verify plan is cached
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.GetCacheSize())
	}
	
	initialSchemaVersion := planner.schemaVersion
	
	// Try to drop non-existent table - should fail and not invalidate cache
	err = invalidatingCatalog.DropTable("public", "nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent table")
	}
	
	// Schema version should not have changed
	if planner.schemaVersion != initialSchemaVersion {
		t.Errorf("Expected schema version to remain %d, got %d", initialSchemaVersion, planner.schemaVersion)
	}
	
	// Cache should still be intact
	if planner.GetCacheSize() != 1 {
		t.Errorf("Expected cache size to remain 1, got %d", planner.GetCacheSize())
	}
}

func TestInvalidatingCatalog_WithNilPlanner(t *testing.T) {
	// Test that the catalog works without a planner (no panics)
	baseCatalog := catalog.NewMemoryCatalog()
	invalidatingCatalog := NewInvalidatingCatalog(baseCatalog, nil)
	
	// Create table - should work without panicking
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	_, err := invalidatingCatalog.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Drop table - should work without panicking
	err = invalidatingCatalog.DropTable("public", "users")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

func TestInvalidatingCatalog_SetPlanner(t *testing.T) {
	// Test setting planner after creation
	baseCatalog := catalog.NewMemoryCatalog()
	invalidatingCatalog := NewInvalidatingCatalog(baseCatalog, nil)
	
	config := CachingPlannerConfig{
		CacheSize:     10,
		Enabled:       true,
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	planner := NewCachingPlannerWithCatalog(baseCatalog, config)
	
	// Set planner
	invalidatingCatalog.SetPlanner(planner)
	
	// Now operations should affect the planner's cache
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
		},
	}
	
	initialSchemaVersion := planner.schemaVersion
	
	_, err := invalidatingCatalog.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Schema version should have been incremented
	if planner.schemaVersion <= initialSchemaVersion {
		t.Errorf("Expected schema version to be incremented after setting planner, got %d", planner.schemaVersion)
	}
}