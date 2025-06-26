package executor

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/txn"
)

// TestCacheInvalidation tests basic cache invalidation functionality
func TestCacheInvalidation(t *testing.T) {
	// Create test environment
	eng := engine.NewMemoryEngine()
	cat := catalog.NewMemoryCatalog()
	txnMgr := txn.NewManager(eng, nil)
	
	cfg := NewExecutorRuntimeConfig()
	cfg.SetCachingEnabled(true)
	cfg.ResultCacheMaxSize = 10 * 1024 * 1024 // 10MB
	cfg.ResultCacheMaxEntries = 100
	cfg.ResultCacheTTL = time.Hour
	
	exec := NewConfigurableExecutor(eng, cat, txnMgr, cfg)
	
	// Create test schema
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
		},
	}
	
	// Test cache operations
	cache := exec.resultCache
	if cache == nil {
		t.Fatal("Result cache should be initialized")
	}
	
	// Test putting and getting from cache
	rows := []*Row{
		{Values: []types.Value{types.NewValue(int32(1)), types.NewValue("Alice")}},
		{Values: []types.Value{types.NewValue(int32(2)), types.NewValue("Bob")}},
	}
	
	deps := []TableDependency{
		{SchemaName: "public", TableName: "users", Version: 1},
	}
	
	queryHash := "test-query-hash"
	err := cache.Put(queryHash, rows, schema, deps)
	if err != nil {
		t.Fatalf("Failed to put result in cache: %v", err)
	}
	
	// Verify we can get it back
	cached, found := cache.Get(queryHash)
	if !found {
		t.Fatal("Should find cached result")
	}
	
	if cached.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", cached.RowCount)
	}
	
	// Test table invalidation
	cache.InvalidateTable("public", "users")
	
	// Should not find result after invalidation
	_, found = cache.Get(queryHash)
	if found {
		t.Fatal("Should not find result after table invalidation")
	}
	
	// Test invalidate all
	err = cache.Put(queryHash, rows, schema, deps)
	if err != nil {
		t.Fatalf("Failed to put result in cache: %v", err)
	}
	
	cache.InvalidateAll()
	
	_, found = cache.Get(queryHash)
	if found {
		t.Fatal("Should not find result after invalidate all")
	}
}

// TestCacheTTL tests that cache entries expire after TTL
func TestCacheTTL(t *testing.T) {
	cfg := &ResultCacheConfig{
		MaxSize:     100,
		MaxMemory:   1024 * 1024,
		TTL:         50 * time.Millisecond, // Short TTL for testing
		EnableStats: true,
	}
	
	cache := NewResultCache(cfg)
	
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
		},
	}
	
	rows := []*Row{
		{Values: []types.Value{types.NewValue(int32(1))}},
	}
	
	queryHash := "ttl-test-query"
	err := cache.Put(queryHash, rows, schema, nil)
	if err != nil {
		t.Fatalf("Failed to put result in cache: %v", err)
	}
	
	// Should find immediately
	_, found := cache.Get(queryHash)
	if !found {
		t.Fatal("Should find cached result immediately")
	}
	
	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)
	
	// Should not find after TTL
	_, found = cache.Get(queryHash)
	if found {
		t.Fatal("Should not find result after TTL expiration")
	}
}

// TestCacheEviction tests LRU eviction
func TestCacheEviction(t *testing.T) {
	cfg := &ResultCacheConfig{
		MaxSize:     2, // Small size to test eviction
		MaxMemory:   1024 * 1024,
		TTL:         time.Hour,
		EnableStats: true,
	}
	
	cache := NewResultCache(cfg)
	
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
		},
	}
	
	// Add first entry
	rows1 := []*Row{{Values: []types.Value{types.NewValue(int32(1))}}}
	err := cache.Put("query1", rows1, schema, nil)
	if err != nil {
		t.Fatalf("Failed to put result 1: %v", err)
	}
	
	// Add second entry
	rows2 := []*Row{{Values: []types.Value{types.NewValue(int32(2))}}}
	err = cache.Put("query2", rows2, schema, nil)
	if err != nil {
		t.Fatalf("Failed to put result 2: %v", err)
	}
	
	// Access first entry to make it more recently used
	_, found := cache.Get("query1")
	if !found {
		t.Fatal("Should find query1")
	}
	
	// Add third entry - should evict query2 (least recently used)
	rows3 := []*Row{{Values: []types.Value{types.NewValue(int32(3))}}}
	err = cache.Put("query3", rows3, schema, nil)
	if err != nil {
		t.Fatalf("Failed to put result 3: %v", err)
	}
	
	// query1 should still be there
	_, found = cache.Get("query1")
	if !found {
		t.Fatal("Should still find query1")
	}
	
	// query2 should be evicted
	_, found = cache.Get("query2")
	if found {
		t.Fatal("Should not find query2 after eviction")
	}
	
	// query3 should be there
	_, found = cache.Get("query3")
	if !found {
		t.Fatal("Should find query3")
	}
}