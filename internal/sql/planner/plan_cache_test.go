package planner

import (
	"fmt"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestPlanCache_BasicOperations(t *testing.T) {
	cache := NewPlanCache(3)

	// Create test key and plan
	key := &PlanCacheKey{
		SQL:            "SELECT * FROM users WHERE id = ?",
		ParameterTypes: []types.DataType{types.Integer},
		SchemaVersion:  1,
		StatsVersion:   1,
	}

	plan := &CachedPlan{
		OriginalSQL:    key.SQL,
		ParameterTypes: key.ParameterTypes,
		CreatedAt:      time.Now(),
		LastUsed:       time.Now(),
		EstimatedCost:  100.0,
		PlanDepth:      3,
		TableCount:     1,
		SchemaVersion:  key.SchemaVersion,
		StatsVersion:   key.StatsVersion,
	}

	// Test cache miss
	result := cache.Get(key)
	if result != nil {
		t.Error("Expected cache miss, got hit")
	}

	stats := cache.GetStats()
	if stats.MissCount != 1 {
		t.Errorf("Expected miss count 1, got %d", stats.MissCount)
	}

	// Test cache put and hit
	cache.Put(key, plan)
	result = cache.Get(key)
	if result == nil {
		t.Error("Expected cache hit, got miss")
	}

	stats = cache.GetStats()
	if stats.HitCount != 1 {
		t.Errorf("Expected hit count 1, got %d", stats.HitCount)
	}
	if stats.CurrentSize != 1 {
		t.Errorf("Expected cache size 1, got %d", stats.CurrentSize)
	}

	// Verify plan data
	if result.OriginalSQL != key.SQL {
		t.Errorf("Expected SQL %s, got %s", key.SQL, result.OriginalSQL)
	}
	if result.EstimatedCost != 100.0 {
		t.Errorf("Expected cost 100.0, got %f", result.EstimatedCost)
	}
}

func TestPlanCache_LRUEviction(t *testing.T) {
	cache := NewPlanCache(2) // Small cache for testing eviction

	// Create test plans
	key1 := &PlanCacheKey{
		SQL:           "SELECT * FROM users",
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	plan1 := &CachedPlan{
		OriginalSQL:   key1.SQL,
		CreatedAt:     time.Now(),
		LastUsed:      time.Now(),
		SchemaVersion: key1.SchemaVersion,
		StatsVersion:  key1.StatsVersion,
	}

	key2 := &PlanCacheKey{
		SQL:           "SELECT * FROM orders",
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	plan2 := &CachedPlan{
		OriginalSQL:   key2.SQL,
		CreatedAt:     time.Now(),
		LastUsed:      time.Now(),
		SchemaVersion: key2.SchemaVersion,
		StatsVersion:  key2.StatsVersion,
	}

	key3 := &PlanCacheKey{
		SQL:           "SELECT * FROM products",
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	plan3 := &CachedPlan{
		OriginalSQL:   key3.SQL,
		CreatedAt:     time.Now(),
		LastUsed:      time.Now(),
		SchemaVersion: key3.SchemaVersion,
		StatsVersion:  key3.StatsVersion,
	}

	// Fill cache to capacity
	cache.Put(key1, plan1)
	cache.Put(key2, plan2)

	// Verify both are cached
	if cache.Get(key1) == nil {
		t.Error("key1 should be cached")
	}
	if cache.Get(key2) == nil {
		t.Error("key2 should be cached")
	}

	// Access key1 to make it more recently used
	cache.Get(key1)

	// Add third plan, should evict key2 (LRU)
	cache.Put(key3, plan3)

	// key1 and key3 should be cached, key2 should be evicted
	if cache.Get(key1) == nil {
		t.Error("key1 should still be cached (most recently used)")
	}
	if cache.Get(key3) == nil {
		t.Error("key3 should be cached (newly added)")
	}
	if cache.Get(key2) != nil {
		t.Error("key2 should have been evicted (LRU)")
	}

	stats := cache.GetStats()
	if stats.EvictionCount != 1 {
		t.Errorf("Expected eviction count 1, got %d", stats.EvictionCount)
	}
	if stats.CurrentSize != 2 {
		t.Errorf("Expected cache size 2, got %d", stats.CurrentSize)
	}
}

func TestPlanCache_KeyGeneration(t *testing.T) {
	cache := NewPlanCache(10)

	// Test that similar but different queries generate different keys
	key1 := &PlanCacheKey{
		SQL:           "SELECT * FROM users WHERE id = ?",
		ParameterTypes: []types.DataType{types.Integer},
		SchemaVersion: 1,
		StatsVersion:  1,
	}

	key2 := &PlanCacheKey{
		SQL:           "SELECT * FROM users WHERE name = ?",
		ParameterTypes: []types.DataType{types.Text},
		SchemaVersion: 1,
		StatsVersion:  1,
	}

	key3 := &PlanCacheKey{
		SQL:           "SELECT * FROM users WHERE id = ?",
		ParameterTypes: []types.DataType{types.Integer},
		SchemaVersion: 2, // Different schema version
		StatsVersion:  1,
	}

	keyStr1 := cache.generateKey(key1)
	keyStr2 := cache.generateKey(key2)
	keyStr3 := cache.generateKey(key3)

	// All keys should be different
	if keyStr1 == keyStr2 {
		t.Error("Different SQL queries should generate different keys")
	}
	if keyStr1 == keyStr3 {
		t.Error("Different schema versions should generate different keys")
	}
	if keyStr2 == keyStr3 {
		t.Error("All keys should be unique")
	}

	// Same key should generate same result
	keyStr1Again := cache.generateKey(key1)
	if keyStr1 != keyStr1Again {
		t.Error("Same key should generate same result")
	}
}

func TestPlanCache_SQLNormalization(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "SELECT * FROM users",
			expected: "select * from users",
		},
		{
			input:    "  SELECT   *   FROM   users  ",
			expected: "select * from users",
		},
		{
			input:    "SELECT\n*\nFROM\nusers",
			expected: "select * from users",
		},
		{
			input:    "select * from USERS",
			expected: "select * from users",
		},
	}

	for _, tt := range tests {
		result := normalizeSQL(tt.input)
		if result != tt.expected {
			t.Errorf("normalizeSQL(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestPlanCache_Statistics(t *testing.T) {
	cache := NewPlanCache(10)

	key := &PlanCacheKey{
		SQL:           "SELECT * FROM users",
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	plan := &CachedPlan{
		OriginalSQL:   key.SQL,
		CreatedAt:     time.Now(),
		LastUsed:      time.Now(),
		SchemaVersion: key.SchemaVersion,
		StatsVersion:  key.StatsVersion,
	}

	// Initial stats
	stats := cache.GetStats()
	if stats.HitCount != 0 || stats.MissCount != 0 {
		t.Error("Initial stats should be zero")
	}

	// Test miss
	cache.Get(key)
	stats = cache.GetStats()
	if stats.MissCount != 1 {
		t.Errorf("Expected miss count 1, got %d", stats.MissCount)
	}

	// Test hit
	cache.Put(key, plan)
	cache.Get(key)
	stats = cache.GetStats()
	if stats.HitCount != 1 {
		t.Errorf("Expected hit count 1, got %d", stats.HitCount)
	}

	// Test hit ratio calculation
	expectedRatio := 1.0 / 2.0 // 1 hit out of 2 total requests
	if stats.HitRatio != expectedRatio {
		t.Errorf("Expected hit ratio %f, got %f", expectedRatio, stats.HitRatio)
	}
}

func TestPlanCache_ExecutionStatsUpdate(t *testing.T) {
	cache := NewPlanCache(10)

	key := &PlanCacheKey{
		SQL:           "SELECT * FROM users",
		SchemaVersion: 1,
		StatsVersion:  1,
	}
	plan := &CachedPlan{
		OriginalSQL:   key.SQL,
		CreatedAt:     time.Now(),
		LastUsed:      time.Now(),
		SchemaVersion: key.SchemaVersion,
		StatsVersion:  key.StatsVersion,
	}

	cache.Put(key, plan)

	// Update execution stats
	execTime1 := 100 * time.Millisecond
	cache.UpdatePlanExecutionStats(key, execTime1)

	result := cache.Get(key)
	if result.ExecutionCount != 1 {
		t.Errorf("Expected execution count 1, got %d", result.ExecutionCount)
	}
	if result.TotalExecTime != execTime1 {
		t.Errorf("Expected total exec time %v, got %v", execTime1, result.TotalExecTime)
	}
	if result.AvgExecTime != execTime1 {
		t.Errorf("Expected avg exec time %v, got %v", execTime1, result.AvgExecTime)
	}

	// Update again
	execTime2 := 200 * time.Millisecond
	cache.UpdatePlanExecutionStats(key, execTime2)

	result = cache.Get(key)
	if result.ExecutionCount != 2 {
		t.Errorf("Expected execution count 2, got %d", result.ExecutionCount)
	}
	expectedTotal := execTime1 + execTime2
	if result.TotalExecTime != expectedTotal {
		t.Errorf("Expected total exec time %v, got %v", expectedTotal, result.TotalExecTime)
	}
	expectedAvg := expectedTotal / 2
	if result.AvgExecTime != expectedAvg {
		t.Errorf("Expected avg exec time %v, got %v", expectedAvg, result.AvgExecTime)
	}
}

func TestPlanCache_Invalidation(t *testing.T) {
	cache := NewPlanCache(10)

	// Add some plans
	for i := 0; i < 3; i++ {
		key := &PlanCacheKey{
			SQL:           fmt.Sprintf("SELECT * FROM table%d", i),
			SchemaVersion: 1,
			StatsVersion:  1,
		}
		plan := &CachedPlan{
			OriginalSQL:   key.SQL,
			CreatedAt:     time.Now(),
			LastUsed:      time.Now(),
			SchemaVersion: key.SchemaVersion,
			StatsVersion:  key.StatsVersion,
		}
		cache.Put(key, plan)
	}

	// Verify cache has entries
	if cache.Size() != 3 {
		t.Errorf("Expected cache size 3, got %d", cache.Size())
	}

	// Test complete invalidation
	cache.Invalidate()
	if cache.Size() != 0 {
		t.Errorf("Expected cache size 0 after invalidation, got %d", cache.Size())
	}

	stats := cache.GetStats()
	if stats.CurrentSize != 0 {
		t.Errorf("Expected current size 0 after invalidation, got %d", stats.CurrentSize)
	}
}

func TestPlanCache_ConcurrentAccess(t *testing.T) {
	cache := NewPlanCache(100)

	// Test concurrent reads and writes
	done := make(chan bool)
	
	// Writer goroutine
	go func() {
		for i := 0; i < 50; i++ {
			key := &PlanCacheKey{
				SQL:           fmt.Sprintf("SELECT * FROM table%d", i),
				SchemaVersion: 1,
				StatsVersion:  1,
			}
			plan := &CachedPlan{
				OriginalSQL:   key.SQL,
				CreatedAt:     time.Now(),
				LastUsed:      time.Now(),
				SchemaVersion: key.SchemaVersion,
				StatsVersion:  key.StatsVersion,
			}
			cache.Put(key, plan)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 50; i++ {
			key := &PlanCacheKey{
				SQL:           fmt.Sprintf("SELECT * FROM table%d", i%10),
				SchemaVersion: 1,
				StatsVersion:  1,
			}
			cache.Get(key)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Verify cache is in consistent state
	stats := cache.GetStats()
	if stats.CurrentSize < 0 || stats.CurrentSize > 100 {
		t.Errorf("Cache size inconsistent: %d", stats.CurrentSize)
	}
}