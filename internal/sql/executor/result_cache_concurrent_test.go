package executor

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/assert"
)

// TestResultCacheConcurrentAccess tests concurrent access to the cache
func TestResultCacheConcurrentAccess(t *testing.T) {
	cache := NewResultCache(&ResultCacheConfig{
		MaxSize:   100,
		MaxMemory: 10 * 1024 * 1024, // 10MB
		TTL:       1 * time.Minute,
	})

	// Test data
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "value", Type: types.Text},
		},
	}

	createTestRows := func(id int) []*Row {
		return []*Row{
			{Values: []types.Value{
				types.NewValue(int32(id)),
				types.NewValue(fmt.Sprintf("value-%d", id)),
			}},
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Start multiple readers
	numReaders := 10
	readsPerReader := 1000
	
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			
			for j := 0; j < readsPerReader; j++ {
				queryHash := fmt.Sprintf("query-%d", j%50)
				_, found := cache.Get(queryHash)
				
				// Some queries should be found after writers populate cache
				if j > 100 && !found && j%50 < 25 {
					// This is expected - not all queries are cached
				}
			}
		}(i)
	}

	// Start multiple writers
	numWriters := 5
	writesPerWriter := 200
	
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			
			for j := 0; j < writesPerWriter; j++ {
				queryHash := fmt.Sprintf("query-%d", j%50)
				rows := createTestRows(j)
				
				err := cache.Put(queryHash, rows, schema, []TableDependency{
					{SchemaName: "public", TableName: "test"},
				})
				
				if err != nil {
					errors <- fmt.Errorf("writer %d: %w", writerID, err)
				}
			}
		}(i)
	}

	// Start invalidators
	numInvalidators := 3
	invalidationsPerWorker := 50
	
	for i := 0; i < numInvalidators; i++ {
		wg.Add(1)
		go func(invalidatorID int) {
			defer wg.Done()
			
			for j := 0; j < invalidationsPerWorker; j++ {
				if j%10 == 0 {
					// Occasionally invalidate all
					cache.InvalidateAll()
				} else {
					// Usually invalidate specific table
					cache.InvalidateTable("public", "test")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	// Wait for all goroutines
	done := make(chan bool)
	go func() {
		wg.Wait()
		close(done)
	}()

	// Check for errors or timeout
	select {
	case <-done:
		// Success
	case err := <-errors:
		t.Fatalf("Error during concurrent access: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("Test timeout - possible deadlock")
	}

	// Verify cache is still functional
	testHash := "test-after-concurrent"
	testRows := createTestRows(9999)
	
	err := cache.Put(testHash, testRows, schema, nil)
	assert.NoError(t, err)
	
	result, found := cache.Get(testHash)
	assert.True(t, found)
	assert.NotNil(t, result)
	assert.Equal(t, 1, result.RowCount)
}

// TestResultCacheExpirationRace tests the race condition during expiration
func TestResultCacheExpirationRace(t *testing.T) {
	cache := NewResultCache(&ResultCacheConfig{
		MaxSize:   100,
		MaxMemory: 1024 * 1024,
		TTL:       50 * time.Millisecond, // Short TTL to trigger expiration
	})

	schema := &Schema{
		Columns: []Column{{Name: "id", Type: types.Integer}},
	}
	
	rows := []*Row{
		{Values: []types.Value{types.NewValue(int32(1))}},
	}

	queryHash := "expiring-query"
	
	// Put an entry that will expire soon
	err := cache.Put(queryHash, rows, schema, nil)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Start multiple goroutines trying to access the expiring entry
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Wait for the entry to be close to expiration
			time.Sleep(45 * time.Millisecond)
			
			// Try to get the entry around expiration time
			for j := 0; j < 20; j++ {
				_, found := cache.Get(queryHash)
				// Both found and not found are valid
				_ = found
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	// Wait for completion
	done := make(chan bool)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case err := <-errors:
		t.Fatalf("Error during expiration race test: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Test timeout")
	}
}

// TestResultCacheMemoryPressure tests behavior under memory pressure
func TestResultCacheMemoryPressure(t *testing.T) {
	cache := NewResultCache(&ResultCacheConfig{
		MaxSize:   1000,
		MaxMemory: 1024, // Very small memory limit
		TTL:       5 * time.Minute,
	})

	schema := &Schema{
		Columns: []Column{
			{Name: "data", Type: types.Text},
		},
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Start multiple writers that will cause memory pressure
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < 100; j++ {
				// Create large rows
				largeData := fmt.Sprintf("%d-%d-%s", id, j, string(make([]byte, 100)))
				rows := []*Row{
					{Values: []types.Value{types.NewValue(largeData)}},
				}
				
				queryHash := fmt.Sprintf("query-%d-%d", id, j)
				err := cache.Put(queryHash, rows, schema, nil)
				
				if err != nil {
					errors <- fmt.Errorf("writer %d: %w", id, err)
				}
			}
		}(i)
	}

	// Wait for completion
	done := make(chan bool)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - cache should handle memory pressure by eviction
		stats := cache.GetStats()
		assert.True(t, stats.EvictionCount > 0, "Expected evictions under memory pressure")
		assert.True(t, stats.CurrentMemory <= cache.maxMemory, "Memory usage should not exceed limit")
	case err := <-errors:
		t.Fatalf("Error during memory pressure test: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Test timeout")
	}
}

// TestResultCacheStatsAccuracy tests concurrent stats updates
func TestResultCacheStatsAccuracy(t *testing.T) {
	cache := NewResultCache(&ResultCacheConfig{
		MaxSize:     100,
		MaxMemory:   10 * 1024 * 1024,
		TTL:         1 * time.Minute,
		EnableStats: true,
	})

	schema := &Schema{
		Columns: []Column{{Name: "id", Type: types.Integer}},
	}
	
	rows := []*Row{
		{Values: []types.Value{types.NewValue(int32(1))}},
	}

	// Pre-populate cache
	for i := 0; i < 50; i++ {
		cache.Put(fmt.Sprintf("query-%d", i), rows, schema, nil)
	}

	var wg sync.WaitGroup
	numWorkers := 10
	opsPerWorker := 1000

	expectedHits := int64(0)
	expectedMisses := int64(0)
	var mu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < opsPerWorker; j++ {
				queryHash := fmt.Sprintf("query-%d", j%100)
				_, found := cache.Get(queryHash)
				
				mu.Lock()
				if found {
					expectedHits++
				} else {
					expectedMisses++
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	stats := cache.GetStats()
	
	// Stats should match our counts
	assert.Equal(t, expectedHits, stats.HitCount, "Hit count mismatch")
	assert.Equal(t, expectedMisses, stats.MissCount, "Miss count mismatch")
	
	// Hit rate should be reasonable (we pre-populated 50 out of 100 possible queries)
	totalOps := expectedHits + expectedMisses
	hitRate := float64(expectedHits) / float64(totalOps)
	assert.True(t, hitRate > 0.4 && hitRate < 0.6, "Hit rate should be around 50%%: %.2f", hitRate)
}