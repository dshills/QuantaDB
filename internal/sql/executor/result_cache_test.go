package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/assert"
)

func TestResultCache(t *testing.T) {
	t.Run("NewResultCache", func(t *testing.T) {
		config := DefaultResultCacheConfig()
		cache := NewResultCache(config)

		assert.NotNil(t, cache)
		assert.Equal(t, config.MaxSize, cache.maxSize)
		assert.Equal(t, config.MaxMemory, cache.maxMemory)
		assert.Equal(t, config.TTL, cache.ttl)
		assert.NotNil(t, cache.cache)
		assert.NotNil(t, cache.lruList)
	})

	t.Run("PutAndGet", func(t *testing.T) {
		cache := NewResultCache(&ResultCacheConfig{
			MaxSize:   10,
			MaxMemory: 1024 * 1024,
			TTL:       5 * time.Minute,
		})

		// Create test data
		schema := &Schema{
			Columns: []Column{
				{Name: "id", Type: types.Integer},
				{Name: "name", Type: types.Text},
			},
		}

		rows := []*Row{
			{Values: []types.Value{
				types.NewValue(int32(1)),
				types.NewValue("Alice"),
			}},
			{Values: []types.Value{
				types.NewValue(int32(2)),
				types.NewValue("Bob"),
			}},
		}

		deps := []TableDependency{
			{SchemaName: "public", TableName: "users", Version: 1},
		}

		// Put result in cache
		queryHash := "test-query-hash"
		err := cache.Put(queryHash, rows, schema, deps)
		assert.NoError(t, err)

		// Get result from cache
		cached, found := cache.Get(queryHash)
		assert.True(t, found)
		assert.NotNil(t, cached)
		assert.Equal(t, 2, cached.RowCount)
		assert.Equal(t, schema, cached.Schema)
		assert.Equal(t, queryHash, cached.QueryHash)
		assert.Equal(t, int64(1), cached.AccessCount)

		// Verify cached rows
		assert.Equal(t, 2, len(cached.Rows))
		assert.Equal(t, int32(1), cached.Rows[0].Values[0].Data)
		assert.Equal(t, "Alice", cached.Rows[0].Values[1].Data)
		assert.Equal(t, int32(2), cached.Rows[1].Values[0].Data)
		assert.Equal(t, "Bob", cached.Rows[1].Values[1].Data)
	})

	t.Run("CacheMiss", func(t *testing.T) {
		cache := NewResultCache(DefaultResultCacheConfig())

		cached, found := cache.Get("non-existent-query")
		assert.False(t, found)
		assert.Nil(t, cached)

		// Check stats
		stats := cache.GetStats()
		assert.Equal(t, int64(0), stats.HitCount)
		assert.Equal(t, int64(1), stats.MissCount)
	})

	t.Run("TTLExpiration", func(t *testing.T) {
		cache := NewResultCache(&ResultCacheConfig{
			MaxSize:   10,
			MaxMemory: 1024 * 1024,
			TTL:       50 * time.Millisecond, // Short TTL for testing
		})

		schema := &Schema{Columns: []Column{{Name: "id", Type: types.Integer}}}
		rows := []*Row{{Values: []types.Value{types.NewValue(int32(1))}}}

		// Put result
		queryHash := "ttl-test"
		err := cache.Put(queryHash, rows, schema, nil)
		assert.NoError(t, err)

		// Should be found immediately
		cached, found := cache.Get(queryHash)
		assert.True(t, found)
		assert.NotNil(t, cached)

		// Wait for expiration
		time.Sleep(100 * time.Millisecond)

		// Should be expired
		cached, found = cache.Get(queryHash)
		assert.False(t, found)
		assert.Nil(t, cached)
	})

	t.Run("LRUEviction", func(t *testing.T) {
		cache := NewResultCache(&ResultCacheConfig{
			MaxSize:   3,
			MaxMemory: 1024 * 1024,
			TTL:       5 * time.Minute,
		})

		schema := &Schema{Columns: []Column{{Name: "id", Type: types.Integer}}}

		// Add 3 entries
		for i := 1; i <= 3; i++ {
			rows := []*Row{{Values: []types.Value{types.NewValue(int32(i))}}}
			err := cache.Put(fmt.Sprintf("query-%d", i), rows, schema, nil)
			assert.NoError(t, err)
		}

		// Access query-1 and query-2 to make them more recent
		cache.Get("query-1")
		cache.Get("query-2")

		// Add a 4th entry, should evict query-3 (least recently used)
		rows := []*Row{{Values: []types.Value{types.NewValue(int32(4))}}}
		err := cache.Put("query-4", rows, schema, nil)
		assert.NoError(t, err)

		// query-3 should be evicted
		_, found := cache.Get("query-3")
		assert.False(t, found)

		// Others should still be there
		_, found = cache.Get("query-1")
		assert.True(t, found)
		_, found = cache.Get("query-2")
		assert.True(t, found)
		_, found = cache.Get("query-4")
		assert.True(t, found)
	})

	t.Run("MemoryLimitEviction", func(t *testing.T) {
		cache := NewResultCache(&ResultCacheConfig{
			MaxSize:   100,
			MaxMemory: 500, // Very small memory limit
			TTL:       5 * time.Minute,
		})

		schema := &Schema{Columns: []Column{{Name: "data", Type: types.Text}}}

		// Add entries with large strings
		for i := 1; i <= 10; i++ {
			// Each row has ~100 bytes of data
			largeString := string(make([]byte, 100))
			rows := []*Row{{Values: []types.Value{types.NewValue(largeString)}}}
			err := cache.Put(fmt.Sprintf("query-%d", i), rows, schema, nil)
			assert.NoError(t, err)
		}

		// Should have evicted some entries to stay under memory limit
		assert.LessOrEqual(t, cache.currentMemory, cache.maxMemory)

		// Count remaining entries
		count := 0
		for i := 1; i <= 10; i++ {
			if _, found := cache.Get(fmt.Sprintf("query-%d", i)); found {
				count++
			}
		}
		assert.Less(t, count, 10)
	})

	t.Run("TableInvalidation", func(t *testing.T) {
		cache := NewResultCache(DefaultResultCacheConfig())

		schema := &Schema{Columns: []Column{{Name: "id", Type: types.Integer}}}
		rows := []*Row{{Values: []types.Value{types.NewValue(int32(1))}}}

		// Add entries with different table dependencies
		deps1 := []TableDependency{{SchemaName: "public", TableName: "users", Version: 1}}
		err := cache.Put("query-users-1", rows, schema, deps1)
		assert.NoError(t, err)

		deps2 := []TableDependency{
			{SchemaName: "public", TableName: "users", Version: 1},
			{SchemaName: "public", TableName: "orders", Version: 1},
		}
		err = cache.Put("query-users-orders", rows, schema, deps2)
		assert.NoError(t, err)

		deps3 := []TableDependency{{SchemaName: "public", TableName: "products", Version: 1}}
		err = cache.Put("query-products", rows, schema, deps3)
		assert.NoError(t, err)

		// Invalidate users table
		cache.InvalidateTable("public", "users")

		// Queries dependent on users should be gone
		_, found := cache.Get("query-users-1")
		assert.False(t, found)
		_, found = cache.Get("query-users-orders")
		assert.False(t, found)

		// Query not dependent on users should remain
		_, found = cache.Get("query-products")
		assert.True(t, found)
	})

	t.Run("InvalidateAll", func(t *testing.T) {
		cache := NewResultCache(DefaultResultCacheConfig())

		schema := &Schema{Columns: []Column{{Name: "id", Type: types.Integer}}}
		rows := []*Row{{Values: []types.Value{types.NewValue(int32(1))}}}

		// Add multiple entries
		for i := 1; i <= 5; i++ {
			err := cache.Put(fmt.Sprintf("query-%d", i), rows, schema, nil)
			assert.NoError(t, err)
		}

		// Invalidate all
		cache.InvalidateAll()

		// All entries should be gone
		for i := 1; i <= 5; i++ {
			_, found := cache.Get(fmt.Sprintf("query-%d", i))
			assert.False(t, found)
		}

		assert.Equal(t, int64(0), cache.currentMemory)
	})

	t.Run("Statistics", func(t *testing.T) {
		cache := NewResultCache(DefaultResultCacheConfig())

		schema := &Schema{Columns: []Column{{Name: "id", Type: types.Integer}}}
		rows := []*Row{{Values: []types.Value{types.NewValue(int32(1))}}}

		// Create some activity
		cache.Put("query-1", rows, schema, nil)
		cache.Get("query-1") // Hit
		cache.Get("query-1") // Hit
		cache.Get("query-2") // Miss
		cache.Get("query-3") // Miss

		stats := cache.GetStats()
		assert.Equal(t, int64(2), stats.HitCount)
		assert.Equal(t, int64(2), stats.MissCount)
		assert.Greater(t, stats.CurrentMemory, int64(0))
		assert.Greater(t, stats.PeakMemory, int64(0))
		assert.Greater(t, stats.BytesSaved, int64(0))
	})
}

func TestCachedResultIterator(t *testing.T) {
	cached := &CachedResult{
		Rows: []CachedRow{
			{Values: []types.Value{types.NewValue(int32(1))}},
			{Values: []types.Value{types.NewValue(int32(2))}},
			{Values: []types.Value{types.NewValue(int32(3))}},
		},
		Schema: &Schema{Columns: []Column{{Name: "id", Type: types.Integer}}},
	}

	iter := &CachedResultIterator{result: cached, index: 0}

	// First row
	row, err := iter.Next()
	assert.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, int32(1), row.Values[0].Data)

	// Second row
	row, err = iter.Next()
	assert.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, int32(2), row.Values[0].Data)

	// Third row
	row, err = iter.Next()
	assert.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, int32(3), row.Values[0].Data)

	// EOF
	row, err = iter.Next()
	assert.NoError(t, err)
	assert.Nil(t, row)

	// Schema
	assert.Equal(t, cached.Schema, iter.Schema())

	// Close
	assert.NoError(t, iter.Close())
}

func TestMaterializedResult(t *testing.T) {
	rows := []*Row{
		{Values: []types.Value{types.NewValue("A")}},
		{Values: []types.Value{types.NewValue("B")}},
	}
	schema := &Schema{Columns: []Column{{Name: "name", Type: types.Text}}}

	result := &MaterializedResult{rows: rows, schema: schema, index: 0}

	// First row
	row, err := result.Next()
	assert.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, "A", row.Values[0].Data)

	// Second row
	row, err = result.Next()
	assert.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, "B", row.Values[0].Data)

	// EOF
	row, err = result.Next()
	assert.NoError(t, err)
	assert.Nil(t, row)

	// Schema
	assert.Equal(t, schema, result.Schema())

	// Close
	assert.NoError(t, result.Close())
}

func BenchmarkResultCache(b *testing.B) {
	cache := NewResultCache(&ResultCacheConfig{
		MaxSize:   1000,
		MaxMemory: 100 * 1024 * 1024,
		TTL:       5 * time.Minute,
	})

	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
			{Name: "score", Type: types.Double},
		},
	}

	// Create test data
	rows := make([]*Row, 100)
	for i := 0; i < 100; i++ {
		rows[i] = &Row{
			Values: []types.Value{
				types.NewValue(int32(i)),
				types.NewValue(fmt.Sprintf("Name%d", i)),
				types.NewValue(float64(i) * 1.5),
			},
		}
	}

	b.Run("Put", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			queryHash := fmt.Sprintf("query-%d", i)
			cache.Put(queryHash, rows, schema, nil)
		}
	})

	b.Run("Get_Hit", func(b *testing.B) {
		// Pre-populate cache
		for i := 0; i < 100; i++ {
			queryHash := fmt.Sprintf("bench-query-%d", i)
			cache.Put(queryHash, rows, schema, nil)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			queryHash := fmt.Sprintf("bench-query-%d", i%100)
			cache.Get(queryHash)
		}
	})

	b.Run("Get_Miss", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			queryHash := fmt.Sprintf("missing-query-%d", i)
			cache.Get(queryHash)
		}
	})
}
