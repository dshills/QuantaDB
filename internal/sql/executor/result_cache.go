package executor

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// ResultCache implements a cache for query results
type ResultCache struct {
	// Core cache data structures
	cache      map[string]*CachedResult
	lruList    *list.List
	
	// Configuration
	maxSize    int   // Maximum number of cached results
	maxMemory  int64 // Maximum memory usage in bytes
	ttl        time.Duration // Time-to-live for cached results
	
	// Current state
	currentMemory int64
	
	// Statistics
	stats      ResultCacheStats
	
	// Thread safety
	mu         sync.RWMutex
}

// CachedResult represents a cached query result
type CachedResult struct {
	// Result data
	Rows       []CachedRow    // Materialized result rows
	Schema     *Schema        // Result schema
	RowCount   int            // Number of rows
	
	// Metadata
	QueryHash  string         // Hash of the query
	CreatedAt  time.Time      // When the result was cached
	LastAccess time.Time      // Last access time
	AccessCount int64         // Number of times accessed
	
	// Memory usage
	MemorySize int64          // Estimated memory usage
	
	// Cache management
	lruElement *list.Element  // LRU list element
	
	// Validity
	ValidUntil time.Time      // Expiration time
	Dependencies []TableDependency // Tables this result depends on
}

// CachedRow represents a single row in cached results
type CachedRow struct {
	Values []types.Value
}

// TableDependency tracks which tables a cached result depends on
type TableDependency struct {
	SchemaName string
	TableName  string
	Version    int64 // Table version at cache time
}

// ResultCacheStats tracks cache performance metrics
type ResultCacheStats struct {
	// Hit/miss statistics
	HitCount     int64
	MissCount    int64
	EvictionCount int64
	
	// Memory statistics
	CurrentMemory int64
	PeakMemory    int64
	
	// Performance metrics
	AvgHitLatency  time.Duration
	AvgMissLatency time.Duration
	BytesSaved     int64 // Bytes saved by serving from cache
	
	mu sync.RWMutex
}

// ResultCacheConfig configures the result cache
type ResultCacheConfig struct {
	MaxSize      int           // Maximum number of cached results
	MaxMemory    int64         // Maximum memory usage in bytes
	TTL          time.Duration // Default time-to-live
	EnableStats  bool          // Whether to collect detailed statistics
}

// DefaultResultCacheConfig returns default configuration
func DefaultResultCacheConfig() *ResultCacheConfig {
	return &ResultCacheConfig{
		MaxSize:   1000,
		MaxMemory: 100 * 1024 * 1024, // 100MB
		TTL:       5 * time.Minute,
		EnableStats: true,
	}
}

// NewResultCache creates a new result cache
func NewResultCache(config *ResultCacheConfig) *ResultCache {
	if config == nil {
		config = DefaultResultCacheConfig()
	}
	
	return &ResultCache{
		cache:     make(map[string]*CachedResult),
		lruList:   list.New(),
		maxSize:   config.MaxSize,
		maxMemory: config.MaxMemory,
		ttl:       config.TTL,
	}
}

// Get retrieves a cached result if available
func (rc *ResultCache) Get(queryHash string) (*CachedResult, bool) {
	startTime := time.Now()
	
	rc.mu.RLock()
	result, exists := rc.cache[queryHash]
	rc.mu.RUnlock()
	
	if !exists {
		rc.recordMiss(time.Since(startTime))
		return nil, false
	}
	
	// Check if result has expired
	if time.Now().After(result.ValidUntil) {
		rc.mu.Lock()
		rc.evictResult(queryHash)
		rc.mu.Unlock()
		rc.recordMiss(time.Since(startTime))
		return nil, false
	}
	
	// Update access time and count
	rc.mu.Lock()
	result.LastAccess = time.Now()
	result.AccessCount++
	rc.lruList.MoveToFront(result.lruElement)
	rc.mu.Unlock()
	
	rc.recordHit(time.Since(startTime))
	return result, true
}

// Put stores a query result in the cache
func (rc *ResultCache) Put(queryHash string, rows []*Row, schema *Schema, deps []TableDependency) error {
	// Convert rows to cached format
	cachedRows := make([]CachedRow, len(rows))
	memorySize := int64(0)
	
	for i, row := range rows {
		cachedRows[i] = CachedRow{
			Values: make([]types.Value, len(row.Values)),
		}
		copy(cachedRows[i].Values, row.Values)
		
		// Estimate memory usage
		for _, val := range row.Values {
			memorySize += rc.estimateValueSize(val)
		}
	}
	
	// Add schema memory estimate
	memorySize += int64(len(schema.Columns) * 64) // Rough estimate
	
	rc.mu.Lock()
	defer rc.mu.Unlock()
	
	// Check memory limit
	if rc.currentMemory + memorySize > rc.maxMemory {
		// Evict entries until we have enough space
		for rc.currentMemory + memorySize > rc.maxMemory && rc.lruList.Len() > 0 {
			oldest := rc.lruList.Back()
			if oldest != nil {
				oldestHash := oldest.Value.(string)
				rc.evictResult(oldestHash)
			}
		}
	}
	
	// Check size limit
	if len(rc.cache) >= rc.maxSize {
		// Evict LRU entry
		oldest := rc.lruList.Back()
		if oldest != nil {
			oldestHash := oldest.Value.(string)
			rc.evictResult(oldestHash)
		}
	}
	
	// Create cached result
	cached := &CachedResult{
		Rows:         cachedRows,
		Schema:       schema,
		RowCount:     len(rows),
		QueryHash:    queryHash,
		CreatedAt:    time.Now(),
		LastAccess:   time.Now(),
		AccessCount:  0,
		MemorySize:   memorySize,
		ValidUntil:   time.Now().Add(rc.ttl),
		Dependencies: deps,
	}
	
	// Add to cache
	element := rc.lruList.PushFront(queryHash)
	cached.lruElement = element
	rc.cache[queryHash] = cached
	rc.currentMemory += memorySize
	
	// Update peak memory
	rc.stats.mu.Lock()
	if rc.currentMemory > rc.stats.PeakMemory {
		rc.stats.PeakMemory = rc.currentMemory
	}
	rc.stats.CurrentMemory = rc.currentMemory
	rc.stats.mu.Unlock()
	
	return nil
}

// InvalidateTable removes all cached results that depend on a table
func (rc *ResultCache) InvalidateTable(schemaName, tableName string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	
	// Find all results that depend on this table
	var toEvict []string
	for hash, result := range rc.cache {
		for _, dep := range result.Dependencies {
			if dep.SchemaName == schemaName && dep.TableName == tableName {
				toEvict = append(toEvict, hash)
				break
			}
		}
	}
	
	// Evict dependent results
	for _, hash := range toEvict {
		rc.evictResult(hash)
	}
}

// InvalidateAll clears the entire cache
func (rc *ResultCache) InvalidateAll() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	
	rc.cache = make(map[string]*CachedResult)
	rc.lruList.Init()
	rc.currentMemory = 0
	
	rc.stats.mu.Lock()
	rc.stats.CurrentMemory = 0
	rc.stats.mu.Unlock()
}

// evictResult removes a result from the cache (must be called with lock held)
func (rc *ResultCache) evictResult(queryHash string) {
	result, exists := rc.cache[queryHash]
	if !exists {
		return
	}
	
	// Remove from LRU list
	rc.lruList.Remove(result.lruElement)
	
	// Update memory usage
	rc.currentMemory -= result.MemorySize
	
	// Remove from cache
	delete(rc.cache, queryHash)
	
	// Update stats
	rc.stats.mu.Lock()
	rc.stats.EvictionCount++
	rc.stats.CurrentMemory = rc.currentMemory
	rc.stats.mu.Unlock()
}

// estimateValueSize estimates the memory size of a value
func (rc *ResultCache) estimateValueSize(val types.Value) int64 {
	if val.IsNull() {
		return 8 // Just the null flag
	}
	
	size := int64(16) // Base Value struct size
	
	switch v := val.Data.(type) {
	case string:
		size += int64(len(v))
	case []byte:
		size += int64(len(v))
	case int32, float32, bool:
		size += 4
	case int64, float64, time.Time:
		size += 8
	default:
		size += 32 // Generic estimate
	}
	
	return size
}

// GetStats returns cache statistics
func (rc *ResultCache) GetStats() ResultCacheStats {
	rc.stats.mu.RLock()
	defer rc.stats.mu.RUnlock()
	
	stats := rc.stats
	
	// Calculate hit rate
	total := stats.HitCount + stats.MissCount
	if total > 0 {
		stats.BytesSaved = stats.HitCount * 1024 // Rough estimate
	}
	
	return stats
}

// recordHit records a cache hit
func (rc *ResultCache) recordHit(latency time.Duration) {
	rc.stats.mu.Lock()
	defer rc.stats.mu.Unlock()
	
	rc.stats.HitCount++
	
	// Update average latency
	if rc.stats.HitCount == 1 {
		rc.stats.AvgHitLatency = latency
	} else {
		rc.stats.AvgHitLatency = (rc.stats.AvgHitLatency + latency) / 2
	}
}

// recordMiss records a cache miss
func (rc *ResultCache) recordMiss(latency time.Duration) {
	rc.stats.mu.Lock()
	defer rc.stats.mu.Unlock()
	
	rc.stats.MissCount++
	
	// Update average latency
	if rc.stats.MissCount == 1 {
		rc.stats.AvgMissLatency = latency
	} else {
		rc.stats.AvgMissLatency = (rc.stats.AvgMissLatency + latency) / 2
	}
}

// CachingExecutor wraps an executor with result caching
type CachingExecutor struct {
	executor     Executor
	resultCache  *ResultCache
	enabled      bool
}

// NewCachingExecutor creates a new caching executor
func NewCachingExecutor(executor Executor, cache *ResultCache) *CachingExecutor {
	return &CachingExecutor{
		executor:    executor,
		resultCache: cache,
		enabled:     true,
	}
}

// Execute executes a plan with result caching
func (ce *CachingExecutor) Execute(plan planner.Plan, ctx *ExecContext) (Result, error) {
	if !ce.enabled || !ce.isCacheable(plan) {
		// Execute without caching
		return ce.executor.Execute(plan, ctx)
	}
	
	// Generate cache key
	cacheKey := ce.generateCacheKey(plan, ctx)
	
	// Check cache
	if cached, found := ce.resultCache.Get(cacheKey); found {
		// Return cached result
		return &CachedResultIterator{
			result: cached,
			index:  0,
		}, nil
	}
	
	// Execute query
	result, err := ce.executor.Execute(plan, ctx)
	if err != nil {
		return nil, err
	}
	
	// Materialize and cache result
	if ce.shouldCache(plan, ctx) {
		rows, schema, err := ce.materializeResult(result)
		if err == nil && len(rows) > 0 {
			deps := ce.extractDependencies(plan)
			ce.resultCache.Put(cacheKey, rows, schema, deps)
		}
		
		// Return new iterator over materialized rows
		return &MaterializedResult{
			rows:   rows,
			schema: schema,
			index:  0,
		}, nil
	}
	
	return result, nil
}

// isCacheable determines if a plan's results can be cached
func (ce *CachingExecutor) isCacheable(plan planner.Plan) bool {
	// For now, cache all queries - in a real implementation we'd check plan types
	// TODO: Add proper plan type checking when DML plan types are defined
	return true
}

// shouldCache determines if results should be cached based on cost/benefit
func (ce *CachingExecutor) shouldCache(plan planner.Plan, ctx *ExecContext) bool {
	// Simple heuristic: cache if query took more than 10ms
	if ctx.ExecutionTime > 10*time.Millisecond {
		return true
	}
	
	// Or if it's marked as expensive in the plan
	// This would need plan cost information
	
	return false
}

// generateCacheKey generates a unique key for a query plan
func (ce *CachingExecutor) generateCacheKey(plan planner.Plan, ctx *ExecContext) string {
	// Create a string representation of the plan
	// This is simplified - a real implementation would need to be more thorough
	planStr := fmt.Sprintf("%T:%v", plan, ctx.Params)
	
	// Hash it
	hash := sha256.Sum256([]byte(planStr))
	return hex.EncodeToString(hash[:])
}

// extractDependencies extracts table dependencies from a plan
func (ce *CachingExecutor) extractDependencies(plan planner.Plan) []TableDependency {
	// This would need to walk the plan tree and find all table references
	// Simplified for now
	return []TableDependency{}
}

// materializeResult reads all rows from a result
func (ce *CachingExecutor) materializeResult(result Result) ([]*Row, *Schema, error) {
	var rows []*Row
	schema := result.Schema()
	
	for {
		row, err := result.Next()
		if err != nil {
			return nil, nil, err
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	
	return rows, schema, nil
}

// CachedResultIterator iterates over cached results
type CachedResultIterator struct {
	result *CachedResult
	index  int
}

func (cri *CachedResultIterator) Next() (*Row, error) {
	if cri.index >= len(cri.result.Rows) {
		return nil, nil
	}
	
	row := &Row{
		Values: cri.result.Rows[cri.index].Values,
	}
	cri.index++
	
	return row, nil
}

func (cri *CachedResultIterator) Close() error {
	return nil
}

func (cri *CachedResultIterator) Schema() *Schema {
	return cri.result.Schema
}

// MaterializedResult is a result backed by materialized rows
type MaterializedResult struct {
	rows   []*Row
	schema *Schema
	index  int
}

func (mr *MaterializedResult) Next() (*Row, error) {
	if mr.index >= len(mr.rows) {
		return nil, nil
	}
	
	row := mr.rows[mr.index]
	mr.index++
	
	return row, nil
}

func (mr *MaterializedResult) Close() error {
	return nil
}

func (mr *MaterializedResult) Schema() *Schema {
	return mr.schema
}