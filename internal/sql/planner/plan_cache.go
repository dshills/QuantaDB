package planner

import (
	"container/list"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// PlanCache provides LRU caching for query plans to eliminate planning overhead
// for repeated queries. Thread-safe with configurable size limits.
type PlanCache struct {
	// Core cache data structures
	cache     map[string]*cacheEntry // Key -> Cache entry mapping
	lruList   *list.List             // LRU ordering of cache keys
	maxSize   int                    // Maximum number of cached plans
	mu        sync.RWMutex           // Read-write mutex for thread safety

	// Statistics for monitoring cache performance
	stats PlanCacheStats
}

// cacheEntry represents an entry in the LRU list
type cacheEntry struct {
	key      string      // Cache key
	plan     *CachedPlan // Cached plan data
	lruNode  *list.Element // Node in LRU list
}

// CachedPlan represents a cached query plan with metadata
type CachedPlan struct {
	// Core plan data
	Plan           Plan                 // The actual physical plan
	OriginalSQL    string               // Original SQL query text
	ParameterTypes []types.DataType     // Parameter types for parameterized queries
	Schema         *Schema              // Output schema of the plan
	
	// Usage statistics
	CreatedAt      time.Time            // When this plan was first cached
	LastUsed       time.Time            // Last access time for LRU
	ExecutionCount int64                // Number of times this plan was executed
	TotalExecTime  time.Duration        // Cumulative execution time
	AvgExecTime    time.Duration        // Average execution time
	
	// Plan metadata
	EstimatedCost  float64              // Optimizer cost estimate
	PlanDepth      int                  // Depth of plan tree
	TableCount     int                  // Number of tables accessed
	IndexCount     int                  // Number of indexes used
	
	// Cache metadata
	SchemaVersion  int64                // Schema version when plan was created
	StatsVersion   int64                // Statistics version when plan was created
}

// PlanCacheStats tracks cache performance metrics
type PlanCacheStats struct {
	// Hit/miss statistics
	HitCount          int64   // Number of cache hits
	MissCount         int64   // Number of cache misses
	EvictionCount     int64   // Number of plans evicted
	InsertionCount    int64   // Number of plans inserted
	
	// Performance metrics
	HitRatio          float64 // Cache hit ratio (hits / (hits + misses))
	AvgLookupTime     time.Duration // Average cache lookup time
	AvgPlanningTime   time.Duration // Average planning time for cache misses
	PlanningTimeSpent time.Duration // Total time spent planning (cache misses)
	PlanningSaved     time.Duration // Total planning time saved (cache hits)
	
	// Cache state
	CurrentSize       int     // Current number of cached plans
	MaxSize           int     // Maximum cache size
	MemoryUsage       int64   // Estimated memory usage in bytes
	
	mu sync.RWMutex
}

// PlanCacheKey represents a cache key for looking up plans
type PlanCacheKey struct {
	SQL            string               // Normalized SQL text
	ParameterTypes []types.DataType     // Parameter types
	SchemaVersion  int64                // Schema version
	StatsVersion   int64                // Statistics version
}

// NewPlanCache creates a new plan cache with the specified maximum size
func NewPlanCache(maxSize int) *PlanCache {
	if maxSize <= 0 {
		maxSize = 1000 // Default cache size
	}
	
	return &PlanCache{
		cache:   make(map[string]*cacheEntry),
		lruList: list.New(),
		maxSize: maxSize,
		stats: PlanCacheStats{
			MaxSize: maxSize,
		},
	}
}

// Get retrieves a cached plan if available, returns nil if not found
func (pc *PlanCache) Get(key *PlanCacheKey) *CachedPlan {
	startTime := time.Now()
	defer func() {
		pc.updateLookupTime(time.Since(startTime))
	}()
	
	keyStr := pc.generateKey(key)
	
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	entry, exists := pc.cache[keyStr]
	if !exists {
		pc.recordMiss()
		return nil
	}
	
	// Update LRU order - move to front
	pc.lruList.MoveToFront(entry.lruNode)
	
	// Update access time
	entry.plan.LastUsed = time.Now()
	
	pc.recordHit()
	return entry.plan
}

// Put stores a plan in the cache, evicting LRU entries if necessary
func (pc *PlanCache) Put(key *PlanCacheKey, plan *CachedPlan) {
	keyStr := pc.generateKey(key)
	
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	// Check if key already exists
	if entry, exists := pc.cache[keyStr]; exists {
		// Update existing entry
		entry.plan = plan
		pc.lruList.MoveToFront(entry.lruNode)
		return
	}
	
	// Check if we need to evict
	if len(pc.cache) >= pc.maxSize {
		pc.evictLRU()
	}
	
	// Create new entry
	newEntry := &cacheEntry{
		key:  keyStr,
		plan: plan,
	}
	
	// Add to LRU list (front = most recently used)
	newEntry.lruNode = pc.lruList.PushFront(keyStr)
	
	// Add to cache map
	pc.cache[keyStr] = newEntry
	
	pc.recordInsertion()
}

// evictLRU removes the least recently used entry from the cache
// Must be called with lock held
func (pc *PlanCache) evictLRU() {
	if pc.lruList.Len() == 0 {
		return
	}
	
	// Get least recently used entry (back of list)
	lruElement := pc.lruList.Back()
	if lruElement == nil {
		return
	}
	
	keyStr := lruElement.Value.(string)
	
	// Remove from cache map
	delete(pc.cache, keyStr)
	
	// Remove from LRU list
	pc.lruList.Remove(lruElement)
	
	pc.recordEviction()
}

// Invalidate removes all cached plans (called on schema changes)
func (pc *PlanCache) Invalidate() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	// Clear cache map
	pc.cache = make(map[string]*cacheEntry)
	
	// Clear LRU list
	pc.lruList.Init()
	
	// Reset current size
	pc.stats.mu.Lock()
	pc.stats.CurrentSize = 0
	pc.stats.mu.Unlock()
}

// InvalidateTable removes cached plans that access a specific table
func (pc *PlanCache) InvalidateTable(tableName string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	// For now, invalidate all plans. In the future, we could analyze
	// plan dependencies and only invalidate plans that use this table
	pc.cache = make(map[string]*cacheEntry)
	pc.lruList.Init()
	
	pc.stats.mu.Lock()
	pc.stats.CurrentSize = 0
	pc.stats.mu.Unlock()
}

// GetStats returns a copy of current cache statistics
func (pc *PlanCache) GetStats() PlanCacheStats {
	pc.stats.mu.RLock()
	defer pc.stats.mu.RUnlock()
	
	// Calculate hit ratio
	totalRequests := pc.stats.HitCount + pc.stats.MissCount
	var hitRatio float64
	if totalRequests > 0 {
		hitRatio = float64(pc.stats.HitCount) / float64(totalRequests)
	}
	
	stats := pc.stats
	stats.HitRatio = hitRatio
	stats.CurrentSize = len(pc.cache)
	
	return stats
}

// generateKey creates a cache key from the normalized components
func (pc *PlanCache) generateKey(key *PlanCacheKey) string {
	// Normalize SQL by removing extra whitespace and converting to lowercase
	normalizedSQL := normalizeSQL(key.SQL)
	
	// Create parameter type signature
	var paramSig strings.Builder
	for i, paramType := range key.ParameterTypes {
		if i > 0 {
			paramSig.WriteString(",")
		}
		paramSig.WriteString(paramType.Name())
	}
	
	// Combine all components
	combined := fmt.Sprintf("%s|%s|%d|%d",
		normalizedSQL,
		paramSig.String(),
		key.SchemaVersion,
		key.StatsVersion,
	)
	
	// Hash to create fixed-length key
	hash := sha256.Sum256([]byte(combined))
	return fmt.Sprintf("%x", hash)
}

// normalizeSQL normalizes SQL text for consistent cache keys
func normalizeSQL(sql string) string {
	// Convert to lowercase and trim whitespace
	normalized := strings.TrimSpace(strings.ToLower(sql))
	
	// Replace multiple whitespace with single space
	// This is a simple normalization - could be enhanced with proper SQL parsing
	words := strings.Fields(normalized)
	return strings.Join(words, " ")
}

// Statistics recording methods
func (pc *PlanCache) recordHit() {
	pc.stats.mu.Lock()
	defer pc.stats.mu.Unlock()
	pc.stats.HitCount++
}

func (pc *PlanCache) recordMiss() {
	pc.stats.mu.Lock()
	defer pc.stats.mu.Unlock()
	pc.stats.MissCount++
}

func (pc *PlanCache) recordEviction() {
	pc.stats.mu.Lock()
	defer pc.stats.mu.Unlock()
	pc.stats.EvictionCount++
}

func (pc *PlanCache) recordInsertion() {
	pc.stats.mu.Lock()
	defer pc.stats.mu.Unlock()
	pc.stats.InsertionCount++
	pc.stats.CurrentSize = len(pc.cache)
}

func (pc *PlanCache) updateLookupTime(duration time.Duration) {
	pc.stats.mu.Lock()
	defer pc.stats.mu.Unlock()
	
	// Update rolling average of lookup time
	if pc.stats.HitCount+pc.stats.MissCount == 1 {
		pc.stats.AvgLookupTime = duration
	} else {
		// Simple moving average
		pc.stats.AvgLookupTime = (pc.stats.AvgLookupTime + duration) / 2
	}
}

// UpdatePlanExecutionStats updates execution statistics for a cached plan
func (pc *PlanCache) UpdatePlanExecutionStats(key *PlanCacheKey, execTime time.Duration) {
	keyStr := pc.generateKey(key)
	
	pc.mu.RLock()
	entry, exists := pc.cache[keyStr]
	pc.mu.RUnlock()
	
	if !exists {
		return
	}
	
	// Update execution statistics
	entry.plan.ExecutionCount++
	entry.plan.TotalExecTime += execTime
	entry.plan.AvgExecTime = entry.plan.TotalExecTime / time.Duration(entry.plan.ExecutionCount)
}

// Size returns the current number of cached plans
func (pc *PlanCache) Size() int {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return len(pc.cache)
}

// Clear removes all cached plans
func (pc *PlanCache) Clear() {
	pc.Invalidate()
}