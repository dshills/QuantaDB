package planner

import (
	"fmt"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CachingPlanner wraps a BasicPlanner with plan caching capabilities
// to eliminate planning overhead for repeated queries.
type CachingPlanner struct {
	basePlanner   *BasicPlanner
	cache         *PlanCache
	catalog       catalog.Catalog
	enabled       bool
	schemaVersion int64
	statsVersion  int64
}

// CachingPlannerConfig configures the caching planner
type CachingPlannerConfig struct {
	CacheSize     int   // Maximum number of cached plans
	Enabled       bool  // Whether caching is enabled
	SchemaVersion int64 // Current schema version for cache invalidation
	StatsVersion  int64 // Current statistics version for cache invalidation
}

// NewCachingPlanner creates a new caching planner with the specified configuration
func NewCachingPlanner(config CachingPlannerConfig) *CachingPlanner {
	basePlanner := NewBasicPlanner()
	
	cacheSize := config.CacheSize
	if cacheSize <= 0 {
		cacheSize = 1000 // Default cache size
	}
	
	return &CachingPlanner{
		basePlanner:   basePlanner,
		cache:         NewPlanCache(cacheSize),
		catalog:       basePlanner.catalog,
		enabled:       config.Enabled,
		schemaVersion: config.SchemaVersion,
		statsVersion:  config.StatsVersion,
	}
}

// NewCachingPlannerWithCatalog creates a new caching planner with a specific catalog
func NewCachingPlannerWithCatalog(cat catalog.Catalog, config CachingPlannerConfig) *CachingPlanner {
	basePlanner := NewBasicPlannerWithCatalog(cat)
	
	cacheSize := config.CacheSize
	if cacheSize <= 0 {
		cacheSize = 1000 // Default cache size
	}
	
	return &CachingPlanner{
		basePlanner:   basePlanner,
		cache:         NewPlanCache(cacheSize),
		catalog:       cat,
		enabled:       config.Enabled,
		schemaVersion: config.SchemaVersion,
		statsVersion:  config.StatsVersion,
	}
}

// Plan transforms a statement into a query plan, using caching when possible
func (cp *CachingPlanner) Plan(stmt parser.Statement) (Plan, error) {
	return cp.PlanWithParameters(stmt, nil)
}

// PlanWithParameters plans a statement with parameter types (for prepared statements)
func (cp *CachingPlanner) PlanWithParameters(stmt parser.Statement, paramTypes []types.DataType) (Plan, error) {
	// If caching is disabled, use base planner directly
	if !cp.enabled {
		return cp.basePlanner.Plan(stmt)
	}
	
	// Generate cache key
	cacheKey := cp.generateCacheKey(stmt, paramTypes)
	
	// Check cache first
	startTime := time.Now()
	cachedPlan := cp.cache.Get(cacheKey)
	if cachedPlan != nil {
		// Cache hit - update stats and return cached plan
		cp.updateCacheHitStats(time.Since(startTime))
		return cachedPlan.Plan, nil
	}
	
	// Cache miss - plan the query
	planningStart := time.Now()
	plan, err := cp.basePlanner.Plan(stmt)
	if err != nil {
		return nil, err
	}
	planningTime := time.Since(planningStart)
	
	// Cache the plan if it's cacheable
	if cp.isCacheable(stmt) {
		cachedPlan := &CachedPlan{
			Plan:           plan,
			OriginalSQL:    cp.extractSQL(stmt),
			ParameterTypes: paramTypes,
			Schema:         plan.Schema(),
			CreatedAt:      time.Now(),
			LastUsed:       time.Now(),
			ExecutionCount: 0,
			TotalExecTime:  0,
			AvgExecTime:    0,
			EstimatedCost:  cp.estimatePlanCost(plan),
			PlanDepth:      cp.calculatePlanDepth(plan),
			TableCount:     cp.countTables(plan),
			IndexCount:     cp.countIndexes(plan),
			SchemaVersion:  cp.schemaVersion,
			StatsVersion:   cp.statsVersion,
		}
		
		cp.cache.Put(cacheKey, cachedPlan)
	}
	
	cp.updateCacheMissStats(time.Since(startTime), planningTime)
	return plan, nil
}

// generateCacheKey creates a cache key for the given statement and parameters
func (cp *CachingPlanner) generateCacheKey(stmt parser.Statement, paramTypes []types.DataType) *PlanCacheKey {
	return &PlanCacheKey{
		SQL:            cp.extractSQL(stmt),
		ParameterTypes: paramTypes,
		SchemaVersion:  cp.schemaVersion,
		StatsVersion:   cp.statsVersion,
	}
}

// extractSQL extracts normalized SQL text from a parsed statement
func (cp *CachingPlanner) extractSQL(stmt parser.Statement) string {
	// For now, we'll use a simple string representation
	// In a full implementation, we'd need to reconstruct normalized SQL
	// or store the original SQL text during parsing
	return normalizeSQL(fmt.Sprintf("%s", stmt))
}

// isCacheable determines if a statement's plan should be cached
func (cp *CachingPlanner) isCacheable(stmt parser.Statement) bool {
	switch stmt.(type) {
	case *parser.SelectStmt:
		return true // SELECT queries are highly cacheable
	case *parser.InsertStmt:
		return true // INSERT with prepared statements benefit from caching
	case *parser.UpdateStmt:
		return true // UPDATE queries can be cached
	case *parser.DeleteStmt:
		return true // DELETE queries can be cached
	case *parser.CreateTableStmt:
		return false // DDL statements are typically one-time operations
	case *parser.CreateIndexStmt:
		return false // DDL statements are typically one-time operations
	case *parser.DropTableStmt:
		return false // DDL statements are typically one-time operations
	case *parser.AlterTableStmt:
		return false // DDL statements are typically one-time operations
	default:
		return false // Conservative: don't cache unknown statement types
	}
}

// estimatePlanCost estimates the cost of a plan for caching statistics
func (cp *CachingPlanner) estimatePlanCost(plan Plan) float64 {
	// Simple cost estimation based on plan type and depth
	// In a full implementation, this would use the optimizer's cost model
	cost := 1.0
	
	// Add cost based on plan depth
	depth := cp.calculatePlanDepth(plan)
	cost += float64(depth) * 10.0
	
	// Add cost based on number of tables
	tableCount := cp.countTables(plan)
	cost += float64(tableCount) * 100.0
	
	return cost
}

// calculatePlanDepth calculates the depth of a plan tree
func (cp *CachingPlanner) calculatePlanDepth(plan Plan) int {
	if plan == nil {
		return 0
	}
	
	children := plan.Children()
	if len(children) == 0 {
		return 1
	}
	
	maxChildDepth := 0
	for _, child := range children {
		childDepth := cp.calculatePlanDepth(child)
		if childDepth > maxChildDepth {
			maxChildDepth = childDepth
		}
	}
	
	return 1 + maxChildDepth
}

// countTables counts the number of table accesses in a plan
func (cp *CachingPlanner) countTables(plan Plan) int {
	if plan == nil {
		return 0
	}
	
	count := 0
	
	// Check if this is a table scan node
	switch plan.(type) {
	case *LogicalScan:
		count = 1
	case *IndexScan:
		count = 1
	}
	
	// Recursively count children
	for _, child := range plan.Children() {
		count += cp.countTables(child)
	}
	
	return count
}

// countIndexes counts the number of index accesses in a plan
func (cp *CachingPlanner) countIndexes(plan Plan) int {
	if plan == nil {
		return 0
	}
	
	count := 0
	
	// Check if this is an index access node
	switch plan.(type) {
	case *IndexScan:
		count = 1
	case *CompositeIndexScan:
		count = 1
	case *IndexOnlyScan:
		count = 1
	case *BitmapIndexScan:
		count = 1
	}
	
	// Recursively count children
	for _, child := range plan.Children() {
		count += cp.countIndexes(child)
	}
	
	return count
}

// UpdateSchemaVersion updates the schema version and invalidates cache if needed
func (cp *CachingPlanner) UpdateSchemaVersion(newVersion int64) {
	if newVersion != cp.schemaVersion {
		cp.schemaVersion = newVersion
		cp.cache.Invalidate() // Invalidate all cached plans
	}
}

// UpdateStatsVersion updates the statistics version and invalidates cache if needed
func (cp *CachingPlanner) UpdateStatsVersion(newVersion int64) {
	if newVersion != cp.statsVersion {
		cp.statsVersion = newVersion
		cp.cache.Invalidate() // Invalidate all cached plans
	}
}

// InvalidateTable invalidates cached plans that access a specific table
func (cp *CachingPlanner) InvalidateTable(tableName string) {
	cp.cache.InvalidateTable(tableName)
}

// GetCacheStats returns current cache statistics
func (cp *CachingPlanner) GetCacheStats() *PlanCacheStats {
	stats := cp.cache.GetStats()
	return &stats
}

// SetCacheEnabled enables or disables plan caching
func (cp *CachingPlanner) SetCacheEnabled(enabled bool) {
	cp.enabled = enabled
	if !enabled {
		cp.cache.Clear() // Clear cache when disabling
	}
}

// UpdatePlanExecutionStats updates execution statistics for a cached plan
func (cp *CachingPlanner) UpdatePlanExecutionStats(stmt parser.Statement, paramTypes []types.DataType, execTime time.Duration) {
	if !cp.enabled {
		return
	}
	
	cacheKey := cp.generateCacheKey(stmt, paramTypes)
	cp.cache.UpdatePlanExecutionStats(cacheKey, execTime)
}

// GetCachedPlan retrieves a cached plan without updating LRU order (for inspection)
func (cp *CachingPlanner) GetCachedPlan(stmt parser.Statement, paramTypes []types.DataType) *CachedPlan {
	if !cp.enabled {
		return nil
	}
	
	cacheKey := cp.generateCacheKey(stmt, paramTypes)
	return cp.cache.Get(cacheKey)
}

// Statistics update helpers
func (cp *CachingPlanner) updateCacheHitStats(lookupTime time.Duration) {
	// Cache hit statistics are automatically updated by the cache itself
	// This method could be extended to track planner-specific metrics
}

func (cp *CachingPlanner) updateCacheMissStats(totalTime, planningTime time.Duration) {
	// Cache miss statistics are automatically updated by the cache itself
	// This method could be extended to track planner-specific metrics
}

// ClearCache clears all cached plans
func (cp *CachingPlanner) ClearCache() {
	cp.cache.Clear()
}

// GetCacheSize returns the current number of cached plans
func (cp *CachingPlanner) GetCacheSize() int {
	return cp.cache.Size()
}

// GetCatalog returns the catalog used by this planner
func (cp *CachingPlanner) GetCatalog() catalog.Catalog {
	return cp.catalog
}

// normalizeStatementSQL normalizes SQL for consistent caching
// This is a simple implementation - could be enhanced with proper SQL normalization
func normalizeStatementSQL(sql string) string {
	// Convert to lowercase and normalize whitespace
	normalized := strings.TrimSpace(strings.ToLower(sql))
	
	// Replace multiple spaces with single space
	parts := strings.Fields(normalized)
	return strings.Join(parts, " ")
}