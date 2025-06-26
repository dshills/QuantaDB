package executor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/txn"
)

// ConfigurableExecutor wraps the standard executor with configuration support
type ConfigurableExecutor struct {
	engine     engine.Engine
	catalog    catalog.Catalog
	txnManager *txn.Manager
	config     *ExecutorRuntimeConfig

	// Runtime statistics
	stats struct {
		mu                   sync.RWMutex
		vectorizedQueries    int64
		fallbackQueries      int64
		cacheHits            int64
		cacheMisses          int64
		totalMemoryUsed      int64
		peakMemoryUsed       int64
		queriesExecuted      int64
		averageExecutionTime time.Duration
	}

	// Result cache (if enabled)
	resultCache *ResultCache

	// Memory tracking
	memoryTracker *MemoryTracker

	// Feature flags (cached for performance)
	enableVectorized atomic.Bool
	enableCaching    atomic.Bool
}

// NewConfigurableExecutor creates a new executor with configuration support
func NewConfigurableExecutor(engine engine.Engine, catalog catalog.Catalog, txnManager *txn.Manager, cfg *ExecutorRuntimeConfig) *ConfigurableExecutor {
	exec := &ConfigurableExecutor{
		engine:        engine,
		catalog:       catalog,
		txnManager:    txnManager,
		config:        cfg,
		memoryTracker: NewMemoryTracker(cfg.QueryMemoryLimit),
	}

	// Initialize feature flags
	exec.enableVectorized.Store(cfg.IsVectorizedEnabled())
	exec.enableCaching.Store(cfg.IsCachingEnabled())

	// Initialize result cache if enabled
	if cfg.IsCachingEnabled() {
		cacheConfig := &ResultCacheConfig{
			MaxSize:     cfg.ResultCacheMaxEntries,
			MaxMemory:   cfg.ResultCacheMaxSize,
			TTL:         cfg.ResultCacheTTL,
			EnableStats: cfg.EnableStatistics,
		}
		exec.resultCache = NewResultCache(cacheConfig)
	}

	return exec
}

// Execute executes a plan with configuration-aware optimizations
func (ce *ConfigurableExecutor) Execute(ctx *ExecContext, plan planner.Plan, tx *txn.Transaction) (Result, error) {
	startTime := time.Now()
	defer func() {
		ce.updateStats(time.Since(startTime))
	}()

	// Check if caching is enabled and query is cacheable
	if ce.enableCaching.Load() && ce.resultCache != nil {
		if cached, ok := ce.checkCache(plan); ok {
			atomic.AddInt64(&ce.stats.cacheHits, 1)
			return cached, nil
		}
		atomic.AddInt64(&ce.stats.cacheMisses, 1)
	}

	// Create operator tree with configuration
	rootOp, err := ce.createOperator(plan, ctx)
	if err != nil {
		return nil, err
	}

	// Wrap with memory tracking
	rootOp = ce.wrapWithMemoryTracking(rootOp)

	// Execute the operator tree
	if err := rootOp.Open(ctx); err != nil {
		return nil, err
	}

	// Create result set
	result := &operatorResult{
		operator: rootOp,
		schema:   rootOp.Schema(),
	}

	// Cache result if applicable
	if ce.enableCaching.Load() && ce.resultCache != nil && ce.isCacheable(plan) {
		return ce.wrapWithCaching(result, plan), nil
	}

	atomic.AddInt64(&ce.stats.queriesExecuted, 1)
	return result, nil
}

// createOperator creates an operator tree with vectorized support if enabled
func (ce *ConfigurableExecutor) createOperator(plan planner.Plan, ctx *ExecContext) (Operator, error) {
	switch p := plan.(type) {
	case *planner.LogicalScan:
		if ce.shouldUseVectorized(p) {
			atomic.AddInt64(&ce.stats.vectorizedQueries, 1)
			return ce.createVectorizedScan(p, ctx)
		}
		return ce.createRegularScan(p, ctx)

	case *planner.LogicalFilter:
		if len(p.Children()) == 0 {
			return nil, fmt.Errorf("filter has no child")
		}
		child, err := ce.createOperator(p.Children()[0], ctx)
		if err != nil {
			return nil, err
		}

		if ce.shouldUseVectorized(p) && ce.supportsVectorizedExpression(p.Predicate) {
			return NewVectorizedFilterOperatorWithFallback(
				child.(VectorizedOperator),
				p.Predicate,
			), nil
		}

		// Build expression evaluator from planner expression
		evaluator, err := buildExprEvaluator(p.Predicate)
		if err != nil {
			return nil, fmt.Errorf("failed to build filter evaluator: %w", err)
		}
		return NewFilterOperator(child, evaluator), nil

	// Add other operator types...
	default:
		// Fallback to regular operator creation
		return nil, fmt.Errorf("unsupported plan type: %T", plan)
	}
}

// shouldUseVectorized determines if vectorized execution should be used
func (ce *ConfigurableExecutor) shouldUseVectorized(plan planner.Plan) bool {
	if !ce.enableVectorized.Load() {
		return false
	}

	// For now, enable vectorized execution for all supported plan types
	// In the future, this can use cost-based decisions
	switch plan.(type) {
	case *planner.LogicalScan, *planner.LogicalFilter:
		return true
	default:
		return false
	}
}

// supportsVectorizedExpression checks if an expression can be vectorized
func (ce *ConfigurableExecutor) supportsVectorizedExpression(expr planner.Expression) bool {
	_, supported := tryCreateVectorizedEvaluator(expr)
	return supported
}

// createVectorizedScan creates a vectorized scan operator
func (ce *ConfigurableExecutor) createVectorizedScan(scan *planner.LogicalScan, ctx *ExecContext) (VectorizedOperator, error) {
	table, err := ctx.Catalog.GetTable("public", scan.TableName)
	if err != nil {
		return nil, err
	}

	// Build schema from table
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name: col.Name,
			Type: col.DataType,
		}
	}

	// For now, use table ID as a simple identifier
	// In a real implementation, we'd need proper table access
	return NewVectorizedScanOperator(schema, table.ID), nil
}

// createRegularScan creates a regular scan operator
func (ce *ConfigurableExecutor) createRegularScan(scan *planner.LogicalScan, ctx *ExecContext) (Operator, error) {
	table, err := ctx.Catalog.GetTable("public", scan.TableName)
	if err != nil {
		return nil, err
	}

	return NewScanOperator(table, ctx), nil
}

// wrapWithMemoryTracking wraps an operator with memory tracking
func (ce *ConfigurableExecutor) wrapWithMemoryTracking(op Operator) Operator {
	return &memoryTrackingOperator{
		child:   op,
		tracker: ce.memoryTracker,
	}
}

// checkCache checks if a query result is in the cache
func (ce *ConfigurableExecutor) checkCache(plan planner.Plan) (Result, bool) {
	if ce.resultCache == nil {
		return nil, false
	}

	key := ce.generateCacheKey(plan)
	cached, ok := ce.resultCache.Get(key)
	if !ok {
		return nil, false
	}

	// Convert cached result to Result interface
	return &cachedResultWrapper{
		cached: cached,
		index:  0,
	}, true
}

// isCacheable determines if a query result can be cached
func (ce *ConfigurableExecutor) isCacheable(plan planner.Plan) bool {
	// Don't cache writes
	switch plan.(type) {
	case *planner.LogicalInsert, *planner.LogicalUpdate, *planner.LogicalDelete:
		return false
	}

	// Don't cache queries with non-deterministic functions
	if ce.hasNonDeterministicFunctions(plan) {
		return false
	}

	return true
}

// wrapWithCaching wraps a result set to cache its results
func (ce *ConfigurableExecutor) wrapWithCaching(result Result, plan planner.Plan) Result {
	key := ce.generateCacheKey(plan)
	tables := ce.extractTableDependencies(plan)

	return &cachingResultWrapper{
		Result: result,
		cache:  ce.resultCache,
		key:    key,
		tables: tables,
	}
}

// generateCacheKey generates a cache key for a query plan
func (ce *ConfigurableExecutor) generateCacheKey(plan planner.Plan) string {
	// Simple implementation - in practice would be more sophisticated
	return plan.String()
}

// extractTableDependencies extracts table names from a plan
func (ce *ConfigurableExecutor) extractTableDependencies(plan planner.Plan) []string {
	// Implementation would traverse plan tree and collect table names
	return []string{}
}

// hasNonDeterministicFunctions checks if plan contains non-deterministic functions
func (ce *ConfigurableExecutor) hasNonDeterministicFunctions(plan planner.Plan) bool {
	// Implementation would check for NOW(), RANDOM(), etc.
	return false
}

// updateStats updates execution statistics
func (ce *ConfigurableExecutor) updateStats(duration time.Duration) {
	ce.stats.mu.Lock()
	defer ce.stats.mu.Unlock()

	// Update average execution time
	totalQueries := ce.stats.queriesExecuted
	if totalQueries > 0 {
		avgNanos := int64(ce.stats.averageExecutionTime)
		newAvgNanos := (avgNanos*totalQueries + int64(duration)) / (totalQueries + 1)
		ce.stats.averageExecutionTime = time.Duration(newAvgNanos)
	} else {
		ce.stats.averageExecutionTime = duration
	}

	// Update memory stats
	currentMem := ce.memoryTracker.CurrentUsage()
	ce.stats.totalMemoryUsed = currentMem
	if currentMem > ce.stats.peakMemoryUsed {
		ce.stats.peakMemoryUsed = currentMem
	}
}

// GetStatistics returns current execution statistics
func (ce *ConfigurableExecutor) GetStatistics() ExecutorStatistics {
	ce.stats.mu.RLock()
	defer ce.stats.mu.RUnlock()

	return ExecutorStatistics{
		VectorizedQueries:    ce.stats.vectorizedQueries,
		FallbackQueries:      ce.stats.fallbackQueries,
		CacheHits:            ce.stats.cacheHits,
		CacheMisses:          ce.stats.cacheMisses,
		TotalMemoryUsed:      ce.stats.totalMemoryUsed,
		PeakMemoryUsed:       ce.stats.peakMemoryUsed,
		QueriesExecuted:      ce.stats.queriesExecuted,
		AverageExecutionTime: ce.stats.averageExecutionTime,
		CacheEnabled:         ce.enableCaching.Load(),
		VectorizedEnabled:    ce.enableVectorized.Load(),
	}
}

// UpdateConfig updates runtime configuration
func (ce *ConfigurableExecutor) UpdateConfig(cfg *ExecutorRuntimeConfig) {
	ce.config = cfg
	ce.enableVectorized.Store(cfg.IsVectorizedEnabled())
	ce.enableCaching.Store(cfg.IsCachingEnabled())

	// Update memory limits
	ce.memoryTracker.SetLimit(cfg.QueryMemoryLimit)

	// Recreate cache if settings changed
	if cfg.IsCachingEnabled() && ce.resultCache == nil {
		cacheConfig := &ResultCacheConfig{
			MaxSize:     cfg.ResultCacheMaxEntries,
			MaxMemory:   cfg.ResultCacheMaxSize,
			TTL:         cfg.ResultCacheTTL,
			EnableStats: cfg.EnableStatistics,
		}
		ce.resultCache = NewResultCache(cacheConfig)
	} else if !cfg.IsCachingEnabled() && ce.resultCache != nil {
		ce.resultCache.InvalidateAll()
		ce.resultCache = nil
	}
}

// ExecutorStatistics holds executor runtime statistics
type ExecutorStatistics struct {
	VectorizedQueries    int64
	FallbackQueries      int64
	CacheHits            int64
	CacheMisses          int64
	TotalMemoryUsed      int64
	PeakMemoryUsed       int64
	QueriesExecuted      int64
	AverageExecutionTime time.Duration
	CacheEnabled         bool
	VectorizedEnabled    bool
}

// cachedResultWrapper wraps a cached result as a Result interface
type cachedResultWrapper struct {
	cached *CachedResult
	index  int
}

func (cr *cachedResultWrapper) Next() (*Row, error) {
	if cr.index >= len(cr.cached.Rows) {
		return nil, nil
	}

	row := &Row{
		Values: cr.cached.Rows[cr.index].Values,
	}
	cr.index++
	return row, nil
}

func (cr *cachedResultWrapper) Close() error {
	return nil
}

func (cr *cachedResultWrapper) Schema() *Schema {
	return cr.cached.Schema
}

// cachingResultWrapper wraps a result to cache it as it's consumed
type cachingResultWrapper struct {
	Result
	cache  *ResultCache
	key    string
	tables []string
}
