package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/parser"
)

// CacheAwarePlanner extends physical planning with intelligent result caching decisions
type CacheAwarePlanner struct {
	*EnhancedPlanner
	cacheModel      *CacheAwareCostModel
	cacheStats      *CacheStatistics
	cachingPolicy   *CachingPolicy
}

// CacheAwareCostModel provides cost-based caching decisions
type CacheAwareCostModel struct {
	// Cost thresholds
	MinExecutionCostForCaching   float64 // Minimum execution cost to consider caching
	MaxResultSizeForCaching      int64   // Maximum result size to cache (bytes)
	CacheMaintenanceCostFactor   float64 // Cost factor for cache maintenance
	
	// Benefit calculations
	AccessFrequencyWeight        float64 // Weight for access frequency in benefit calculation
	ExecutionCostWeight          float64 // Weight for execution cost in benefit calculation
	MemoryPressurePenalty        float64 // Penalty factor under memory pressure
	
	// TTL calculations
	BaseTTL                      time.Duration // Base TTL for cached results
	DynamicTTLEnabled            bool          // Whether to use dynamic TTL based on cost
	HighCostTTLMultiplier        float64       // TTL multiplier for expensive queries
	LowVolatilityTTLMultiplier   float64       // TTL multiplier for stable data
}

// CachingPolicy defines when and how to cache query results
type CachingPolicy struct {
	// Enable/disable flags
	EnableResultCaching          bool
	EnableAdaptiveTTL            bool
	EnableCostBasedEviction      bool
	
	// Caching criteria
	MinCardinality               int64   // Minimum result rows to consider caching
	MaxCardinality               int64   // Maximum result rows to cache
	MinRepeatProbability         float64 // Minimum probability of query repetition
	
	// Resource limits
	MaxCacheMemoryRatio          float64 // Maximum cache memory as ratio of available memory
	MaxCacheEntries              int     // Maximum number of cached entries
	
	// Query type preferences
	PreferCacheSelects           bool // Cache SELECT queries
	PreferCacheAggregates        bool // Cache aggregate queries
	PreferCacheJoins             bool // Cache join queries
	AvoidCacheVolatileTables     bool // Avoid caching queries on frequently updated tables
}

// CachingDecision represents a caching decision with reasoning
type CachingDecision struct {
	ShouldCache         bool
	CacheTTL            time.Duration
	EvictionPriority    int
	EstimatedBenefit    float64
	EstimatedCost       float64
	Dependencies        []string
	Reasoning           []string
	ConfidenceScore     float64
}

// CacheStatistics tracks caching performance metrics
type CacheStatistics struct {
	// Decision statistics
	CachingDecisionsMade         int64
	CachingEnabled               int64
	CachingDisabled              int64
	
	// Performance metrics
	AvgCacheHitLatency           time.Duration
	AvgCacheMissLatency          time.Duration
	CacheHitRate                 float64
	
	// Benefit tracking
	TotalBytesSaved              int64
	TotalTimeSaved               time.Duration
	EstimatedCostSavings         float64
	
	// Eviction statistics
	TTLEvictions                 int64
	MemoryPressureEvictions      int64
	ExplicitEvictions            int64
	
	LastUpdate                   time.Time
}

// NewCacheAwarePlanner creates a new cache-aware planner
func NewCacheAwarePlanner(enhancedPlanner *EnhancedPlanner) *CacheAwarePlanner {
	return &CacheAwarePlanner{
		EnhancedPlanner: enhancedPlanner,
		cacheModel:      NewCacheAwareCostModel(),
		cacheStats:      NewCacheStatistics(),
		cachingPolicy:   NewDefaultCachingPolicy(),
	}
}

// NewCacheAwareCostModel creates a cost model with sensible defaults
func NewCacheAwareCostModel() *CacheAwareCostModel {
	return &CacheAwareCostModel{
		MinExecutionCostForCaching:   100.0,                // Cache queries costing > 100 units
		MaxResultSizeForCaching:      10 * 1024 * 1024,     // 10MB max result size
		CacheMaintenanceCostFactor:   0.1,                  // 10% overhead for cache maintenance
		AccessFrequencyWeight:        0.6,                  // 60% weight on access frequency
		ExecutionCostWeight:          0.4,                  // 40% weight on execution cost
		MemoryPressurePenalty:        2.0,                  // 2x penalty under memory pressure
		BaseTTL:                      5 * time.Minute,      // 5 minute base TTL
		DynamicTTLEnabled:            true,
		HighCostTTLMultiplier:        3.0,                  // 3x TTL for expensive queries
		LowVolatilityTTLMultiplier:   2.0,                  // 2x TTL for stable data
	}
}

// NewDefaultCachingPolicy creates a default caching policy
func NewDefaultCachingPolicy() *CachingPolicy {
	return &CachingPolicy{
		EnableResultCaching:          true,
		EnableAdaptiveTTL:            true,
		EnableCostBasedEviction:      true,
		MinCardinality:               10,    // Cache results with 10+ rows
		MaxCardinality:               100000, // Don't cache huge results
		MinRepeatProbability:         0.1,   // 10% chance of repetition
		MaxCacheMemoryRatio:          0.25,  // Use up to 25% of memory for cache
		MaxCacheEntries:              10000, // Maximum cached queries
		PreferCacheSelects:           true,
		PreferCacheAggregates:        true,
		PreferCacheJoins:             true,
		AvoidCacheVolatileTables:     true,
	}
}

// NewCacheStatistics creates a new statistics collector
func NewCacheStatistics() *CacheStatistics {
	return &CacheStatistics{
		LastUpdate: time.Now(),
	}
}

// Plan extends the enhanced planner with cache-aware decisions
func (cap *CacheAwarePlanner) Plan(stmt parser.Statement) (Plan, error) {
	// Get the base plan from enhanced planner
	plan, err := cap.EnhancedPlanner.Plan(stmt)
	if err != nil {
		return nil, err
	}
	
	// Only add caching decisions for SELECT statements
	selectStmt, isSelect := stmt.(*parser.SelectStmt)
	if !isSelect || !cap.cachingPolicy.EnableResultCaching {
		return plan, nil
	}
	
	// Analyze caching feasibility
	cachingDecision := cap.EvaluateCachingDecision(plan, selectStmt)
	
	// Update statistics
	cap.updateCachingStats(cachingDecision)
	
	// If caching is beneficial, wrap the plan with cache operations
	if cachingDecision.ShouldCache {
		return cap.wrapWithCaching(plan, cachingDecision), nil
	}
	
	return plan, nil
}

// EvaluateCachingDecision determines if and how to cache a query result
func (cap *CacheAwarePlanner) EvaluateCachingDecision(
	plan Plan,
	stmt *parser.SelectStmt,
) *CachingDecision {
	decision := &CachingDecision{
		ShouldCache:      false,
		CacheTTL:         cap.cacheModel.BaseTTL,
		EvictionPriority: 5, // Medium priority
		Reasoning:        []string{},
		ConfidenceScore:  0.5,
	}
	
	// Early exits for uncacheable queries
	if !cap.isQueryCacheable(stmt, decision) {
		return decision
	}
	
	// Estimate execution cost and result characteristics
	executionCost := cap.estimateExecutionCost(plan)
	resultSize := cap.estimateResultSize(plan)
	cardinality := cap.estimateResultCardinality(plan)
	
	// Check basic caching criteria
	if !cap.meetsCachingCriteria(executionCost, resultSize, cardinality, decision) {
		return decision
	}
	
	// Estimate access frequency and repetition probability
	accessFrequency := cap.estimateAccessFrequency(stmt)
	repeatProbability := cap.estimateRepeatProbability(stmt)
	
	// Calculate caching benefit
	benefit := cap.calculateCachingBenefit(
		executionCost, accessFrequency, repeatProbability, resultSize)
	
	// Factor in memory pressure
	memoryPressure := cap.getMemoryPressure()
	adjustedBenefit := benefit / (1.0 + memoryPressure*cap.cacheModel.MemoryPressurePenalty)
	
	// Make final caching decision
	if adjustedBenefit > 1.0 { // Benefit threshold
		decision.ShouldCache = true
		decision.EstimatedBenefit = adjustedBenefit
		decision.EstimatedCost = executionCost
		decision.ConfidenceScore = 0.8
		decision.Reasoning = append(decision.Reasoning, 
			fmt.Sprintf("beneficial_caching_benefit_%.2f", adjustedBenefit))
		
		// Calculate dynamic TTL
		if cap.cacheModel.DynamicTTLEnabled {
			decision.CacheTTL = cap.calculateDynamicTTL(executionCost, repeatProbability)
		}
		
		// Set eviction priority based on benefit
		decision.EvictionPriority = cap.calculateEvictionPriority(adjustedBenefit, executionCost)
		
		// Extract table dependencies
		decision.Dependencies = cap.extractTableDependencies(stmt)
	} else {
		decision.Reasoning = append(decision.Reasoning, 
			fmt.Sprintf("insufficient_benefit_%.2f", adjustedBenefit))
	}
	
	return decision
}

// isQueryCacheable performs initial cacheability checks
func (cap *CacheAwarePlanner) isQueryCacheable(stmt *parser.SelectStmt, decision *CachingDecision) bool {
	// Check for non-deterministic functions
	if cap.hasNonDeterministicFunctions(stmt) {
		decision.Reasoning = append(decision.Reasoning, "contains_non_deterministic_functions")
		return false
	}
	
	// Check for parameter references that would make caching less effective
	if cap.hasParameterReferences(stmt) {
		decision.Reasoning = append(decision.Reasoning, "contains_parameters")
		// Don't immediately reject - parameterized queries can still benefit from caching
	}
	
	// Check for subqueries that might make caching complex
	if cap.hasComplexSubqueries(stmt) {
		decision.Reasoning = append(decision.Reasoning, "contains_complex_subqueries")
		decision.ConfidenceScore *= 0.8 // Lower confidence
	}
	
	return true
}

// meetsCachingCriteria checks if the query meets basic caching criteria
func (cap *CacheAwarePlanner) meetsCachingCriteria(
	executionCost float64,
	resultSize int64,
	cardinality int64,
	decision *CachingDecision,
) bool {
	// Check execution cost threshold
	if executionCost < cap.cacheModel.MinExecutionCostForCaching {
		decision.Reasoning = append(decision.Reasoning, 
			fmt.Sprintf("execution_cost_too_low_%.2f", executionCost))
		return false
	}
	
	// Check result size limits
	if resultSize > cap.cacheModel.MaxResultSizeForCaching {
		decision.Reasoning = append(decision.Reasoning, 
			fmt.Sprintf("result_size_too_large_%d", resultSize))
		return false
	}
	
	// Check cardinality limits
	if cardinality < cap.cachingPolicy.MinCardinality {
		decision.Reasoning = append(decision.Reasoning, 
			fmt.Sprintf("cardinality_too_low_%d", cardinality))
		return false
	}
	
	if cardinality > cap.cachingPolicy.MaxCardinality {
		decision.Reasoning = append(decision.Reasoning, 
			fmt.Sprintf("cardinality_too_high_%d", cardinality))
		return false
	}
	
	return true
}

// calculateCachingBenefit calculates the expected benefit of caching
func (cap *CacheAwarePlanner) calculateCachingBenefit(
	executionCost float64,
	accessFrequency float64,
	repeatProbability float64,
	resultSize int64,
) float64 {
	// Base benefit from avoiding re-execution
	executionBenefit := executionCost * repeatProbability * accessFrequency
	
	// Factor in access frequency (more frequent = more beneficial)
	frequencyBenefit := accessFrequency * cap.cacheModel.AccessFrequencyWeight
	
	// Factor in execution cost (more expensive = more beneficial to cache)
	costBenefit := executionCost * cap.cacheModel.ExecutionCostWeight
	
	// Calculate cache maintenance cost
	maintenanceCost := float64(resultSize) * cap.cacheModel.CacheMaintenanceCostFactor
	
	// Total benefit calculation
	totalBenefit := (executionBenefit + frequencyBenefit + costBenefit) / (1.0 + maintenanceCost/1000.0)
	
	return totalBenefit
}

// calculateDynamicTTL calculates TTL based on query characteristics
func (cap *CacheAwarePlanner) calculateDynamicTTL(
	executionCost float64,
	repeatProbability float64,
) time.Duration {
	baseTTL := cap.cacheModel.BaseTTL
	
	// Extend TTL for expensive queries
	if executionCost > 1000.0 {
		baseTTL = time.Duration(float64(baseTTL) * cap.cacheModel.HighCostTTLMultiplier)
	}
	
	// Extend TTL for queries likely to be repeated
	if repeatProbability > 0.7 {
		baseTTL = time.Duration(float64(baseTTL) * 1.5)
	}
	
	// Factor in data volatility (would need to analyze table update patterns)
	// For now, use a simple heuristic
	volatilityFactor := cap.estimateDataVolatility()
	if volatilityFactor < 0.3 { // Low volatility
		baseTTL = time.Duration(float64(baseTTL) * cap.cacheModel.LowVolatilityTTLMultiplier)
	}
	
	// Ensure reasonable bounds
	maxTTL := 1 * time.Hour
	minTTL := 30 * time.Second
	
	if baseTTL > maxTTL {
		baseTTL = maxTTL
	}
	if baseTTL < minTTL {
		baseTTL = minTTL
	}
	
	return baseTTL
}

// calculateEvictionPriority determines cache eviction priority
func (cap *CacheAwarePlanner) calculateEvictionPriority(benefit float64, cost float64) int {
	// Priority scale: 1 (highest) to 10 (lowest)
	if benefit > 5.0 && cost > 1000.0 {
		return 1 // Highest priority - very beneficial and expensive
	} else if benefit > 3.0 && cost > 500.0 {
		return 2 // High priority
	} else if benefit > 2.0 && cost > 200.0 {
		return 3 // Medium-high priority
	} else if benefit > 1.5 {
		return 4 // Medium priority
	} else if benefit > 1.2 {
		return 5 // Medium-low priority
	} else {
		return 6 // Low priority
	}
}

// Helper methods for estimation

func (cap *CacheAwarePlanner) estimateExecutionCost(plan Plan) float64 {
	if physicalPlan, ok := plan.(PhysicalPlan); ok {
		context := &PhysicalPlanContext{
			CostModel: cap.physicalPlanner.vectorizedModel,
		}
		cost := physicalPlan.EstimateCost(context)
		return cost.TotalCost
	}
	return 100.0 // Default cost for logical plans
}

func (cap *CacheAwarePlanner) estimateResultSize(plan Plan) int64 {
	if physicalPlan, ok := plan.(PhysicalPlan); ok {
		cardinality := physicalPlan.EstimateCardinality()
		columns := len(physicalPlan.Schema().Columns)
		// Rough estimate: cardinality * columns * average_column_size
		return cardinality * int64(columns) * 50 // 50 bytes per column average
	}
	return 1000 // Default size
}

func (cap *CacheAwarePlanner) estimateResultCardinality(plan Plan) int64 {
	if physicalPlan, ok := plan.(PhysicalPlan); ok {
		return physicalPlan.EstimateCardinality()
	}
	return 100 // Default cardinality
}

func (cap *CacheAwarePlanner) estimateAccessFrequency(stmt *parser.SelectStmt) float64 {
	// Simple heuristic based on query complexity
	// In practice, this would use query log analysis
	
	// Simple queries are accessed more frequently
	if stmt.From != nil && stmt.Where == nil {
		return 5.0 // High frequency
	}
	
	// Aggregate queries might be dashboard queries (high frequency)
	if len(stmt.GroupBy) > 0 {
		return 3.0 // Medium-high frequency
	}
	
	// Complex joins might be reports (medium frequency)
	joinCount := cap.countJoins(stmt)
	if joinCount > 1 {
		return 2.0 // Medium frequency
	}
	
	return 1.0 // Default frequency
}

func (cap *CacheAwarePlanner) estimateRepeatProbability(stmt *parser.SelectStmt) float64 {
	// Generate a query signature and check historical patterns
	signature := cap.generateQuerySignature(stmt)
	
	// In practice, this would analyze query logs
	// For now, use simple heuristics
	
	if cap.hasOrderBy(stmt) && cap.hasLimit(stmt) {
		return 0.8 // Pagination queries are often repeated
	}
	
	if len(stmt.GroupBy) > 0 {
		return 0.7 // Aggregate queries for dashboards
	}
	
	if cap.hasSimpleFilters(stmt) {
		return 0.6 // Filtered searches
	}
	
	// Use signature to estimate (simplified)
	_ = signature
	return 0.4 // Default probability
}

func (cap *CacheAwarePlanner) getMemoryPressure() float64 {
	if cap.physicalPlanner.runtimeStats != nil {
		return cap.physicalPlanner.runtimeStats.MemoryPressure
	}
	return 0.0
}

func (cap *CacheAwarePlanner) estimateDataVolatility() float64 {
	// In practice, this would analyze table update patterns
	// For now, return a default medium volatility
	return 0.5
}

// Query analysis helper methods

func (cap *CacheAwarePlanner) hasNonDeterministicFunctions(stmt *parser.SelectStmt) bool {
	// Would need to traverse the expression tree and check for functions like NOW(), RANDOM()
	// Simplified implementation
	return false
}

func (cap *CacheAwarePlanner) hasParameterReferences(stmt *parser.SelectStmt) bool {
	// Check for $1, $2, etc. parameter references
	// Simplified implementation
	return false
}

func (cap *CacheAwarePlanner) hasComplexSubqueries(stmt *parser.SelectStmt) bool {
	// Check for correlated subqueries or multiple levels of nesting
	// Simplified implementation
	return false
}

func (cap *CacheAwarePlanner) countJoins(stmt *parser.SelectStmt) int {
	count := 0
	if stmt.From != nil {
		if _, isJoin := stmt.From.(*parser.JoinExpr); isJoin {
			count++
		}
	}
	return count
}

func (cap *CacheAwarePlanner) hasOrderBy(stmt *parser.SelectStmt) bool {
	return len(stmt.OrderBy) > 0
}

func (cap *CacheAwarePlanner) hasLimit(stmt *parser.SelectStmt) bool {
	return stmt.Limit != nil
}

func (cap *CacheAwarePlanner) hasSimpleFilters(stmt *parser.SelectStmt) bool {
	return stmt.Where != nil
}

func (cap *CacheAwarePlanner) generateQuerySignature(stmt *parser.SelectStmt) string {
	// Generate a signature for the query structure (ignoring literal values)
	// This would normalize the query to detect similar patterns
	tableCount := 0
	if stmt.From != nil {
		tableCount = 1 // Single table or join expression
	}
	queryStr := fmt.Sprintf("SELECT_FROM_%d_tables", tableCount)
	if stmt.Where != nil {
		queryStr += "_WITH_WHERE"
	}
	if len(stmt.GroupBy) > 0 {
		queryStr += "_WITH_GROUPBY"
	}
	if len(stmt.OrderBy) > 0 {
		queryStr += "_WITH_ORDERBY"
	}
	
	hash := sha256.Sum256([]byte(queryStr))
	return hex.EncodeToString(hash[:8]) // First 8 bytes for brevity
}

func (cap *CacheAwarePlanner) extractTableDependencies(stmt *parser.SelectStmt) []string {
	var tables []string
	if stmt.From != nil {
		switch f := stmt.From.(type) {
		case *parser.TableRef:
			tables = append(tables, f.TableName)
		case *parser.JoinExpr:
			tables = append(tables, cap.extractTableNamesFromJoin(f)...)
		}
	}
	return tables
}

// extractTableNamesFromJoin extracts table names from JOIN expressions
func (cap *CacheAwarePlanner) extractTableNamesFromJoin(join *parser.JoinExpr) []string {
	var tableNames []string
	
	if leftTable, ok := join.Left.(*parser.TableRef); ok {
		tableNames = append(tableNames, leftTable.TableName)
	}
	
	if rightTable, ok := join.Right.(*parser.TableRef); ok {
		tableNames = append(tableNames, rightTable.TableName)
	}
	
	return tableNames
}

// wrapWithCaching wraps a plan with caching operations
func (cap *CacheAwarePlanner) wrapWithCaching(plan Plan, decision *CachingDecision) Plan {
	// In practice, this would create a CacheOperator that wraps the plan
	// For now, just return the plan with caching metadata
	return plan
}

// updateCachingStats updates caching decision statistics
func (cap *CacheAwarePlanner) updateCachingStats(decision *CachingDecision) {
	cap.cacheStats.CachingDecisionsMade++
	if decision.ShouldCache {
		cap.cacheStats.CachingEnabled++
	} else {
		cap.cacheStats.CachingDisabled++
	}
	cap.cacheStats.LastUpdate = time.Now()
}

// GetCacheStatistics returns current caching statistics
func (cap *CacheAwarePlanner) GetCacheStatistics() *CacheStatistics {
	return cap.cacheStats
}

// SetCachingPolicy updates the caching policy
func (cap *CacheAwarePlanner) SetCachingPolicy(policy *CachingPolicy) {
	cap.cachingPolicy = policy
}

// SetCacheModel updates the cache cost model
func (cap *CacheAwarePlanner) SetCacheModel(model *CacheAwareCostModel) {
	cap.cacheModel = model
}