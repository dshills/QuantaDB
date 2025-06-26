package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// PhysicalPlanner converts logical plans to optimized physical plans
type PhysicalPlanner struct {
	costEstimator      *CostEstimator
	vectorizedModel    *VectorizedCostModel
	catalog           catalog.Catalog
	runtimeStats      *RuntimeStatistics
	config            *PhysicalPlannerConfig
}

// PhysicalPlannerConfig controls physical planning behavior
type PhysicalPlannerConfig struct {
	EnableVectorization      bool
	VectorizationThreshold   int64  // Minimum rows to consider vectorization
	MemoryPressureThreshold  float64 // 0.0-1.0, when to avoid vectorization
	CostDifferenceThreshold  float64 // Minimum cost difference to switch modes
	DefaultBatchSize         int
	EnableAdaptivePlanning   bool
	MaxPlanningTime         int64 // Milliseconds
}

// DefaultPhysicalPlannerConfig returns default configuration
func DefaultPhysicalPlannerConfig() *PhysicalPlannerConfig {
	return &PhysicalPlannerConfig{
		EnableVectorization:      true,
		VectorizationThreshold:   1000,
		MemoryPressureThreshold:  0.8,
		CostDifferenceThreshold:  1.2, // 20% improvement required
		DefaultBatchSize:         1024,
		EnableAdaptivePlanning:   true,
		MaxPlanningTime:         100, // 100ms
	}
}

// NewPhysicalPlanner creates a new physical planner
func NewPhysicalPlanner(
	costEstimator *CostEstimator,
	vectorizedModel *VectorizedCostModel,
	catalog catalog.Catalog,
) *PhysicalPlanner {
	return &PhysicalPlanner{
		costEstimator:   costEstimator,
		vectorizedModel: vectorizedModel,
		catalog:        catalog,
		runtimeStats:   &RuntimeStatistics{OperatorStats: make(map[string]*OperatorStats)},
		config:         DefaultPhysicalPlannerConfig(),
	}
}

// SetConfig updates the planner configuration
func (pp *PhysicalPlanner) SetConfig(config *PhysicalPlannerConfig) {
	pp.config = config
}

// SetRuntimeStats updates the runtime statistics
func (pp *PhysicalPlanner) SetRuntimeStats(stats *RuntimeStatistics) {
	pp.runtimeStats = stats
}

// GeneratePhysicalPlan converts a logical plan to an optimized physical plan
func (pp *PhysicalPlanner) GeneratePhysicalPlan(
	logical LogicalPlan,
	context *PhysicalPlanContext,
) (PhysicalPlan, error) {
	if context == nil {
		context = pp.createDefaultContext()
	}
	
	// Analyze the logical plan for optimization opportunities
	analysis := pp.analyzeLogicalPlan(logical)
	
	// Generate physical plan with cost-based decisions
	physical, err := pp.convertToPhysical(logical, context, analysis)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to physical plan: %w", err)
	}
	
	// Post-process for final optimizations
	optimized := pp.postOptimize(physical, context)
	
	return optimized, nil
}

// PlanAnalysis contains analysis results for optimization decisions
type PlanAnalysis struct {
	EstimatedCardinality map[int]int64           // Plan ID -> estimated rows
	ExpressionComplexity map[Expression]int     // Expression -> complexity score
	MemoryRequirements   map[int]int64          // Plan ID -> memory requirement
	VectorizationCandidates []int               // Plan IDs that could benefit from vectorization
	CachingCandidates    []int                  // Plan IDs that could benefit from caching
}

// analyzeLogicalPlan performs analysis to inform physical planning decisions
func (pp *PhysicalPlanner) analyzeLogicalPlan(logical LogicalPlan) *PlanAnalysis {
	analysis := &PlanAnalysis{
		EstimatedCardinality:    make(map[int]int64),
		ExpressionComplexity:    make(map[Expression]int),
		MemoryRequirements:      make(map[int]int64),
		VectorizationCandidates: []int{},
		CachingCandidates:       []int{},
	}
	
	// Traverse the plan tree and collect analysis data
	pp.analyzeNode(logical, analysis)
	
	return analysis
}

// analyzeNode recursively analyzes a plan node
func (pp *PhysicalPlanner) analyzeNode(node LogicalPlan, analysis *PlanAnalysis) {
	// Analyze node characteristics
	
	// Estimate cardinality
	cardinality := pp.estimateLogicalCardinality(node)
	// Note: In a full implementation, we'd use a proper node ID system
	
	// Estimate memory requirements
	_ = pp.estimateMemoryRequirement(node, cardinality)
	// Note: In a full implementation, we'd track memory per node
	
	// Check if node is a vectorization candidate
	if pp.isVectorizationCandidate(node, cardinality) {
		// Note: In a full implementation, we'd track vectorization candidates
	}
	
	// Check if node is a caching candidate
	if pp.isCachingCandidate(node, cardinality) {
		// Note: In a full implementation, we'd track caching candidates
	}
	
	// Analyze expressions in the node
	pp.analyzeNodeExpressions(node, analysis)
	
	// Recursively analyze children
	for _, child := range node.Children() {
		if logicalChild, ok := child.(LogicalPlan); ok {
			pp.analyzeNode(logicalChild, analysis)
		}
	}
}

// convertToPhysical converts a logical plan to physical plan
func (pp *PhysicalPlanner) convertToPhysical(
	logical LogicalPlan,
	context *PhysicalPlanContext,
	analysis *PlanAnalysis,
) (PhysicalPlan, error) {
	switch node := logical.(type) {
	case *LogicalScan:
		return pp.createPhysicalScan(node, context, analysis)
		
	case *LogicalFilter:
		return pp.createPhysicalFilter(node, context, analysis)
		
	case *LogicalJoin:
		return pp.createPhysicalJoin(node, context, analysis)
		
	case *LogicalProject:
		return pp.createPhysicalProjection(node, context, analysis)
		
	case *LogicalAggregate:
		return pp.createPhysicalAggregate(node, context, analysis)
		
	case *LogicalSort:
		return pp.createPhysicalSort(node, context, analysis)
		
	case *LogicalLimit:
		return pp.createPhysicalLimit(node, context, analysis)
		
	default:
		return nil, fmt.Errorf("unsupported logical plan type: %T", logical)
	}
}

// createPhysicalScan creates an optimized physical scan
func (pp *PhysicalPlanner) createPhysicalScan(
	logical *LogicalScan,
	context *PhysicalPlanContext,
	analysis *PlanAnalysis,
) (PhysicalPlan, error) {
	// Get table information
	table, err := pp.catalog.GetTable("public", logical.TableName)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s", logical.TableName)
	}
	
	// Create base physical scan
	physicalScan := NewPhysicalScan(table, nil) // No predicate in scan
	
	// Determine execution mode
	cardinality := table.Stats.RowCount // Use table stats directly
	executionMode := pp.chooseExecutionMode(OperatorTypeScan, cardinality, nil, context)
	physicalScan.ExecutionMode = executionMode
	
	// Configure batch size for vectorized execution
	if executionMode == ExecutionModeVectorized || executionMode == ExecutionModeHybrid {
		physicalScan.BatchSize = pp.chooseBatchSize(cardinality, context)
	}
	
	// Consider index access
	// Note: LogicalScan doesn't have predicate - index selection would be based on query context
	// indexPath := pp.chooseIndexAccess(table, nil, context)
	// physicalScan.IndexAccess = indexPath
	
	return physicalScan, nil
}

// createPhysicalFilter creates an optimized physical filter
func (pp *PhysicalPlanner) createPhysicalFilter(
	logical *LogicalFilter,
	context *PhysicalPlanContext,
	analysis *PlanAnalysis,
) (PhysicalPlan, error) {
	// Convert input first
	if len(logical.Children()) == 0 {
		return nil, fmt.Errorf("filter has no input")
	}
	
	input, err := pp.convertToPhysical(logical.Children()[0].(LogicalPlan), context, analysis)
	if err != nil {
		return nil, err
	}
	
	// Create physical filter
	physicalFilter := NewPhysicalFilter(input, logical.Predicate)
	
	// Determine execution mode
	cardinality := input.EstimateCardinality() // Use input cardinality
	executionMode := pp.chooseExecutionMode(OperatorTypeFilter, cardinality, logical.Predicate, context)
	physicalFilter.ExecutionMode = executionMode
	
	// Configure batch size
	if executionMode == ExecutionModeVectorized || executionMode == ExecutionModeHybrid {
		physicalFilter.BatchSize = pp.chooseBatchSize(cardinality, context)
	}
	
	return physicalFilter, nil
}

// createPhysicalJoin creates an optimized physical join
func (pp *PhysicalPlanner) createPhysicalJoin(
	logical *LogicalJoin,
	context *PhysicalPlanContext,
	analysis *PlanAnalysis,
) (PhysicalPlan, error) {
	children := logical.Children()
	if len(children) != 2 {
		return nil, fmt.Errorf("join must have exactly 2 children")
	}
	
	// Convert children
	left, err := pp.convertToPhysical(children[0].(LogicalPlan), context, analysis)
	if err != nil {
		return nil, err
	}
	
	right, err := pp.convertToPhysical(children[1].(LogicalPlan), context, analysis)
	if err != nil {
		return nil, err
	}
	
	// Create physical join
	physicalJoin := NewPhysicalJoin(left, right, logical.JoinType, logical.Condition)
	
	// Choose join algorithm
	leftCardinality := left.EstimateCardinality()
	rightCardinality := right.EstimateCardinality()
	physicalJoin.Algorithm = pp.chooseJoinAlgorithm(leftCardinality, rightCardinality, logical.Condition, context)
	
	// Determine execution mode
	totalCardinality := leftCardinality + rightCardinality
	executionMode := pp.chooseExecutionMode(OperatorTypeJoin, totalCardinality, logical.Condition, context)
	physicalJoin.ExecutionMode = executionMode
	
	return physicalJoin, nil
}

// Placeholder implementations for other operators
func (pp *PhysicalPlanner) createPhysicalProjection(logical *LogicalProject, context *PhysicalPlanContext, analysis *PlanAnalysis) (PhysicalPlan, error) {
	// Generate physical plan for input
	if len(logical.Children()) == 0 {
		return nil, fmt.Errorf("projection has no input")
	}
	input, err := pp.GeneratePhysicalPlan(logical.Children()[0].(LogicalPlan), context)
	if err != nil {
		return nil, fmt.Errorf("failed to generate input plan: %w", err)
	}
	
	// Create physical projection
	projection := NewPhysicalProject(input, logical.Projections)
	
	// Determine execution mode based on vectorization analysis
	cardinality := input.EstimateCardinality()
	executionMode := pp.chooseExecutionMode(OperatorTypeProjection, cardinality, nil, context)
	projection.ExecutionMode = executionMode
	
	return projection, nil
}

func (pp *PhysicalPlanner) createPhysicalAggregate(logical *LogicalAggregate, context *PhysicalPlanContext, analysis *PlanAnalysis) (PhysicalPlan, error) {
	// Generate physical plan for input
	if len(logical.Children()) == 0 {
		return nil, fmt.Errorf("aggregate has no input")
	}
	input, err := pp.GeneratePhysicalPlan(logical.Children()[0].(LogicalPlan), context)
	if err != nil {
		return nil, fmt.Errorf("failed to generate input plan: %w", err)
	}
	
	// Create physical aggregate
	aggregate := NewPhysicalAggregate(input, logical.GroupBy, logical.Aggregates)
	
	// Determine execution mode and algorithm
	cardinality := input.EstimateCardinality()
	executionMode := pp.chooseExecutionMode(OperatorTypeAggregate, cardinality, nil, context)
	aggregate.ExecutionMode = executionMode
	
	// Choose algorithm based on cardinality and group by size
	if len(logical.GroupBy) == 0 {
		aggregate.Algorithm = AggregateAlgorithmStream
	} else if cardinality > 10000 {
		aggregate.Algorithm = AggregateAlgorithmHash
	} else {
		aggregate.Algorithm = AggregateAlgorithmSort
	}
	
	return aggregate, nil
}

func (pp *PhysicalPlanner) createPhysicalSort(logical *LogicalSort, context *PhysicalPlanContext, analysis *PlanAnalysis) (PhysicalPlan, error) {
	// Generate physical plan for input
	if len(logical.Children()) == 0 {
		return nil, fmt.Errorf("sort has no input")
	}
	input, err := pp.GeneratePhysicalPlan(logical.Children()[0].(LogicalPlan), context)
	if err != nil {
		return nil, fmt.Errorf("failed to generate input plan: %w", err)
	}
	
	// Create physical sort - convert OrderBy to SortKeys
	sortKeys := make([]SortKey, len(logical.OrderBy))
	for i, orderBy := range logical.OrderBy {
		// Convert expression to column name (simplified)
		columnName := "col_" + fmt.Sprintf("%d", i)
		if col, ok := orderBy.Expr.(*ColumnRef); ok {
			columnName = col.ColumnName
		}
		
		// Convert SortOrder to SortDirection
		direction := SortDirectionAsc
		if orderBy.Order == Descending {
			direction = SortDirectionDesc
		}
		
		sortKeys[i] = SortKey{
			Column:    columnName,
			Direction: direction,
		}
	}
	sort := NewPhysicalSort(input, sortKeys)
	
	// Determine execution mode
	cardinality := input.EstimateCardinality()
	executionMode := pp.chooseExecutionMode(OperatorTypeSort, cardinality, nil, context)
	sort.ExecutionMode = executionMode
	
	// Choose algorithm based on cardinality
	if cardinality > 100000 {
		sort.Algorithm = SortAlgorithmMergeSort // More stable for large datasets
	} else {
		sort.Algorithm = SortAlgorithmQuickSort
	}
	
	return sort, nil
}

func (pp *PhysicalPlanner) createPhysicalLimit(logical *LogicalLimit, context *PhysicalPlanContext, analysis *PlanAnalysis) (PhysicalPlan, error) {
	// Generate physical plan for input
	if len(logical.Children()) == 0 {
		return nil, fmt.Errorf("limit has no input")
	}
	input, err := pp.GeneratePhysicalPlan(logical.Children()[0].(LogicalPlan), context)
	if err != nil {
		return nil, fmt.Errorf("failed to generate input plan: %w", err)
	}
	
	// Create physical limit
	limit := NewPhysicalLimit(input, logical.Limit, logical.Offset)
	
	// Limit operations can always be vectorized efficiently
	if pp.config.EnableVectorization && context.VectorizationMode != VectorizationModeDisabled {
		limit.ExecutionMode = ExecutionModeVectorized
	} else {
		limit.ExecutionMode = ExecutionModeScalar
	}
	
	return limit, nil
}

// chooseExecutionMode determines the optimal execution mode for an operator
func (pp *PhysicalPlanner) chooseExecutionMode(
	opType OperatorType,
	cardinality int64,
	expression Expression,
	context *PhysicalPlanContext,
) ExecutionMode {
	// Check if vectorization is disabled
	if !pp.config.EnableVectorization || context.VectorizationMode == VectorizationModeDisabled {
		return ExecutionModeScalar
	}
	
	// Force vectorization if requested
	if context.VectorizationMode == VectorizationModeForced {
		return ExecutionModeVectorized
	}
	
	// For enabled mode, be more aggressive about vectorization
	if context.VectorizationMode == VectorizationModeEnabled {
		// Only check basic constraints for enabled mode
		if cardinality >= pp.config.VectorizationThreshold && 
		   pp.runtimeStats.MemoryPressure <= pp.config.MemoryPressureThreshold {
			return ExecutionModeVectorized
		}
		// Still fall through to other checks if basic constraints not met
	}
	
	// Check cardinality threshold for adaptive mode
	if cardinality < pp.config.VectorizationThreshold {
		return ExecutionModeScalar
	}
	
	// Check memory pressure for adaptive mode
	if pp.runtimeStats.MemoryPressure > pp.config.MemoryPressureThreshold {
		return ExecutionModeScalar
	}
	
	// Expression complexity analysis
	if expression != nil {
		complexity := pp.analyzeExpressionComplexity(expression)
		if complexity > 5 { // High complexity threshold
			return ExecutionModeScalar
		}
	}
	
	// Use cost model for final decision
	if pp.config.EnableAdaptivePlanning && pp.vectorizedModel != nil {
		choice := pp.costEstimator.GetVectorizedExecutionChoice(
			opType.String(),
			cardinality,
			0.1, // Default selectivity
			expression,
			context.MemoryAvailable,
		)
		
		if choice.UseVectorized {
			return ExecutionModeVectorized
		}
	}
	
	return ExecutionModeScalar
}

// chooseJoinAlgorithm selects the optimal join algorithm
func (pp *PhysicalPlanner) chooseJoinAlgorithm(
	leftCardinality, rightCardinality int64,
	condition Expression,
	context *PhysicalPlanContext,
) JoinAlgorithm {
	// Simple heuristics for join algorithm selection
	ratio := float64(leftCardinality) / float64(rightCardinality)
	
	// If one side is much smaller, use hash join with smaller side as build
	if ratio > 10.0 || ratio < 0.1 {
		return JoinAlgorithmHash
	}
	
	// For very large joins, consider memory constraints
	estimatedMemory := rightCardinality * 100 // Rough hash table estimate
	if estimatedMemory > context.MemoryAvailable/2 {
		return JoinAlgorithmNestedLoop
	}
	
	// Default to hash join
	return JoinAlgorithmHash
}

// Helper methods for analysis
func (pp *PhysicalPlanner) estimateLogicalCardinality(node LogicalPlan) int64 {
	// Simplified cardinality estimation
	switch n := node.(type) {
	case *LogicalScan:
		if table, err := pp.catalog.GetTable("public", n.TableName); err == nil {
			// LogicalScan doesn't have predicate
			return table.Stats.RowCount
		}
		return 1000 // Default
	case *LogicalFilter:
		if len(n.Children()) > 0 {
			inputCard := pp.estimateLogicalCardinality(n.Children()[0].(LogicalPlan))
			return int64(float64(inputCard) * 0.1) // 10% selectivity
		}
		return 100
	case *LogicalJoin:
		if len(n.Children()) == 2 {
			left := pp.estimateLogicalCardinality(n.Children()[0].(LogicalPlan))
			right := pp.estimateLogicalCardinality(n.Children()[1].(LogicalPlan))
			return int64(float64(left*right) * 0.1) // 10% join selectivity
		}
		return 1000
	default:
		return 1000
	}
}

func (pp *PhysicalPlanner) estimateMemoryRequirement(node LogicalPlan, cardinality int64) int64 {
	// Rough memory estimation based on operator type
	switch node.(type) {
	case *LogicalScan:
		return cardinality * 100 // Bytes per row estimate
	case *LogicalJoin:
		return cardinality * 200 // Higher memory for hash tables
	default:
		return cardinality * 50
	}
}

func (pp *PhysicalPlanner) isVectorizationCandidate(node LogicalPlan, cardinality int64) bool {
	return cardinality >= pp.config.VectorizationThreshold
}

func (pp *PhysicalPlanner) isCachingCandidate(node LogicalPlan, cardinality int64) bool {
	// Cache smaller result sets that are expensive to compute
	return cardinality < 10000 && cardinality > 100
}

func (pp *PhysicalPlanner) analyzeNodeExpressions(node LogicalPlan, analysis *PlanAnalysis) {
	// Extract and analyze expressions from the node
	switch n := node.(type) {
	case *LogicalFilter:
		if n.Predicate != nil {
			analysis.ExpressionComplexity[n.Predicate] = pp.analyzeExpressionComplexity(n.Predicate)
		}
	case *LogicalJoin:
		if n.Condition != nil {
			analysis.ExpressionComplexity[n.Condition] = pp.analyzeExpressionComplexity(n.Condition)
		}
	}
}

func (pp *PhysicalPlanner) analyzeExpressionComplexity(expr Expression) int {
	// Simple complexity scoring
	if expr == nil {
		return 0
	}
	
	switch e := expr.(type) {
	case *BinaryOp:
		leftComplexity := pp.analyzeExpressionComplexity(e.Left)
		rightComplexity := pp.analyzeExpressionComplexity(e.Right)
		return 1 + leftComplexity + rightComplexity
	case *FunctionCall:
		complexity := 3 // Base function cost
		for _, arg := range e.Args {
			complexity += pp.analyzeExpressionComplexity(arg)
		}
		return complexity
	case *SubqueryExpr:
		return 10 // High complexity for subqueries
	default:
		return 1 // Base case for literals, column refs
	}
}

func (pp *PhysicalPlanner) chooseBatchSize(cardinality int64, context *PhysicalPlanContext) int {
	batchSize := context.BatchSize
	if batchSize <= 0 {
		batchSize = pp.config.DefaultBatchSize
	}
	
	// Adjust batch size based on cardinality and memory
	if cardinality < int64(batchSize) {
		return int(cardinality)
	}
	
	return batchSize
}

func (pp *PhysicalPlanner) chooseIndexAccess(
	table *catalog.Table,
	predicate Expression,
	context *PhysicalPlanContext,
) *IndexAccessPath {
	// Simplified index selection - choose first applicable index
	for _, index := range table.Indexes {
		if pp.canUseIndex(index, predicate) {
			return &IndexAccessPath{
				Index:       index,
				KeyColumns:  []string{index.Columns[0].Column.Name},
				Conditions:  []Expression{predicate},
				Selectivity: 0.1, // Default selectivity
			}
		}
	}
	return nil
}

func (pp *PhysicalPlanner) canUseIndex(index *catalog.Index, predicate Expression) bool {
	// Simplified index applicability check
	// In practice, this would be much more sophisticated
	return predicate != nil && len(index.Columns) > 0
}

func (pp *PhysicalPlanner) createDefaultContext() *PhysicalPlanContext {
	return &PhysicalPlanContext{
		CostModel:         pp.vectorizedModel,
		MemoryAvailable:   1024 * 1024 * 1024, // 1GB default
		VectorizationMode: VectorizationModeAdaptive,
		BatchSize:         pp.config.DefaultBatchSize,
		EnableCaching:     true,
		TableStats:        make(map[string]*catalog.TableStats),
		IndexStats:        make(map[string]*catalog.IndexStats),
	}
}

func (pp *PhysicalPlanner) postOptimize(plan PhysicalPlan, context *PhysicalPlanContext) PhysicalPlan {
	// Post-processing optimizations
	// For now, just return the plan as-is
	// Future enhancements could include:
	// - Operator fusion
	// - Memory-aware optimizations
	// - Cache placement optimization
	return plan
}