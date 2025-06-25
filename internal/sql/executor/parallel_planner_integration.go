package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/planner"
)

// ParallelPlannerConfig contains configuration for parallel query execution
type ParallelPlannerConfig struct {
	// Enable parallel execution (default: false for backwards compatibility)
	EnableParallelExecution bool
	// Maximum degree of parallelism (default: number of CPU cores)
	MaxDOP int
	// Minimum table size (rows) to consider for parallelization
	MinTableSizeForParallelism int64
	// Minimum estimated cost to consider for parallelization
	MinCostForParallelism float64
	// Force parallel execution (override cost estimation)
	ForceParallel bool
}

// DefaultParallelPlannerConfig returns default configuration
func DefaultParallelPlannerConfig() *ParallelPlannerConfig {
	return &ParallelPlannerConfig{
		EnableParallelExecution:    false, // Conservative default
		MaxDOP:                     4,     // Default to 4-way parallelism
		MinTableSizeForParallelism: 1000,  // Tables with 1000+ rows
		MinCostForParallelism:      100.0, // Operations with cost >= 100
		ForceParallel:              false,
	}
}

// ParallelExecutorBuilder creates parallel execution plans
type ParallelExecutorBuilder struct {
	config *ParallelPlannerConfig
}

// NewParallelExecutorBuilder creates a new parallel executor builder
func NewParallelExecutorBuilder(config *ParallelPlannerConfig) *ParallelExecutorBuilder {
	if config == nil {
		config = DefaultParallelPlannerConfig()
	}
	return &ParallelExecutorBuilder{
		config: config,
	}
}

// BuildParallelPlan attempts to create a parallel execution plan
func (pb *ParallelExecutorBuilder) BuildParallelPlan(plan planner.Plan, ctx *ExecContext) (Operator, error) {
	if !pb.config.EnableParallelExecution {
		// Parallel execution disabled, build sequential plan
		return pb.buildSequentialPlan(plan, ctx)
	}

	// Convert logical plan to physical operators
	physicalOp, err := pb.buildSequentialPlan(plan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build base plan: %w", err)
	}

	// Attempt to parallelize the plan
	if pb.shouldParallelize(plan, physicalOp) {
		parallelOp, err := pb.parallelizePlan(physicalOp, ctx)
		if err != nil {
			// Fall back to sequential execution on error
			return physicalOp, nil
		}
		return parallelOp, nil
	}

	return physicalOp, nil
}

// buildSequentialPlan builds a standard sequential execution plan
func (pb *ParallelExecutorBuilder) buildSequentialPlan(plan planner.Plan, ctx *ExecContext) (Operator, error) {
	// Use existing plan execution logic
	// This would typically be implemented by the main executor
	// For now, we'll return an error indicating this needs to be integrated
	return nil, fmt.Errorf("sequential plan building not implemented - needs integration with main executor")
}

// shouldParallelize determines if a plan should be parallelized
func (pb *ParallelExecutorBuilder) shouldParallelize(plan planner.Plan, op Operator) bool {
	if pb.config.ForceParallel {
		return true
	}

	// Check if the operator supports parallelization
	if parallelizable, ok := op.(ParallelizableOperator); !ok || !parallelizable.CanParallelize() {
		return false
	}

	// Check cost estimation (would need actual cost from planner)
	// For now, we'll use simple heuristics

	// Check for scan operations on large tables
	if pb.isScanOperation(op) {
		return true // Scan operations often benefit from parallelization
	}

	// Check for expensive join operations
	if pb.isJoinOperation(op) {
		return true // Hash joins can benefit from parallelization
	}

	return false
}

// isScanOperation checks if the operator is a table scan
func (pb *ParallelExecutorBuilder) isScanOperation(op Operator) bool {
	switch op.(type) {
	case *StorageScanOperator:
		return true
	default:
		return false
	}
}

// isJoinOperation checks if the operator is a join
func (pb *ParallelExecutorBuilder) isJoinOperation(op Operator) bool {
	switch op.(type) {
	case *HashJoinOperator:
		return true
	case *NestedLoopJoinOperator:
		return false // Nested loop joins typically don't benefit from parallelization
	default:
		return false
	}
}

// parallelizePlan converts a sequential plan to a parallel plan
func (pb *ParallelExecutorBuilder) parallelizePlan(op Operator, ctx *ExecContext) (Operator, error) {
	// Create parallel context
	parallelCtx := NewParallelContext(ctx, pb.config.MaxDOP)

	// Recursively parallelize the plan tree
	parallelOp, err := pb.parallelizeOperator(op, parallelCtx)
	if err != nil {
		parallelCtx.Close()
		return nil, err
	}

	// Wrap in parallel execution plan
	return NewParallelExecutionPlan(parallelOp, pb.config.MaxDOP), nil
}

// parallelizeOperator converts a single operator to its parallel equivalent
func (pb *ParallelExecutorBuilder) parallelizeOperator(op Operator, parallelCtx *ParallelContext) (Operator, error) {
	// Check if operator can be parallelized
	if parallelizable, ok := op.(ParallelizableOperator); ok && parallelizable.CanParallelize() {
		return parallelizable.CreateParallelInstance(parallelCtx)
	}

	// For operators that can't be parallelized, parallelize their children
	return pb.parallelizeChildren(op, parallelCtx)
}

// parallelizeChildren parallelizes child operators while keeping parent sequential
func (pb *ParallelExecutorBuilder) parallelizeChildren(op Operator, parallelCtx *ParallelContext) (Operator, error) {
	// This is a simplified implementation
	// In practice, you'd need to handle each operator type specifically

	switch o := op.(type) {
	case *FilterOperator:
		// Parallelize child, keep filter sequential
		parallelChild, err := pb.parallelizeOperator(o.child, parallelCtx)
		if err != nil {
			return nil, err
		}
		return NewFilterOperator(parallelChild, o.predicate), nil

	case *ProjectOperator:
		// Parallelize child, keep projection sequential
		parallelChild, err := pb.parallelizeOperator(o.child, parallelCtx)
		if err != nil {
			return nil, err
		}
		return NewProjectOperator(parallelChild, o.projections, o.schema), nil

	case *LimitOperator:
		// Parallelize child, keep limit sequential
		parallelChild, err := pb.parallelizeOperator(o.child, parallelCtx)
		if err != nil {
			return nil, err
		}
		// Add exchange operator to collect parallel results
		exchange := NewExchangeOperator(parallelChild, pb.config.MaxDOP*2)
		return NewLimitOperator(exchange, o.limit, o.offset), nil

	default:
		// For unknown operators, return as-is
		return op, nil
	}
}

// ParallelQueryHint represents hints for parallel execution
type ParallelQueryHint struct {
	// Force parallel execution for this query
	ForceParallel bool
	// Override default degree of parallelism
	DegreeOfParallelism int
	// Disable parallel execution for this query
	NoParallel bool
}

// ApplyParallelHints applies parallel execution hints to the config
func (pb *ParallelExecutorBuilder) ApplyParallelHints(hints *ParallelQueryHint) *ParallelExecutorBuilder {
	if hints == nil {
		return pb
	}

	// Create a copy of the config with hints applied
	newConfig := *pb.config

	if hints.NoParallel {
		newConfig.EnableParallelExecution = false
	}

	if hints.ForceParallel {
		newConfig.ForceParallel = true
		newConfig.EnableParallelExecution = true
	}

	if hints.DegreeOfParallelism > 0 {
		newConfig.MaxDOP = hints.DegreeOfParallelism
	}

	return &ParallelExecutorBuilder{
		config: &newConfig,
	}
}

// GetParallelismMetrics returns metrics about parallel execution decisions
type ParallelismMetrics struct {
	// Total queries processed
	TotalQueries int64
	// Queries executed in parallel
	ParallelQueries int64
	// Queries that could have been parallel but weren't (due to cost)
	MissedParallelism int64
	// Average degree of parallelism used
	AvgDOP float64
}

// ParallelExecutionMonitor tracks parallel execution metrics
type ParallelExecutionMonitor struct {
	metrics ParallelismMetrics
}

// NewParallelExecutionMonitor creates a new execution monitor
func NewParallelExecutionMonitor() *ParallelExecutionMonitor {
	return &ParallelExecutionMonitor{}
}

// RecordQuery records a query execution decision
func (pem *ParallelExecutionMonitor) RecordQuery(wasParallel bool, dop int) {
	pem.metrics.TotalQueries++

	if wasParallel {
		pem.metrics.ParallelQueries++
		// Update average DOP calculation
		totalDOP := pem.metrics.AvgDOP * float64(pem.metrics.ParallelQueries-1)
		pem.metrics.AvgDOP = (totalDOP + float64(dop)) / float64(pem.metrics.ParallelQueries)
	}
}

// GetMetrics returns current parallel execution metrics
func (pem *ParallelExecutionMonitor) GetMetrics() ParallelismMetrics {
	return pem.metrics
}

// Reset resets all metrics
func (pem *ParallelExecutionMonitor) Reset() {
	pem.metrics = ParallelismMetrics{}
}
