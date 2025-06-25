package executor

import (
	"fmt"
	"sync"
	"time"
)

// AdaptiveExecutionPlan coordinates adaptive behavior across the entire query plan
type AdaptiveExecutionPlan struct {
	baseOperator
	
	// Original and adaptive plan
	originalPlan Operator
	adaptivePlan Operator
	
	// Adaptive coordination
	adaptiveCtx *AdaptiveContext
	config      *AdaptiveExecutionConfig
	
	// Execution monitoring
	startTime       time.Time
	executionPhase  ExecutionPhase
	adaptiveNodes   []*AdaptiveNode
	
	// Global statistics
	globalStats *GlobalExecutionStats
	
	// Plan modification tracking
	planModifications []PlanModification
}

// ExecutionPhase represents the current phase of query execution
type ExecutionPhase int

const (
	InitializationPhase ExecutionPhase = iota
	ExecutionPhase_Running
	AdaptationPhase
	CompletionPhase
)

func (ep ExecutionPhase) String() string {
	switch ep {
	case InitializationPhase:
		return "Initialization"
	case ExecutionPhase_Running:
		return "Running"
	case AdaptationPhase:
		return "Adaptation"
	case CompletionPhase:
		return "Completion"
	default:
		return "Unknown"
	}
}

// AdaptiveNode represents an operator that supports adaptive behavior
type AdaptiveNode struct {
	Operator     Operator
	AdaptiveType string
	Stats        *RuntimeStats
	LastChecked  time.Time
}

// GlobalExecutionStats tracks query-wide execution statistics
type GlobalExecutionStats struct {
	TotalRows          int64
	TotalExecutionTime time.Duration
	MemoryHighWaterMark int64
	AdaptationCount    int64
	
	// Performance metrics
	RowsPerSecond      float64
	MemoryEfficiency   float64
	ParallelEfficiency float64
	
	mu sync.RWMutex
}

// PlanModification records changes made to the execution plan
type PlanModification struct {
	Timestamp   time.Time
	NodeType    string
	Modification string
	Reason      string
	Impact      string
}

// AdaptiveExecutionConfig configures adaptive execution behavior
type AdaptiveExecutionConfig struct {
	// Global adaptation settings
	EnableGlobalAdaptation bool
	GlobalCheckInterval    time.Duration
	MaxAdaptationsPerQuery int
	
	// Memory management
	QueryMemoryLimit   int64
	MemoryCheckInterval time.Duration
	
	// Performance thresholds
	MinRowsPerSecond    float64
	MaxExecutionTime    time.Duration
	AdaptationCooldown  time.Duration
	
	// Parallel execution settings
	EnableParallelAdaptation bool
	MinParallelRows          int64
	MaxParallelWorkers       int
	
	// Plan modification settings
	EnablePlanRewriting    bool
	MaxPlanModifications   int
	RewritingCostThreshold float64
}

// DefaultAdaptiveExecutionConfig returns reasonable defaults
func DefaultAdaptiveExecutionConfig() *AdaptiveExecutionConfig {
	return &AdaptiveExecutionConfig{
		EnableGlobalAdaptation:     true,
		GlobalCheckInterval:        200 * time.Millisecond,
		MaxAdaptationsPerQuery:     5,
		QueryMemoryLimit:           256 * 1024 * 1024, // 256MB default
		MemoryCheckInterval:        100 * time.Millisecond,
		MinRowsPerSecond:           1000,
		MaxExecutionTime:           30 * time.Second,
		AdaptationCooldown:         1 * time.Second,
		EnableParallelAdaptation:   true,
		MinParallelRows:            10000,
		MaxParallelWorkers:         8,
		EnablePlanRewriting:        false, // Conservative default
		MaxPlanModifications:       3,
		RewritingCostThreshold:     1000.0,
	}
}

// NewAdaptiveExecutionPlan creates a new adaptive execution plan
func NewAdaptiveExecutionPlan(
	originalPlan Operator,
	adaptiveCtx *AdaptiveContext,
	config *AdaptiveExecutionConfig,
) *AdaptiveExecutionPlan {
	if config == nil {
		config = DefaultAdaptiveExecutionConfig()
	}
	
	if adaptiveCtx == nil {
		// Create default adaptive context
		adaptiveCtx = &AdaptiveContext{
			Thresholds:  DefaultAdaptiveThresholds(),
			DecisionLog: NewAdaptiveDecisionLog(),
			Enabled:     true,
			MaxMemory:   config.QueryMemoryLimit,
		}
	}
	
	return &AdaptiveExecutionPlan{
		baseOperator: baseOperator{
			schema: originalPlan.Schema(),
		},
		originalPlan:      originalPlan,
		adaptivePlan:      originalPlan, // Start with original plan
		adaptiveCtx:       adaptiveCtx,
		config:            config,
		executionPhase:    InitializationPhase,
		adaptiveNodes:     make([]*AdaptiveNode, 0),
		globalStats:       &GlobalExecutionStats{},
		planModifications: make([]PlanModification, 0),
	}
}

// Open initializes the adaptive execution plan
func (aep *AdaptiveExecutionPlan) Open(ctx *ExecContext) error {
	aep.ctx = ctx
	aep.startTime = time.Now()
	aep.executionPhase = InitializationPhase
	
	// Initialize statistics
	aep.initStats(1000)
	
	// Set up adaptive context
	aep.adaptiveCtx.ExecCtx = ctx
	
	// Transform plan to include adaptive operators
	var err error
	aep.adaptivePlan, err = aep.transformToAdaptivePlan(aep.originalPlan)
	if err != nil {
		// Fall back to original plan
		aep.adaptivePlan = aep.originalPlan
	}
	
	// Discover adaptive nodes in the plan
	aep.discoverAdaptiveNodes(aep.adaptivePlan)
	
	// Log plan transformation
	if aep.adaptiveCtx.DecisionLog != nil {
		stats := NewRuntimeStats(0)
		aep.adaptiveCtx.DecisionLog.LogDecision(
			"AdaptiveExecutionPlan",
			"PlanInitialization",
			fmt.Sprintf("Transformed plan with %d adaptive nodes", len(aep.adaptiveNodes)),
			*stats,
		)
	}
	
	// Open the adaptive plan
	err = aep.adaptivePlan.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open adaptive plan: %w", err)
	}
	
	aep.executionPhase = ExecutionPhase_Running
	return nil
}

// transformToAdaptivePlan transforms operators to their adaptive equivalents
func (aep *AdaptiveExecutionPlan) transformToAdaptivePlan(plan Operator) (Operator, error) {
	if !aep.config.EnableGlobalAdaptation {
		return plan, nil
	}
	
	// Transform different operator types to adaptive versions
	switch op := plan.(type) {
	case *StorageScanOperator:
		// Transform to adaptive scan
		adaptiveScan := NewAdaptiveScanOperator(
			op.table,
			op.tableAlias,
			op.storage,
			nil, // TODO: Extract predicate if wrapped in FilterOperator
			aep.adaptiveCtx,
		)
		return adaptiveScan, nil
		
	case *HashJoinOperator:
		// Transform to adaptive join
		adaptiveJoin := NewAdaptiveJoinOperator(
			op.left, op.right,
			op.leftKeys, op.rightKeys,
			op.predicate, op.joinType,
			aep.adaptiveCtx,
		)
		return adaptiveJoin, nil
		
	case *FilterOperator:
		// Transform child and wrap with filter
		adaptiveChild, err := aep.transformToAdaptivePlan(op.child)
		if err != nil {
			return plan, err
		}
		return NewFilterOperator(adaptiveChild, op.predicate), nil
		
	case *ProjectOperator:
		// Transform child and wrap with projection
		adaptiveChild, err := aep.transformToAdaptivePlan(op.child)
		if err != nil {
			return plan, err
		}
		return NewProjectOperator(adaptiveChild, op.projections, op.schema), nil
		
	case *LimitOperator:
		// Transform child and wrap with limit
		adaptiveChild, err := aep.transformToAdaptivePlan(op.child)
		if err != nil {
			return plan, err
		}
		return NewLimitOperator(adaptiveChild, op.limit, op.offset), nil
		
	default:
		// For other operators, return as-is
		return plan, nil
	}
}

// discoverAdaptiveNodes finds all adaptive operators in the plan tree
func (aep *AdaptiveExecutionPlan) discoverAdaptiveNodes(plan Operator) {
	// Check if this operator is adaptive
	switch op := plan.(type) {
	case *AdaptiveScanOperator:
		aep.adaptiveNodes = append(aep.adaptiveNodes, &AdaptiveNode{
			Operator:     op,
			AdaptiveType: "AdaptiveScan",
			Stats:        op.scanStats,
			LastChecked:  time.Now(),
		})
	case *AdaptiveJoinOperator:
		aep.adaptiveNodes = append(aep.adaptiveNodes, &AdaptiveNode{
			Operator:     op,
			AdaptiveType: "AdaptiveJoin",
			Stats:        op.joinStats,
			LastChecked:  time.Now(),
		})
	}
	
	// Recursively check children (simplified - real implementation would handle all operator types)
	switch op := plan.(type) {
	case *FilterOperator:
		aep.discoverAdaptiveNodes(op.child)
	case *ProjectOperator:
		aep.discoverAdaptiveNodes(op.child)
	case *LimitOperator:
		aep.discoverAdaptiveNodes(op.child)
	case *HashJoinOperator:
		aep.discoverAdaptiveNodes(op.left)
		aep.discoverAdaptiveNodes(op.right)
	case *AdaptiveJoinOperator:
		aep.discoverAdaptiveNodes(op.left)
		aep.discoverAdaptiveNodes(op.right)
	}
}

// Next returns the next row with global adaptive coordination
func (aep *AdaptiveExecutionPlan) Next() (*Row, error) {
	// Check for global adaptations periodically
	if aep.shouldPerformGlobalCheck() {
		if err := aep.performGlobalAdaptation(); err != nil {
			// Log error but continue execution
			fmt.Printf("Global adaptation error: %v\n", err)
		}
	}
	
	// Get next row from adaptive plan
	row, err := aep.adaptivePlan.Next()
	if err != nil {
		return nil, err
	}
	
	if row != nil {
		// Update global statistics
		aep.updateGlobalStats()
		aep.recordRow()
	} else {
		// EOF - finalize statistics
		aep.executionPhase = CompletionPhase
		aep.finalizeGlobalStats()
		aep.finishStats()
		if aep.ctx != nil && aep.ctx.StatsCollector != nil && aep.stats != nil {
			aep.ctx.StatsCollector(aep, aep.stats)
		}
	}
	
	return row, nil
}

// shouldPerformGlobalCheck determines if we should check for global adaptations
func (aep *AdaptiveExecutionPlan) shouldPerformGlobalCheck() bool {
	if !aep.config.EnableGlobalAdaptation {
		return false
	}
	
	if aep.executionPhase != ExecutionPhase_Running {
		return false
	}
	
	// Don't check too frequently
	now := time.Now()
	for _, node := range aep.adaptiveNodes {
		if now.Sub(node.LastChecked) >= aep.config.GlobalCheckInterval {
			return true
		}
	}
	
	return false
}

// performGlobalAdaptation checks all adaptive nodes and coordinates changes
func (aep *AdaptiveExecutionPlan) performGlobalAdaptation() error {
	aep.executionPhase = AdaptationPhase
	defer func() { aep.executionPhase = ExecutionPhase_Running }()
	
	now := time.Now()
	adaptationsMade := 0
	
	// Check memory pressure globally
	if aep.isGlobalMemoryPressure() {
		if err := aep.handleGlobalMemoryPressure(); err != nil {
			return fmt.Errorf("failed to handle memory pressure: %w", err)
		}
		adaptationsMade++
	}
	
	// Check performance across all nodes
	if aep.isGlobalPerformancePoor() {
		if err := aep.handleGlobalPerformanceIssues(); err != nil {
			return fmt.Errorf("failed to handle performance issues: %w", err)
		}
		adaptationsMade++
	}
	
	// Update last checked time for all nodes
	for _, node := range aep.adaptiveNodes {
		node.LastChecked = now
	}
	
	// Log global adaptation if any changes were made
	if adaptationsMade > 0 {
		aep.globalStats.AdaptationCount += int64(adaptationsMade)
		
		if aep.adaptiveCtx.DecisionLog != nil {
			stats := NewRuntimeStats(aep.globalStats.TotalRows)
			aep.adaptiveCtx.DecisionLog.LogDecision(
				"AdaptiveExecutionPlan",
				"GlobalAdaptation",
				fmt.Sprintf("Made %d global adaptations", adaptationsMade),
				*stats,
			)
		}
	}
	
	return nil
}

// isGlobalMemoryPressure checks if the query is using too much memory
func (aep *AdaptiveExecutionPlan) isGlobalMemoryPressure() bool {
	if aep.config.QueryMemoryLimit <= 0 {
		return false
	}
	
	// In a real implementation, this would check actual memory usage
	// For now, we'll use a placeholder based on row count heuristic
	estimatedMemory := aep.globalStats.TotalRows * 1024 // 1KB per row estimate
	
	return estimatedMemory > aep.config.QueryMemoryLimit
}

// handleGlobalMemoryPressure takes actions to reduce memory usage
func (aep *AdaptiveExecutionPlan) handleGlobalMemoryPressure() error {
	// Log the decision
	if aep.adaptiveCtx.DecisionLog != nil {
		stats := NewRuntimeStats(aep.globalStats.TotalRows)
		aep.adaptiveCtx.DecisionLog.LogDecision(
			"AdaptiveExecutionPlan",
			"MemoryPressureResponse",
			"Detected global memory pressure",
			*stats,
		)
	}
	
	// In a real implementation, we would:
	// 1. Force spilling in hash joins
	// 2. Switch parallel scans to sequential
	// 3. Reduce buffer sizes
	// 4. Enable external sorting
	
	return nil
}

// isGlobalPerformancePoor checks if overall query performance is poor
func (aep *AdaptiveExecutionPlan) isGlobalPerformancePoor() bool {
	// Check if we're taking too long
	if time.Since(aep.startTime) > aep.config.MaxExecutionTime {
		return true
	}
	
	// Check if throughput is too low
	if aep.globalStats.RowsPerSecond > 0 && aep.globalStats.RowsPerSecond < aep.config.MinRowsPerSecond {
		return true
	}
	
	return false
}

// handleGlobalPerformanceIssues takes actions to improve performance
func (aep *AdaptiveExecutionPlan) handleGlobalPerformanceIssues() error {
	// Log the decision
	if aep.adaptiveCtx.DecisionLog != nil {
		stats := NewRuntimeStats(aep.globalStats.TotalRows)
		aep.adaptiveCtx.DecisionLog.LogDecision(
			"AdaptiveExecutionPlan",
			"PerformanceOptimization",
			fmt.Sprintf("Low performance detected (%.1f rows/s)", aep.globalStats.RowsPerSecond),
			*stats,
		)
	}
	
	// In a real implementation, we would:
	// 1. Enable parallelism where possible
	// 2. Switch join algorithms
	// 3. Push filters down
	// 4. Consider plan rewriting
	
	return nil
}

// updateGlobalStats updates query-wide execution statistics
func (aep *AdaptiveExecutionPlan) updateGlobalStats() {
	aep.globalStats.mu.Lock()
	defer aep.globalStats.mu.Unlock()
	
	aep.globalStats.TotalRows++
	aep.globalStats.TotalExecutionTime = time.Since(aep.startTime)
	
	// Calculate rows per second
	if aep.globalStats.TotalExecutionTime > 0 {
		aep.globalStats.RowsPerSecond = float64(aep.globalStats.TotalRows) / aep.globalStats.TotalExecutionTime.Seconds()
	}
}

// finalizeGlobalStats finalizes statistics at query completion
func (aep *AdaptiveExecutionPlan) finalizeGlobalStats() {
	aep.globalStats.mu.Lock()
	defer aep.globalStats.mu.Unlock()
	
	aep.globalStats.TotalExecutionTime = time.Since(aep.startTime)
	
	// Calculate final metrics
	if aep.globalStats.TotalExecutionTime > 0 {
		aep.globalStats.RowsPerSecond = float64(aep.globalStats.TotalRows) / aep.globalStats.TotalExecutionTime.Seconds()
	}
}

// Close cleans up the adaptive execution plan
func (aep *AdaptiveExecutionPlan) Close() error {
	aep.executionPhase = CompletionPhase
	
	// Ensure stats are finalized
	aep.finishStats()
	if aep.ctx != nil && aep.ctx.StatsCollector != nil && aep.stats != nil {
		aep.ctx.StatsCollector(aep, aep.stats)
	}
	
	// Close the adaptive plan
	if aep.adaptivePlan != nil {
		return aep.adaptivePlan.Close()
	}
	
	return nil
}

// GetAdaptiveExecutionReport generates a comprehensive adaptive execution report
func (aep *AdaptiveExecutionPlan) GetAdaptiveExecutionReport() *AdaptiveExecutionReport {
	report := &AdaptiveExecutionReport{
		QueryExecutionTime: aep.globalStats.TotalExecutionTime,
		TotalRowsProcessed: aep.globalStats.TotalRows,
		AdaptationCount:    aep.globalStats.AdaptationCount,
		FinalThroughput:    aep.globalStats.RowsPerSecond,
		AdaptiveNodes:      len(aep.adaptiveNodes),
		PlanModifications:  len(aep.planModifications),
	}
	
	// Add decisions from log
	if aep.adaptiveCtx.DecisionLog != nil {
		report.Decisions = aep.adaptiveCtx.DecisionLog.GetDecisions()
	}
	
	// Add node-specific statistics
	for _, node := range aep.adaptiveNodes {
		nodeReport := AdaptiveNodeReport{
			NodeType: node.AdaptiveType,
		}
		
		// Add node-specific stats
		switch adaptiveOp := node.Operator.(type) {
		case *AdaptiveScanOperator:
			nodeReport.SpecificStats = adaptiveOp.GetAdaptiveStats()
		case *AdaptiveJoinOperator:
			nodeReport.SpecificStats = adaptiveOp.GetAdaptiveStats()
		}
		
		report.NodeReports = append(report.NodeReports, nodeReport)
	}
	
	return report
}

// AdaptiveExecutionReport provides comprehensive information about adaptive execution
type AdaptiveExecutionReport struct {
	QueryExecutionTime time.Duration
	TotalRowsProcessed int64
	AdaptationCount    int64
	FinalThroughput    float64
	AdaptiveNodes      int
	PlanModifications  int
	Decisions          []AdaptiveDecision
	NodeReports        []AdaptiveNodeReport
}

// AdaptiveNodeReport provides information about a specific adaptive node
type AdaptiveNodeReport struct {
	NodeType      string
	SpecificStats map[string]interface{}
}