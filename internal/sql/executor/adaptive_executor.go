package executor

import (
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/txn"
)

// AdaptiveExecutor integrates with the adaptive physical planner for runtime optimization
type AdaptiveExecutor struct {
	*ConfigurableExecutor
	adaptivePlanner      *planner.AdaptivePhysicalPlanner
	feedbackCollector    *planner.RuntimeFeedbackCollector
	executionMonitor     *ExecutionMonitor
	adaptiveConfig       *AdaptiveExecutorConfig
	operatorRegistry     *OperatorRegistry
}

// AdaptiveExecutorConfig configures adaptive execution behavior
type AdaptiveExecutorConfig struct {
	// Enable adaptive execution
	EnableAdaptiveExecution bool
	// Threshold for re-planning during execution
	ReplanThreshold float64
	// Maximum re-planning attempts
	MaxReplanAttempts int
	// Enable operator-level adaptation
	EnableOperatorAdaptation bool
	// Batch size for adaptive operators
	AdaptiveBatchSize int
	// Memory pressure threshold for adaptation
	MemoryPressureThreshold float64
}

// DefaultAdaptiveExecutorConfig returns default configuration
func DefaultAdaptiveExecutorConfig() *AdaptiveExecutorConfig {
	return &AdaptiveExecutorConfig{
		EnableAdaptiveExecution:  true,
		ReplanThreshold:         2.0, // Replan if actual cost is 2x predicted
		MaxReplanAttempts:       3,
		EnableOperatorAdaptation: true,
		AdaptiveBatchSize:       1024,
		MemoryPressureThreshold: 0.8,
	}
}

// ExecutionMonitor tracks execution progress and detects adaptation opportunities
type ExecutionMonitor struct {
	mu              sync.RWMutex
	operatorMetrics map[string]*OperatorMetrics
	currentMemory   int64
	peakMemory      int64
	startTime       time.Time
}

// OperatorMetrics tracks runtime metrics for an operator
type OperatorMetrics struct {
	OperatorID       string
	OperatorType     planner.OperatorType
	RowsProcessed    int64
	ExecutionTime    time.Duration
	MemoryUsed       int64
	ActualSelectivity float64
	ExecutionMode    planner.ExecutionMode
	LastUpdate       time.Time
}

// OperatorRegistry manages operator implementations
type OperatorRegistry struct {
	scalarOperators     map[planner.OperatorType]OperatorFactory
	vectorizedOperators map[planner.OperatorType]VectorizedOperatorFactory
	adaptiveOperators   map[planner.OperatorType]AdaptiveOperatorFactory
}

// OperatorFactory creates scalar operators
type OperatorFactory func(plan planner.PhysicalPlan, ctx *ExecContext) (Operator, error)

// VectorizedOperatorFactory creates vectorized operators
type VectorizedOperatorFactory func(plan planner.PhysicalPlan, ctx *ExecContext) (VectorizedOperator, error)

// AdaptiveOperatorFactory creates adaptive operators
type AdaptiveOperatorFactory func(plan planner.PhysicalPlan, ctx *ExecContext, monitor *ExecutionMonitor) (AdaptiveOperator, error)

// AdaptiveOperator can switch execution modes during runtime
type AdaptiveOperator interface {
	Operator
	// SwitchMode switches between scalar and vectorized execution
	SwitchMode(mode planner.ExecutionMode) error
	// GetCurrentMode returns the current execution mode
	GetCurrentMode() planner.ExecutionMode
	// GetMetrics returns current operator metrics
	GetMetrics() *OperatorMetrics
}

// NewAdaptiveExecutor creates a new adaptive executor
func NewAdaptiveExecutor(
	engine engine.Engine,
	catalog catalog.Catalog,
	txnManager *txn.Manager,
	adaptivePlanner *planner.AdaptivePhysicalPlanner,
	cfg *ExecutorRuntimeConfig,
) *AdaptiveExecutor {
	configurable := NewConfigurableExecutor(engine, catalog, txnManager, cfg)
	
	return &AdaptiveExecutor{
		ConfigurableExecutor: configurable,
		adaptivePlanner:      adaptivePlanner,
		feedbackCollector:    planner.NewRuntimeFeedbackCollector(),
		executionMonitor:     NewExecutionMonitor(),
		adaptiveConfig:       DefaultAdaptiveExecutorConfig(),
		operatorRegistry:     NewOperatorRegistry(),
	}
}

// NewExecutionMonitor creates a new execution monitor
func NewExecutionMonitor() *ExecutionMonitor {
	return &ExecutionMonitor{
		operatorMetrics: make(map[string]*OperatorMetrics),
		startTime:       time.Now(),
	}
}

// NewOperatorRegistry creates a new operator registry
func NewOperatorRegistry() *OperatorRegistry {
	registry := &OperatorRegistry{
		scalarOperators:     make(map[planner.OperatorType]OperatorFactory),
		vectorizedOperators: make(map[planner.OperatorType]VectorizedOperatorFactory),
		adaptiveOperators:   make(map[planner.OperatorType]AdaptiveOperatorFactory),
	}
	
	// Register default operators
	registry.registerDefaultOperators()
	
	return registry
}

// Execute executes a plan with adaptive optimization
func (ae *AdaptiveExecutor) Execute(ctx *ExecContext, plan planner.Plan, tx *txn.Transaction) (Result, error) {
	startTime := time.Now()
	
	// Convert logical plan to adaptive physical plan
	physicalPlan, err := ae.createAdaptivePhysicalPlan(plan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create physical plan: %w", err)
	}
	
	// Create adaptive operator tree
	rootOp, err := ae.createAdaptiveOperator(physicalPlan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create operator: %w", err)
	}
	
	// Wrap with monitoring
	monitoredOp := ae.wrapWithMonitoring(rootOp)
	
	// Execute with adaptation
	result, err := ae.executeWithAdaptation(monitoredOp, ctx, physicalPlan)
	
	// Collect feedback
	ae.collectExecutionFeedback(physicalPlan, startTime, err == nil)
	
	return result, err
}

// createAdaptivePhysicalPlan creates an adaptive physical plan
func (ae *AdaptiveExecutor) createAdaptivePhysicalPlan(
	logical planner.Plan,
	ctx *ExecContext,
) (planner.PhysicalPlan, error) {
	// Create physical plan context
	planContext := &planner.PhysicalPlanContext{
		MemoryAvailable:   ae.getAvailableMemory(),
		VectorizationMode: planner.VectorizationModeAdaptive,
		BatchSize:         ae.adaptiveConfig.AdaptiveBatchSize,
		EnableCaching:     ae.config.IsCachingEnabled(),
	}
	
	// Add table statistics
	planContext.TableStats = ae.gatherTableStats(logical)
	
	// Generate adaptive physical plan
	// First try to convert Plan to LogicalPlan
	logicalPlan, ok := logical.(planner.LogicalPlan)
	if !ok {
		// If it's already a PhysicalPlan, just return it
		if physicalPlan, ok := logical.(planner.PhysicalPlan); ok {
			return physicalPlan, nil
		}
		return nil, fmt.Errorf("plan is not a logical plan: %T", logical)
	}
	
	return ae.adaptivePlanner.GenerateAdaptivePhysicalPlan(logicalPlan, planContext)
}

// createAdaptiveOperator creates an adaptive operator from a physical plan
func (ae *AdaptiveExecutor) createAdaptiveOperator(
	plan planner.PhysicalPlan,
	ctx *ExecContext,
) (AdaptiveOperator, error) {
	opType := plan.GetOperatorType()
	
	// Check if we have an adaptive implementation
	if factory, ok := ae.operatorRegistry.adaptiveOperators[opType]; ok {
		return factory(plan, ctx, ae.executionMonitor)
	}
	
	// Fall back to creating a wrapped operator
	return ae.createWrappedAdaptiveOperator(plan, ctx)
}

// createWrappedAdaptiveOperator wraps scalar/vectorized operators with adaptive behavior
func (ae *AdaptiveExecutor) createWrappedAdaptiveOperator(
	plan planner.PhysicalPlan,
	ctx *ExecContext,
) (AdaptiveOperator, error) {
	opType := plan.GetOperatorType()
	executionMode := plan.GetExecutionMode()
	
	// Create scalar and vectorized implementations
	var scalarOp Operator
	var vectorizedOp VectorizedOperator
	var err error
	
	if scalarFactory, ok := ae.operatorRegistry.scalarOperators[opType]; ok {
		scalarOp, err = scalarFactory(plan, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create scalar operator: %w", err)
		}
	}
	
	if vectorizedFactory, ok := ae.operatorRegistry.vectorizedOperators[opType]; ok {
		vectorizedOp, err = vectorizedFactory(plan, ctx)
		if err != nil && executionMode == planner.ExecutionModeVectorized {
			return nil, fmt.Errorf("failed to create vectorized operator: %w", err)
		}
	}
	
	// Create adaptive wrapper
	return &adaptiveOperatorWrapper{
		physicalPlan:  plan,
		scalarOp:      scalarOp,
		vectorizedOp:  vectorizedOp,
		currentMode:   executionMode,
		metrics:       ae.createOperatorMetrics(plan),
		monitor:       ae.executionMonitor,
	}, nil
}

// executeWithAdaptation executes an operator with runtime adaptation
func (ae *AdaptiveExecutor) executeWithAdaptation(
	op AdaptiveOperator,
	ctx *ExecContext,
	plan planner.PhysicalPlan,
) (Result, error) {
	// Open operator
	if err := op.Open(ctx); err != nil {
		return nil, err
	}
	
	// Create adaptive result wrapper
	result := &adaptiveResultWrapper{
		operator:     op,
		executor:     ae,
		context:      ctx,
		physicalPlan: plan,
		replanCount:  0,
	}
	
	return result, nil
}

// wrapWithMonitoring wraps an operator with execution monitoring
func (ae *AdaptiveExecutor) wrapWithMonitoring(op AdaptiveOperator) AdaptiveOperator {
	return &monitoringOperatorWrapper{
		AdaptiveOperator: op,
		monitor:         ae.executionMonitor,
	}
}

// collectExecutionFeedback collects feedback for the planner
func (ae *AdaptiveExecutor) collectExecutionFeedback(
	plan planner.PhysicalPlan,
	startTime time.Time,
	success bool,
) {
	executionTime := time.Since(startTime)
	
	// Get final metrics from monitor
	metrics := ae.executionMonitor.GetMetrics()
	
	// Record operator performance
	ae.adaptivePlanner.RecordOperatorPerformance(
		plan,
		executionTime,
		metrics.TotalMemory,
		metrics.TotalRows,
		success,
	)
	
	// Record query execution
	signature := ae.createQuerySignature(plan)
	ae.feedbackCollector.RecordQueryExecution(
		signature,
		executionTime,
		metrics.TotalMemory,
		metrics.TotalRows,
		plan.GetExecutionMode(),
		success,
	)
}

// getAvailableMemory returns available memory for query execution
func (ae *AdaptiveExecutor) getAvailableMemory() int64 {
	// Get total memory limit
	totalLimit := ae.config.QueryMemoryLimit
	
	// Subtract current usage
	currentUsage := ae.memoryTracker.CurrentUsage()
	
	available := totalLimit - currentUsage
	if available < 0 {
		return 0
	}
	
	return available
}

// gatherTableStats gathers table statistics for planning
func (ae *AdaptiveExecutor) gatherTableStats(plan planner.Plan) map[string]*catalog.TableStats {
	stats := make(map[string]*catalog.TableStats)
	
	// Walk plan tree and collect table references
	ae.walkPlanForTables(plan, func(tableName string) {
		if table, err := ae.catalog.GetTable("public", tableName); err == nil {
			stats[tableName] = table.Stats
		}
	})
	
	return stats
}

// walkPlanForTables walks a plan tree collecting table names
func (ae *AdaptiveExecutor) walkPlanForTables(plan planner.Plan, visitor func(string)) {
	switch p := plan.(type) {
	case *planner.LogicalScan:
		visitor(p.TableName)
	case *planner.PhysicalScan:
		visitor(p.Table.TableName)
	}
	
	// Recursively visit children
	for _, child := range plan.Children() {
		ae.walkPlanForTables(child, visitor)
	}
}

// createQuerySignature creates a signature for query tracking
func (ae *AdaptiveExecutor) createQuerySignature(plan planner.PhysicalPlan) planner.QuerySignature {
	return planner.QuerySignature{
		QueryHash:      fmt.Sprintf("%p", plan), // Simple hash
		MainOperator:   plan.GetOperatorType(),
		JoinCount:      ae.countOperators(plan, planner.OperatorTypeJoin),
		PredicateCount: ae.countPredicates(plan),
		AggregateCount: ae.countOperators(plan, planner.OperatorTypeAggregate),
	}
}

// countOperators counts operators of a specific type
func (ae *AdaptiveExecutor) countOperators(plan planner.PhysicalPlan, opType planner.OperatorType) int {
	count := 0
	if plan.GetOperatorType() == opType {
		count = 1
	}
	
	for _, input := range plan.GetInputs() {
		count += ae.countOperators(input, opType)
	}
	
	return count
}

// countPredicates counts predicates in a plan
func (ae *AdaptiveExecutor) countPredicates(plan planner.PhysicalPlan) int {
	count := 0
	
	switch p := plan.(type) {
	case *planner.PhysicalFilter:
		if p.Predicate != nil {
			count = 1
		}
	case *planner.PhysicalJoin:
		if p.Condition != nil {
			count = 1
		}
	}
	
	for _, input := range plan.GetInputs() {
		count += ae.countPredicates(input)
	}
	
	return count
}

// createOperatorMetrics creates metrics for an operator
func (ae *AdaptiveExecutor) createOperatorMetrics(plan planner.PhysicalPlan) *OperatorMetrics {
	return &OperatorMetrics{
		OperatorID:    fmt.Sprintf("%p", plan),
		OperatorType:  plan.GetOperatorType(),
		ExecutionMode: plan.GetExecutionMode(),
		LastUpdate:    time.Now(),
	}
}

// ExecutionMonitor methods

// RecordOperatorMetrics records metrics for an operator
func (em *ExecutionMonitor) RecordOperatorMetrics(metrics *OperatorMetrics) {
	em.mu.Lock()
	defer em.mu.Unlock()
	
	em.operatorMetrics[metrics.OperatorID] = metrics
	
	// Update memory tracking
	totalMemory := int64(0)
	for _, m := range em.operatorMetrics {
		totalMemory += m.MemoryUsed
	}
	em.currentMemory = totalMemory
	if totalMemory > em.peakMemory {
		em.peakMemory = totalMemory
	}
}

// GetMetrics returns aggregated execution metrics
func (em *ExecutionMonitor) GetMetrics() struct {
	TotalRows   int64
	TotalMemory int64
	PeakMemory  int64
	Duration    time.Duration
} {
	em.mu.RLock()
	defer em.mu.RUnlock()
	
	totalRows := int64(0)
	for _, m := range em.operatorMetrics {
		totalRows += m.RowsProcessed
	}
	
	return struct {
		TotalRows   int64
		TotalMemory int64
		PeakMemory  int64
		Duration    time.Duration
	}{
		TotalRows:   totalRows,
		TotalMemory: em.currentMemory,
		PeakMemory:  em.peakMemory,
		Duration:    time.Since(em.startTime),
	}
}

// OperatorRegistry methods

// registerDefaultOperators registers default operator implementations
func (or *OperatorRegistry) registerDefaultOperators() {
	// Register scalar operators
	or.scalarOperators[planner.OperatorTypeScan] = createScalarScan
	or.scalarOperators[planner.OperatorTypeFilter] = createScalarFilter
	or.scalarOperators[planner.OperatorTypeJoin] = createScalarJoin
	or.scalarOperators[planner.OperatorTypeAggregate] = createScalarAggregate
	or.scalarOperators[planner.OperatorTypeSort] = createScalarSort
	or.scalarOperators[planner.OperatorTypeProjection] = createScalarProjection
	
	// Register vectorized operators
	or.vectorizedOperators[planner.OperatorTypeScan] = createVectorizedScan
	or.vectorizedOperators[planner.OperatorTypeFilter] = createVectorizedFilter
	or.vectorizedOperators[planner.OperatorTypeProjection] = createVectorizedProjection
	
	// Register adaptive operators
	or.adaptiveOperators[planner.OperatorTypeScan] = createAdaptiveScan
	or.adaptiveOperators[planner.OperatorTypeFilter] = createAdaptiveFilter
}

// Operator factory implementations (simplified)

func createScalarScan(plan planner.PhysicalPlan, ctx *ExecContext) (Operator, error) {
	scan := plan.(*planner.PhysicalScan)
	return NewScanOperator(scan.Table, ctx), nil
}

func createScalarFilter(plan planner.PhysicalPlan, ctx *ExecContext) (Operator, error) {
	filter := plan.(*planner.PhysicalFilter)
	
	// Create input operator
	inputOp, err := createOperatorFromPlan(filter.Input, ctx)
	if err != nil {
		return nil, err
	}
	
	// Build expression evaluator
	evaluator, err := buildExprEvaluator(filter.Predicate)
	if err != nil {
		return nil, err
	}
	
	return NewFilterOperator(inputOp, evaluator), nil
}

func createScalarJoin(plan planner.PhysicalPlan, ctx *ExecContext) (Operator, error) {
	// Implementation would create appropriate join operator
	return nil, fmt.Errorf("scalar join not implemented")
}

func createScalarAggregate(plan planner.PhysicalPlan, ctx *ExecContext) (Operator, error) {
	// Implementation would create aggregate operator
	return nil, fmt.Errorf("scalar aggregate not implemented")
}

func createScalarSort(plan planner.PhysicalPlan, ctx *ExecContext) (Operator, error) {
	// Implementation would create sort operator
	return nil, fmt.Errorf("scalar sort not implemented")
}

func createScalarProjection(plan planner.PhysicalPlan, ctx *ExecContext) (Operator, error) {
	// Implementation would create projection operator
	return nil, fmt.Errorf("scalar projection not implemented")
}

func createVectorizedScan(plan planner.PhysicalPlan, ctx *ExecContext) (VectorizedOperator, error) {
	scan := plan.(*planner.PhysicalScan)
	
	schema := &Schema{
		Columns: make([]Column, len(scan.Table.Columns)),
	}
	for i, col := range scan.Table.Columns {
		schema.Columns[i] = Column{
			Name: col.Name,
			Type: col.DataType,
		}
	}
	
	return NewVectorizedScanOperator(schema, int64(scan.Table.ID)), nil
}

func createVectorizedFilter(plan planner.PhysicalPlan, ctx *ExecContext) (VectorizedOperator, error) {
	filter := plan.(*planner.PhysicalFilter)
	
	// Create input operator
	inputOp, err := createVectorizedOperatorFromPlan(filter.Input, ctx)
	if err != nil {
		return nil, err
	}
	
	return NewVectorizedFilterOperatorWithFallback(inputOp, filter.Predicate), nil
}

func createVectorizedProjection(plan planner.PhysicalPlan, ctx *ExecContext) (VectorizedOperator, error) {
	// Implementation would create vectorized projection
	return nil, fmt.Errorf("vectorized projection not implemented")
}

func createAdaptiveScan(plan planner.PhysicalPlan, ctx *ExecContext, monitor *ExecutionMonitor) (AdaptiveOperator, error) {
	// Create both scalar and vectorized versions
	scalarOp, err := createScalarScan(plan, ctx)
	if err != nil {
		return nil, err
	}
	
	vectorizedOp, err := createVectorizedScan(plan, ctx)
	if err != nil {
		// Vectorized scan failed, use scalar only
		return &adaptiveOperatorWrapper{
			physicalPlan: plan,
			scalarOp:     scalarOp,
			currentMode:  planner.ExecutionModeScalar,
			monitor:      monitor,
		}, nil
	}
	
	return &adaptiveOperatorWrapper{
		physicalPlan: plan,
		scalarOp:     scalarOp,
		vectorizedOp: vectorizedOp,
		currentMode:  plan.GetExecutionMode(),
		monitor:      monitor,
	}, nil
}

func createAdaptiveFilter(plan planner.PhysicalPlan, ctx *ExecContext, monitor *ExecutionMonitor) (AdaptiveOperator, error) {
	// Similar to adaptive scan
	return nil, fmt.Errorf("adaptive filter not implemented")
}

// Helper functions

func createOperatorFromPlan(plan planner.PhysicalPlan, ctx *ExecContext) (Operator, error) {
	// Simplified operator creation
	return nil, fmt.Errorf("operator creation not implemented")
}

func createVectorizedOperatorFromPlan(plan planner.PhysicalPlan, ctx *ExecContext) (VectorizedOperator, error) {
	// Simplified vectorized operator creation
	return nil, fmt.Errorf("vectorized operator creation not implemented")
}