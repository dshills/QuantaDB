package planner

import (
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// AdaptivePhysicalPlanner extends PhysicalPlanner with runtime adaptability
type AdaptivePhysicalPlanner struct {
	*PhysicalPlanner
	memoryMonitor     *MemoryPressureMonitor
	historyTracker    *OperatorHistoryTracker
	adaptiveDecider   *AdaptiveVectorizationDecider
	feedbackCollector *RuntimeFeedbackCollector
}

// MemoryPressureMonitor tracks system memory usage
type MemoryPressureMonitor struct {
	mu                   sync.RWMutex
	currentMemoryUsed    int64
	totalMemoryAvailable int64
	pressureThreshold    float64
	lastUpdate           time.Time
	historicalPressure   []float64
	maxHistorySize       int
}

// NewMemoryPressureMonitor creates a new memory monitor
func NewMemoryPressureMonitor(totalMemory int64, pressureThreshold float64) *MemoryPressureMonitor {
	return &MemoryPressureMonitor{
		totalMemoryAvailable: totalMemory,
		pressureThreshold:    pressureThreshold,
		maxHistorySize:       100,
		historicalPressure:   make([]float64, 0, 100),
	}
}

// UpdateMemoryUsage updates the current memory usage
func (mpm *MemoryPressureMonitor) UpdateMemoryUsage(used int64) {
	mpm.mu.Lock()
	defer mpm.mu.Unlock()

	mpm.currentMemoryUsed = used
	mpm.lastUpdate = time.Now()

	pressure := float64(used) / float64(mpm.totalMemoryAvailable)
	mpm.historicalPressure = append(mpm.historicalPressure, pressure)

	if len(mpm.historicalPressure) > mpm.maxHistorySize {
		mpm.historicalPressure = mpm.historicalPressure[1:]
	}
}

// GetMemoryPressure returns the current memory pressure level
func (mpm *MemoryPressureMonitor) GetMemoryPressure() float64 {
	mpm.mu.RLock()
	defer mpm.mu.RUnlock()

	if mpm.totalMemoryAvailable == 0 {
		return 0.0
	}

	return float64(mpm.currentMemoryUsed) / float64(mpm.totalMemoryAvailable)
}

// IsUnderPressure checks if system is under memory pressure
func (mpm *MemoryPressureMonitor) IsUnderPressure() bool {
	return mpm.GetMemoryPressure() > mpm.pressureThreshold
}

// GetAveragePressure returns the average memory pressure over history
func (mpm *MemoryPressureMonitor) GetAveragePressure() float64 {
	mpm.mu.RLock()
	defer mpm.mu.RUnlock()

	if len(mpm.historicalPressure) == 0 {
		return 0.0
	}

	sum := 0.0
	for _, p := range mpm.historicalPressure {
		sum += p
	}

	return sum / float64(len(mpm.historicalPressure))
}

// OperatorHistoryTracker tracks performance history of operators
type OperatorHistoryTracker struct {
	mu       sync.RWMutex
	history  map[OperatorSignature]*OperatorHistory
	maxItems int
}

// OperatorSignature uniquely identifies an operator configuration
type OperatorSignature struct {
	OperatorType  OperatorType
	ExecutionMode ExecutionMode
	Cardinality   int64 // Bucketed to nearest power of 10
	HasPredicate  bool
}

// OperatorHistory tracks historical performance of an operator
type OperatorHistory struct {
	Signature         OperatorSignature
	ExecutionCount    int64
	TotalTime         time.Duration
	TotalMemoryUsed   int64
	TotalRowsProduced int64
	LastExecution     time.Time
	SuccessRate       float64
}

// NewOperatorHistoryTracker creates a new history tracker
func NewOperatorHistoryTracker(maxItems int) *OperatorHistoryTracker {
	return &OperatorHistoryTracker{
		history:  make(map[OperatorSignature]*OperatorHistory),
		maxItems: maxItems,
	}
}

// RecordExecution records an operator execution
func (oht *OperatorHistoryTracker) RecordExecution(
	signature OperatorSignature,
	executionTime time.Duration,
	memoryUsed int64,
	rowsProduced int64,
	success bool,
) {
	oht.mu.Lock()
	defer oht.mu.Unlock()

	// Bucket cardinality to nearest power of 10
	signature.Cardinality = bucketCardinality(signature.Cardinality)

	history, exists := oht.history[signature]
	if !exists {
		history = &OperatorHistory{
			Signature: signature,
		}
		oht.history[signature] = history
	}

	history.ExecutionCount++
	history.TotalTime += executionTime
	history.TotalMemoryUsed += memoryUsed
	history.TotalRowsProduced += rowsProduced
	history.LastExecution = time.Now()

	// Update success rate
	if success {
		history.SuccessRate = (history.SuccessRate*float64(history.ExecutionCount-1) + 1.0) /
			float64(history.ExecutionCount)
	} else {
		history.SuccessRate = (history.SuccessRate * float64(history.ExecutionCount-1)) /
			float64(history.ExecutionCount)
	}
}

// GetAveragePerformance returns average performance metrics
func (oht *OperatorHistoryTracker) GetAveragePerformance(
	signature OperatorSignature,
) (avgTime time.Duration, avgMemory int64, avgRows int64, found bool) {
	oht.mu.RLock()
	defer oht.mu.RUnlock()

	signature.Cardinality = bucketCardinality(signature.Cardinality)

	history, exists := oht.history[signature]
	if !exists || history.ExecutionCount == 0 {
		return 0, 0, 0, false
	}

	avgTime = history.TotalTime / time.Duration(history.ExecutionCount)
	avgMemory = history.TotalMemoryUsed / history.ExecutionCount
	avgRows = history.TotalRowsProduced / history.ExecutionCount

	return avgTime, avgMemory, avgRows, true
}

// AdaptiveVectorizationDecider makes intelligent vectorization decisions
type AdaptiveVectorizationDecider struct {
	costModel      *EnhancedVectorizedCostModel
	historyTracker *OperatorHistoryTracker
	memoryMonitor  *MemoryPressureMonitor
	config         *AdaptiveDeciderConfig
}

// AdaptiveDeciderConfig configures the adaptive decider
type AdaptiveDeciderConfig struct {
	MinCardinalityForVectorization int64
	MemoryPressureThreshold        float64
	HistoryWeight                  float64
	CostDifferenceThreshold        float64
	EnableFallback                 bool
	PreferVectorizedUnderThreshold int64
}

// NewAdaptiveVectorizationDecider creates a new adaptive decider
func NewAdaptiveVectorizationDecider(
	costModel *EnhancedVectorizedCostModel,
	historyTracker *OperatorHistoryTracker,
	memoryMonitor *MemoryPressureMonitor,
) *AdaptiveVectorizationDecider {
	return &AdaptiveVectorizationDecider{
		costModel:      costModel,
		historyTracker: historyTracker,
		memoryMonitor:  memoryMonitor,
		config: &AdaptiveDeciderConfig{
			MinCardinalityForVectorization: 1000,
			MemoryPressureThreshold:        0.8,
			HistoryWeight:                  0.3,
			CostDifferenceThreshold:        1.2,
			EnableFallback:                 true,
			PreferVectorizedUnderThreshold: 10000,
		},
	}
}

// VectorizationDecision represents a vectorization decision
type VectorizationDecision struct {
	UseVectorized     bool
	BatchSize         int
	FallbackStrategy  FallbackStrategy
	Reasoning         []string
	ConfidenceScore   float64
	EstimatedSpeedup  float64
	MemoryRequirement int64
}

// FallbackStrategy defines how to handle vectorization failures
type FallbackStrategy int

const (
	FallbackNone FallbackStrategy = iota
	FallbackToScalar
	FallbackToHybrid
	FallbackAdaptive
)

// MakeVectorizationDecision makes an intelligent vectorization decision
func (avd *AdaptiveVectorizationDecider) MakeVectorizationDecision(
	operator LogicalPlan,
	context *PhysicalPlanContext,
) *VectorizationDecision {
	decision := &VectorizationDecision{
		UseVectorized:    false,
		BatchSize:        1024,
		FallbackStrategy: FallbackToScalar,
		Reasoning:        []string{},
		ConfidenceScore:  0.5,
	}

	// Extract operator characteristics
	opType := getOperatorType(operator)
	cardinality := estimateCardinality(operator, context)
	expression := extractExpression(operator)

	// Check memory pressure
	memoryPressure := avd.memoryMonitor.GetMemoryPressure()
	if memoryPressure > avd.config.MemoryPressureThreshold {
		decision.Reasoning = append(decision.Reasoning,
			fmt.Sprintf("High memory pressure: %.2f", memoryPressure))
		decision.ConfidenceScore *= 0.7
		return decision
	}

	// Check cardinality threshold
	if cardinality < avd.config.MinCardinalityForVectorization {
		decision.Reasoning = append(decision.Reasoning,
			fmt.Sprintf("Low cardinality: %d", cardinality))
		decision.ConfidenceScore *= 0.8
		return decision
	}

	// Get cost estimates
	scalarCost := avd.costModel.EstimateScalarCost(opType, cardinality, expression)
	vectorizedCost := avd.costModel.EstimateVectorizedCost(opType, cardinality, expression)

	// Check historical performance
	signature := OperatorSignature{
		OperatorType:  opType,
		ExecutionMode: ExecutionModeVectorized,
		Cardinality:   cardinality,
		HasPredicate:  expression != nil,
	}

	avgTime, avgMemory, _, hasHistory := avd.historyTracker.GetAveragePerformance(signature)

	// Adjust costs based on history
	if hasHistory {
		historicalFactor := 1.0
		if avgTime > time.Millisecond*100 {
			historicalFactor = 1.2 // Penalize slow operations
		}
		vectorizedCost *= historicalFactor

		decision.MemoryRequirement = avgMemory
		decision.ConfidenceScore = 0.8 // Higher confidence with history
	} else {
		decision.MemoryRequirement = int64(float64(cardinality) * 100) // Estimate
		decision.ConfidenceScore = 0.6
	}

	// Calculate speedup
	decision.EstimatedSpeedup = scalarCost / vectorizedCost

	// Make decision
	if decision.EstimatedSpeedup > avd.config.CostDifferenceThreshold {
		decision.UseVectorized = true
		decision.Reasoning = append(decision.Reasoning,
			fmt.Sprintf("Estimated speedup: %.2fx", decision.EstimatedSpeedup))

		// Choose batch size based on memory and cardinality
		decision.BatchSize = avd.chooseBatchSize(cardinality, decision.MemoryRequirement, context)

		// Set fallback strategy
		if avd.config.EnableFallback {
			if cardinality < avd.config.PreferVectorizedUnderThreshold {
				decision.FallbackStrategy = FallbackToHybrid
			} else {
				decision.FallbackStrategy = FallbackAdaptive
			}
		}
	} else {
		decision.Reasoning = append(decision.Reasoning,
			fmt.Sprintf("Insufficient speedup: %.2fx", decision.EstimatedSpeedup))
	}

	return decision
}

// chooseBatchSize selects optimal batch size
func (avd *AdaptiveVectorizationDecider) chooseBatchSize(
	cardinality int64,
	memoryRequirement int64,
	context *PhysicalPlanContext,
) int {
	// Start with default batch size
	batchSize := 1024

	// Adjust based on cardinality
	if cardinality < 10000 {
		batchSize = 512
	} else if cardinality > 1000000 {
		batchSize = 2048
	}

	// Adjust based on available memory
	availableMemory := context.MemoryAvailable -
		int64(avd.memoryMonitor.GetMemoryPressure()*float64(context.MemoryAvailable))

	maxBatchSize := int(availableMemory / (memoryRequirement / cardinality))
	if maxBatchSize < batchSize {
		batchSize = maxBatchSize
	}

	// Ensure minimum batch size
	if batchSize < 64 {
		batchSize = 64
	}

	return batchSize
}

// Helper functions
func bucketCardinality(cardinality int64) int64 {
	if cardinality <= 0 {
		return 0
	}

	power := int64(1)
	for power*10 <= cardinality {
		power *= 10
	}

	return power
}

func getOperatorType(operator LogicalPlan) OperatorType {
	switch operator.(type) {
	case *LogicalScan:
		return OperatorTypeScan
	case *LogicalFilter:
		return OperatorTypeFilter
	case *LogicalJoin:
		return OperatorTypeJoin
	case *LogicalAggregate:
		return OperatorTypeAggregate
	case *LogicalSort:
		return OperatorTypeSort
	case *LogicalLimit:
		return OperatorTypeLimit
	case *LogicalProject:
		return OperatorTypeProjection
	default:
		return OperatorTypeScan // Default
	}
}

func estimateCardinality(operator LogicalPlan, context *PhysicalPlanContext) int64 {
	// This would use table statistics and selectivity estimation
	// For now, return a placeholder
	return 10000
}

func extractExpression(operator LogicalPlan) Expression {
	switch op := operator.(type) {
	case *LogicalFilter:
		return op.Predicate
	case *LogicalJoin:
		return op.Condition
	default:
		return nil
	}
}

// NewAdaptivePhysicalPlanner creates a new adaptive physical planner
func NewAdaptivePhysicalPlanner(
	catalog catalog.Catalog,
	memoryLimit int64,
) *AdaptivePhysicalPlanner {
	// Create base components
	costEstimator := NewCostEstimator(catalog)
	vectorizedModel := NewVectorizedCostModel(true, 1024, memoryLimit, 1000)
	enhancedModel := NewEnhancedVectorizedCostModel(vectorizedModel)

	// Create physical planner
	physicalPlanner := NewPhysicalPlanner(costEstimator, vectorizedModel, catalog)

	// Create adaptive components
	memoryMonitor := NewMemoryPressureMonitor(memoryLimit, 0.8)
	historyTracker := NewOperatorHistoryTracker(1000)
	adaptiveDecider := NewAdaptiveVectorizationDecider(enhancedModel, historyTracker, memoryMonitor)
	feedbackCollector := NewRuntimeFeedbackCollector()

	return &AdaptivePhysicalPlanner{
		PhysicalPlanner:   physicalPlanner,
		memoryMonitor:     memoryMonitor,
		historyTracker:    historyTracker,
		adaptiveDecider:   adaptiveDecider,
		feedbackCollector: feedbackCollector,
	}
}

// GenerateAdaptivePhysicalPlan creates a physical plan with adaptive features
func (app *AdaptivePhysicalPlanner) GenerateAdaptivePhysicalPlan(
	logical LogicalPlan,
	context *PhysicalPlanContext,
) (PhysicalPlan, error) {
	// Update runtime stats
	app.runtimeStats.MemoryPressure = app.memoryMonitor.GetMemoryPressure()

	// Generate base physical plan
	physical, err := app.GeneratePhysicalPlan(logical, context)
	if err != nil {
		return nil, err
	}

	// Apply adaptive optimizations
	optimized := app.applyAdaptiveOptimizations(physical, context)

	return optimized, nil
}

// applyAdaptiveOptimizations applies runtime-aware optimizations
func (app *AdaptivePhysicalPlanner) applyAdaptiveOptimizations(
	plan PhysicalPlan,
	context *PhysicalPlanContext,
) PhysicalPlan {
	// This would traverse the plan tree and apply adaptive decisions
	// For now, return the plan as-is
	return plan
}

// RecordOperatorPerformance records actual operator performance
func (app *AdaptivePhysicalPlanner) RecordOperatorPerformance(
	operator PhysicalPlan,
	executionTime time.Duration,
	memoryUsed int64,
	rowsProduced int64,
	success bool,
) {
	signature := OperatorSignature{
		OperatorType:  operator.GetOperatorType(),
		ExecutionMode: operator.GetExecutionMode(),
		Cardinality:   operator.EstimateCardinality(),
		HasPredicate:  false, // Would need to check operator specifics
	}

	app.historyTracker.RecordExecution(signature, executionTime, memoryUsed, rowsProduced, success)

	// Update memory monitor
	app.memoryMonitor.UpdateMemoryUsage(memoryUsed)

	// Update feedback collector
	app.feedbackCollector.RecordOperatorMetrics(
		signature,
		&PerformanceMetrics{
			ActualExecutionTime: executionTime,
			MemoryUsed:          memoryUsed,
			RowsProcessed:       rowsProduced,
		},
	)
}
