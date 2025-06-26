package planner

import (
	"math"
)

// EnhancedVectorizedCostModel extends VectorizedCostModel with per-operator decisions
type EnhancedVectorizedCostModel struct {
	*VectorizedCostModel

	// Per-operator vectorization parameters
	scanVectorizationThreshold   int64
	filterVectorizationThreshold int64
	joinVectorizationThreshold   int64
	aggVectorizationThreshold    int64
	sortVectorizationThreshold   int64

	// Expression complexity factors
	arithmeticSpeedup   float64
	comparisonSpeedup   float64
	functionCallPenalty float64

	// Memory pressure handling
	memoryPressureThresholds map[MemoryPressureLevel]float64
	vectorizedMemoryOverhead float64

	// Adaptive parameters
	historicalWeight    float64
	confidenceThreshold float64
}

// MemoryPressureLevel categorizes memory pressure
type MemoryPressureLevel int

const (
	MemoryPressureLow MemoryPressureLevel = iota
	MemoryPressureMedium
	MemoryPressureHigh
	MemoryPressureCritical
)

// OperatorCostEstimate provides detailed cost breakdown
type OperatorCostEstimate struct {
	ScalarCost        float64
	VectorizedCost    float64
	MemoryRequirement int64
	RecommendedMode   ExecutionMode
	ConfidenceLevel   float64
	CostBreakdown     *CostBreakdown
}

// CostBreakdown provides detailed cost components
type CostBreakdown struct {
	StartupCost      float64
	PerRowCost       float64
	MemoryAccessCost float64
	ComputeCost      float64
	SIMDSavings      float64
}

// NewEnhancedVectorizedCostModel creates an enhanced cost model
func NewEnhancedVectorizedCostModel(base *VectorizedCostModel) *EnhancedVectorizedCostModel {
	model := &EnhancedVectorizedCostModel{
		VectorizedCostModel:          base,
		scanVectorizationThreshold:   1000,
		filterVectorizationThreshold: 800,
		joinVectorizationThreshold:   5000,
		aggVectorizationThreshold:    2000,
		sortVectorizationThreshold:   3000,
		arithmeticSpeedup:            2.5,
		comparisonSpeedup:            2.2,
		functionCallPenalty:          0.7,
		memoryPressureThresholds:     make(map[MemoryPressureLevel]float64),
		vectorizedMemoryOverhead:     1.2,
		historicalWeight:             0.3,
		confidenceThreshold:          0.7,
	}

	// Initialize memory pressure thresholds
	model.memoryPressureThresholds[MemoryPressureLow] = 0.3
	model.memoryPressureThresholds[MemoryPressureMedium] = 0.6
	model.memoryPressureThresholds[MemoryPressureHigh] = 0.8
	model.memoryPressureThresholds[MemoryPressureCritical] = 0.95

	return model
}

// EstimateOperatorCost estimates cost for a specific operator
func (evcm *EnhancedVectorizedCostModel) EstimateOperatorCost(
	opType OperatorType,
	inputCardinality int64,
	selectivity float64,
	expression Expression,
	memoryAvailable int64,
) *OperatorCostEstimate {
	estimate := &OperatorCostEstimate{
		ConfidenceLevel: 0.8,
		CostBreakdown:   &CostBreakdown{},
	}

	// Calculate base costs
	switch opType {
	case OperatorTypeScan:
		evcm.estimateScanCost(estimate, inputCardinality, expression, memoryAvailable)
	case OperatorTypeFilter:
		evcm.estimateFilterCost(estimate, inputCardinality, selectivity, expression, memoryAvailable)
	case OperatorTypeJoin:
		evcm.estimateJoinCost(estimate, inputCardinality, selectivity, expression, memoryAvailable)
	case OperatorTypeAggregate:
		evcm.estimateAggregateCost(estimate, inputCardinality, selectivity, expression, memoryAvailable)
	case OperatorTypeSort:
		evcm.estimateSortCost(estimate, inputCardinality, expression, memoryAvailable)
	case OperatorTypeProjection:
		evcm.estimateProjectionCost(estimate, inputCardinality, expression, memoryAvailable)
	default:
		evcm.estimateDefaultCost(estimate, inputCardinality, expression, memoryAvailable)
	}

	// Determine recommended execution mode
	evcm.determineRecommendedMode(estimate, opType, inputCardinality, memoryAvailable)

	return estimate
}

// estimateScanCost estimates cost for table scans
func (evcm *EnhancedVectorizedCostModel) estimateScanCost(
	estimate *OperatorCostEstimate,
	cardinality int64,
	predicate Expression,
	memoryAvailable int64,
) {
	// Base I/O cost (same for both modes)
	ioCost := float64(cardinality) * 1.0 // Default page cost

	// Scalar cost
	estimate.ScalarCost = ioCost + float64(cardinality)*evcm.VectorizedCostModel.scalarRowCost
	estimate.CostBreakdown.StartupCost = 0
	estimate.CostBreakdown.PerRowCost = evcm.VectorizedCostModel.scalarRowCost

	// Vectorized cost
	if cardinality >= evcm.scanVectorizationThreshold {
		_ = math.Ceil(float64(cardinality) / float64(evcm.VectorizedCostModel.vectorizedBatchSize))
		vectorizedProcessingCost := float64(cardinality) * evcm.VectorizedCostModel.vectorizedRowCost

		estimate.VectorizedCost = ioCost + evcm.VectorizedCostModel.vectorizedSetupCost + vectorizedProcessingCost
		estimate.CostBreakdown.SIMDSavings = estimate.ScalarCost - estimate.VectorizedCost

		// Memory requirement
		estimate.MemoryRequirement = int64(evcm.VectorizedCostModel.vectorizedBatchSize) * 100 // Rough estimate
	} else {
		estimate.VectorizedCost = estimate.ScalarCost * 1.1 // Slight overhead for small data
		estimate.MemoryRequirement = cardinality * 50
	}

	// Apply predicate complexity if exists
	if predicate != nil {
		predicateFactor := evcm.analyzePredicateComplexity(predicate)
		estimate.ScalarCost *= predicateFactor
		estimate.VectorizedCost *= predicateFactor * 0.8 // Vectorization helps with predicates
	}
}

// estimateFilterCost estimates cost for filter operations
func (evcm *EnhancedVectorizedCostModel) estimateFilterCost(
	estimate *OperatorCostEstimate,
	inputCardinality int64,
	selectivity float64,
	predicate Expression,
	memoryAvailable int64,
) {
	// Filter doesn't have I/O cost (processes input stream)

	// Scalar cost
	predicateEvalCost := evcm.VectorizedCostModel.scalarRowCost * 0.5 // Filter is cheaper than full row processing
	if predicate != nil {
		predicateEvalCost *= evcm.analyzePredicateComplexity(predicate)
	}
	estimate.ScalarCost = float64(inputCardinality) * predicateEvalCost

	// Vectorized cost
	if inputCardinality >= evcm.filterVectorizationThreshold {
		_ = math.Ceil(float64(inputCardinality) / float64(evcm.VectorizedCostModel.vectorizedBatchSize))

		// Vectorized predicate evaluation is more efficient
		vectorizedPredicateCost := float64(inputCardinality) * evcm.VectorizedCostModel.vectorizedRowCost * 0.3
		if predicate != nil {
			complexity := evcm.analyzePredicateComplexity(predicate)
			vectorizedPredicateCost *= complexity * 0.7 // Better than scalar
		}

		estimate.VectorizedCost = evcm.VectorizedCostModel.vectorizedSetupCost + vectorizedPredicateCost
		estimate.CostBreakdown.SIMDSavings = estimate.ScalarCost - estimate.VectorizedCost

		// Memory for batch processing
		estimate.MemoryRequirement = int64(evcm.VectorizedCostModel.vectorizedBatchSize) * 80
	} else {
		estimate.VectorizedCost = estimate.ScalarCost * 1.05
		estimate.MemoryRequirement = inputCardinality * 40
	}

	// Adjust confidence based on selectivity
	if selectivity < 0.1 || selectivity > 0.9 {
		estimate.ConfidenceLevel *= 0.9 // Extreme selectivities are harder to predict
	}
}

// estimateJoinCost estimates cost for join operations
func (evcm *EnhancedVectorizedCostModel) estimateJoinCost(
	estimate *OperatorCostEstimate,
	leftCardinality int64,
	joinSelectivity float64,
	condition Expression,
	memoryAvailable int64,
) {
	// Assume hash join for now
	rightCardinality := leftCardinality // Simplified assumption

	// Build phase cost
	buildCost := float64(rightCardinality) * evcm.VectorizedCostModel.scalarRowCost * 2.0 // Hash table build

	// Probe phase cost
	probeCost := float64(leftCardinality) * evcm.VectorizedCostModel.scalarRowCost * 1.5

	// Scalar total
	estimate.ScalarCost = buildCost + probeCost

	// Vectorized cost
	totalRows := leftCardinality + rightCardinality
	if totalRows >= evcm.joinVectorizationThreshold {
		// Vectorized hash table operations
		_ = math.Ceil(float64(rightCardinality) / float64(evcm.VectorizedCostModel.vectorizedBatchSize))
		_ = math.Ceil(float64(leftCardinality) / float64(evcm.VectorizedCostModel.vectorizedBatchSize))

		vectorizedBuildCost := float64(rightCardinality) * evcm.VectorizedCostModel.vectorizedRowCost * 1.5
		vectorizedProbeCost := float64(leftCardinality) * evcm.VectorizedCostModel.vectorizedRowCost * 1.2

		estimate.VectorizedCost = evcm.VectorizedCostModel.vectorizedSetupCost*2 + vectorizedBuildCost + vectorizedProbeCost

		// Memory for hash table and batches
		hashTableSize := rightCardinality * 100
		batchMemory := int64(evcm.VectorizedCostModel.vectorizedBatchSize) * 200
		estimate.MemoryRequirement = hashTableSize + batchMemory
	} else {
		estimate.VectorizedCost = estimate.ScalarCost * 1.1
		estimate.MemoryRequirement = rightCardinality * 100
	}

	// Apply join condition complexity
	if condition != nil {
		conditionFactor := evcm.analyzePredicateComplexity(condition)
		estimate.ScalarCost *= conditionFactor
		estimate.VectorizedCost *= conditionFactor * 0.85
	}
}

// estimateAggregateCost estimates cost for aggregation operations
func (evcm *EnhancedVectorizedCostModel) estimateAggregateCost(
	estimate *OperatorCostEstimate,
	inputCardinality int64,
	groupingFactor float64,
	expression Expression,
	memoryAvailable int64,
) {
	// Hash aggregation cost model
	numGroups := int64(float64(inputCardinality) * groupingFactor)
	if numGroups < 1 {
		numGroups = 1
	}

	// Scalar cost: build hash table + aggregate computation
	hashTableCost := float64(inputCardinality) * evcm.VectorizedCostModel.scalarRowCost * 1.5
	aggregateCost := float64(inputCardinality) * evcm.VectorizedCostModel.scalarRowCost * 0.5
	estimate.ScalarCost = hashTableCost + aggregateCost

	// Vectorized cost
	if inputCardinality >= evcm.aggVectorizationThreshold {
		_ = math.Ceil(float64(inputCardinality) / float64(evcm.VectorizedCostModel.vectorizedBatchSize))

		// Vectorized aggregation is very efficient
		vectorizedAggCost := float64(inputCardinality) * evcm.VectorizedCostModel.vectorizedRowCost * 0.8
		estimate.VectorizedCost = evcm.VectorizedCostModel.vectorizedSetupCost + vectorizedAggCost

		// Memory for hash table and intermediate results
		estimate.MemoryRequirement = numGroups*150 + int64(evcm.VectorizedCostModel.vectorizedBatchSize)*100
	} else {
		estimate.VectorizedCost = estimate.ScalarCost * 1.05
		estimate.MemoryRequirement = numGroups * 150
	}
}

// estimateSortCost estimates cost for sort operations
func (evcm *EnhancedVectorizedCostModel) estimateSortCost(
	estimate *OperatorCostEstimate,
	cardinality int64,
	sortKeys Expression,
	memoryAvailable int64,
) {
	// Sort cost: O(n log n)
	logFactor := math.Log2(float64(cardinality))
	if logFactor < 1 {
		logFactor = 1
	}

	// Scalar cost
	compareCost := evcm.VectorizedCostModel.scalarRowCost * 0.3
	estimate.ScalarCost = float64(cardinality) * logFactor * compareCost

	// Vectorized cost
	if cardinality >= evcm.sortVectorizationThreshold {
		// Vectorized sorting can use SIMD for comparisons
		_ = math.Ceil(float64(cardinality) / float64(evcm.VectorizedCostModel.vectorizedBatchSize))
		vectorizedCompareCost := float64(cardinality) * evcm.VectorizedCostModel.vectorizedRowCost * 0.4 * logFactor

		estimate.VectorizedCost = evcm.VectorizedCostModel.vectorizedSetupCost + vectorizedCompareCost

		// Memory for sort buffer
		estimate.MemoryRequirement = cardinality * 120
	} else {
		estimate.VectorizedCost = estimate.ScalarCost * 1.02
		estimate.MemoryRequirement = cardinality * 100
	}
}

// estimateProjectionCost estimates cost for projection operations
func (evcm *EnhancedVectorizedCostModel) estimateProjectionCost(
	estimate *OperatorCostEstimate,
	cardinality int64,
	projections Expression,
	memoryAvailable int64,
) {
	// Base projection cost
	numProjections := 5 // Default, would count actual projections
	projectionComplexity := 1.0

	if projections != nil {
		projectionComplexity = evcm.analyzeExpressionComplexity(projections)
	}

	// Scalar cost
	perRowCost := evcm.VectorizedCostModel.scalarRowCost * 0.2 * float64(numProjections) * projectionComplexity
	estimate.ScalarCost = float64(cardinality) * perRowCost

	// Vectorized cost
	if cardinality >= 1000 { // Lower threshold for projections
		_ = math.Ceil(float64(cardinality) / float64(evcm.VectorizedCostModel.vectorizedBatchSize))

		// Vectorized expression evaluation
		vectorizedExprCost := float64(cardinality) * evcm.VectorizedCostModel.vectorizedRowCost * 0.15 * float64(numProjections) * projectionComplexity * 0.7
		estimate.VectorizedCost = evcm.VectorizedCostModel.vectorizedSetupCost + vectorizedExprCost

		estimate.MemoryRequirement = int64(evcm.VectorizedCostModel.vectorizedBatchSize) * 50 * int64(numProjections)
	} else {
		estimate.VectorizedCost = estimate.ScalarCost * 1.01
		estimate.MemoryRequirement = cardinality * 50
	}
}

// estimateDefaultCost provides default cost estimation
func (evcm *EnhancedVectorizedCostModel) estimateDefaultCost(
	estimate *OperatorCostEstimate,
	cardinality int64,
	expression Expression,
	memoryAvailable int64,
) {
	// Default scalar cost
	estimate.ScalarCost = float64(cardinality) * evcm.VectorizedCostModel.scalarRowCost

	// Default vectorized cost
	if cardinality >= 1000 {
		_ = math.Ceil(float64(cardinality) / float64(evcm.VectorizedCostModel.vectorizedBatchSize))
		estimate.VectorizedCost = evcm.VectorizedCostModel.vectorizedSetupCost + float64(cardinality)*evcm.VectorizedCostModel.vectorizedRowCost
		estimate.MemoryRequirement = int64(evcm.VectorizedCostModel.vectorizedBatchSize) * 100
	} else {
		estimate.VectorizedCost = estimate.ScalarCost * 1.05
		estimate.MemoryRequirement = cardinality * 80
	}
}

// analyzePredicateComplexity analyzes predicate complexity
func (evcm *EnhancedVectorizedCostModel) analyzePredicateComplexity(predicate Expression) float64 {
	// Simplified complexity analysis
	var complexity float64

	switch p := predicate.(type) {
	case *BinaryOp:
		// Different operators have different costs
		switch p.Operator {
		case OpEqual, OpNotEqual:
			complexity = 1.0
		case OpLess, OpGreater, OpLessEqual, OpGreaterEqual:
			complexity = 1.1
		case OpLike, OpNotLike:
			complexity = 3.0 // Pattern matching is expensive
		case OpIn, OpNotIn:
			complexity = 2.0
		default:
			complexity = 1.5
		}

		// Recursively analyze operands
		leftComplexity := evcm.analyzeExpressionComplexity(p.Left)
		rightComplexity := evcm.analyzeExpressionComplexity(p.Right)
		complexity *= (leftComplexity + rightComplexity) / 2

	case *FunctionCall:
		// Function calls are more expensive
		complexity = 2.5 + float64(len(p.Args))*0.5

	case *SubqueryExpr:
		// Subqueries are very expensive
		complexity = 10.0

	case *CaseExpr:
		// CASE expressions involve branching
		complexity = 2.0 + float64(len(p.WhenList))*0.5

	default:
		complexity = 1.0
	}

	return complexity
}

// analyzeExpressionComplexity analyzes general expression complexity
func (evcm *EnhancedVectorizedCostModel) analyzeExpressionComplexity(expr Expression) float64 {
	// This is similar to predicate complexity but for general expressions
	return evcm.analyzePredicateComplexity(expr)
}

// determineRecommendedMode determines the recommended execution mode
func (evcm *EnhancedVectorizedCostModel) determineRecommendedMode(
	estimate *OperatorCostEstimate,
	opType OperatorType,
	cardinality int64,
	memoryAvailable int64,
) {
	// Check if we have enough memory for vectorization
	if estimate.MemoryRequirement > memoryAvailable/2 {
		estimate.RecommendedMode = ExecutionModeScalar
		estimate.ConfidenceLevel *= 0.8
		return
	}

	// Calculate cost ratio
	costRatio := estimate.ScalarCost / estimate.VectorizedCost

	// Operator-specific thresholds
	var threshold float64
	switch opType {
	case OperatorTypeScan:
		threshold = 1.2
	case OperatorTypeFilter:
		threshold = 1.3
	case OperatorTypeJoin:
		threshold = 1.5
	case OperatorTypeAggregate:
		threshold = 1.4
	case OperatorTypeSort:
		threshold = 1.3
	default:
		threshold = 1.25
	}

	// Make recommendation
	if costRatio > threshold && cardinality >= evcm.getOperatorThreshold(opType) {
		estimate.RecommendedMode = ExecutionModeVectorized
	} else if costRatio > 1.1 && cardinality >= evcm.getOperatorThreshold(opType)/2 {
		estimate.RecommendedMode = ExecutionModeHybrid
	} else {
		estimate.RecommendedMode = ExecutionModeScalar
	}

	// Adjust confidence based on cost difference
	if math.Abs(estimate.ScalarCost-estimate.VectorizedCost) < 0.1*estimate.ScalarCost {
		estimate.ConfidenceLevel *= 0.7 // Low confidence when costs are similar
	}
}

// getOperatorThreshold returns the cardinality threshold for an operator
func (evcm *EnhancedVectorizedCostModel) getOperatorThreshold(opType OperatorType) int64 {
	switch opType {
	case OperatorTypeScan:
		return evcm.scanVectorizationThreshold
	case OperatorTypeFilter:
		return evcm.filterVectorizationThreshold
	case OperatorTypeJoin:
		return evcm.joinVectorizationThreshold
	case OperatorTypeAggregate:
		return evcm.aggVectorizationThreshold
	case OperatorTypeSort:
		return evcm.sortVectorizationThreshold
	default:
		return 1000
	}
}

// EstimateScalarCost estimates scalar execution cost
func (evcm *EnhancedVectorizedCostModel) EstimateScalarCost(
	opType OperatorType,
	cardinality int64,
	expression Expression,
) float64 {
	estimate := evcm.EstimateOperatorCost(opType, cardinality, 1.0, expression, math.MaxInt64)
	return estimate.ScalarCost
}

// EstimateVectorizedCost estimates vectorized execution cost
func (evcm *EnhancedVectorizedCostModel) EstimateVectorizedCost(
	opType OperatorType,
	cardinality int64,
	expression Expression,
) float64 {
	estimate := evcm.EstimateOperatorCost(opType, cardinality, 1.0, expression, math.MaxInt64)
	return estimate.VectorizedCost
}
