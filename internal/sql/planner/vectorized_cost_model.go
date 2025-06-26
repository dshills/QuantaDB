package planner

import (
	"math"
)

// VectorizedCostModel provides cost estimates for vectorized vs scalar execution
type VectorizedCostModel struct {
	// Configuration parameters
	enableVectorizedExecution   bool
	vectorizedBatchSize         int
	vectorizedMemoryLimit       int64
	vectorizedFallbackThreshold int

	// Cost factors (calibrated from benchmarks)
	vectorizedSetupCost   float64 // Fixed cost to set up vectorized execution
	vectorizedRowCost     float64 // Cost per row for vectorized operations
	scalarRowCost         float64 // Cost per row for scalar operations
	vectorizedBatchFactor float64 // Efficiency factor for batch processing
	memoryPressureFactor  float64 // Cost increase under memory pressure
}

// NewVectorizedCostModel creates a new cost model with calibrated parameters
func NewVectorizedCostModel(enableVectorized bool, batchSize int, memoryLimit int64, fallbackThreshold int) *VectorizedCostModel {
	return &VectorizedCostModel{
		enableVectorizedExecution:   enableVectorized,
		vectorizedBatchSize:         batchSize,
		vectorizedMemoryLimit:       memoryLimit,
		vectorizedFallbackThreshold: fallbackThreshold,
		vectorizedSetupCost:         50.0, // Setup overhead for vectorization
		vectorizedRowCost:           0.8,  // 20% faster than scalar (from benchmarks)
		scalarRowCost:               1.0,  // Baseline scalar cost
		vectorizedBatchFactor:       0.75, // Additional efficiency from batching
		memoryPressureFactor:        1.5,  // 50% cost increase under memory pressure
	}
}

// NewDefaultVectorizedCostModel creates a cost model with default parameters
func NewDefaultVectorizedCostModel() *VectorizedCostModel {
	return NewVectorizedCostModel(
		true,         // enableVectorized
		1024,         // batchSize
		64*1024*1024, // memoryLimit (64MB)
		100,          // fallbackThreshold
	)
}

// EstimateVectorizedScanCost estimates cost for vectorized table scan
func (vcm *VectorizedCostModel) EstimateVectorizedScanCost(rowCount int64, memoryAvailable int64) float64 {
	if !vcm.enableVectorizedExecution {
		return math.Inf(1) // Infinite cost if disabled
	}

	// Don't vectorize small datasets
	if rowCount < int64(vcm.vectorizedFallbackThreshold) {
		return math.Inf(1)
	}

	baseCost := vcm.vectorizedSetupCost + float64(rowCount)*vcm.vectorizedRowCost

	// Apply batch efficiency factor
	batchCount := math.Ceil(float64(rowCount) / float64(vcm.vectorizedBatchSize))
	batchEfficiency := math.Pow(vcm.vectorizedBatchFactor, math.Log(batchCount))
	baseCost *= batchEfficiency

	// Apply memory pressure penalty
	estimatedMemory := vcm.estimateVectorizedMemoryUsage(rowCount)
	if estimatedMemory > memoryAvailable {
		pressureRatio := float64(estimatedMemory) / float64(memoryAvailable)
		baseCost *= math.Pow(vcm.memoryPressureFactor, pressureRatio-1)
	}

	return baseCost
}

// EstimateScalarScanCost estimates cost for scalar table scan
func (vcm *VectorizedCostModel) EstimateScalarScanCost(rowCount int64) float64 {
	return float64(rowCount) * vcm.scalarRowCost
}

// EstimateVectorizedFilterCost estimates cost for vectorized filter operation
func (vcm *VectorizedCostModel) EstimateVectorizedFilterCost(inputRows int64, selectivity float64, expressionComplexity int, memoryAvailable int64) float64 {
	if !vcm.enableVectorizedExecution {
		return math.Inf(1)
	}

	if inputRows < int64(vcm.vectorizedFallbackThreshold) {
		return math.Inf(1)
	}

	// Base cost includes setup + processing
	baseCost := vcm.vectorizedSetupCost + float64(inputRows)*vcm.vectorizedRowCost

	// Expression complexity factor (simple = 1, complex = 2-3)
	complexityFactor := 1.0 + 0.3*float64(expressionComplexity-1)
	baseCost *= complexityFactor

	// Batch efficiency
	batchCount := math.Ceil(float64(inputRows) / float64(vcm.vectorizedBatchSize))
	batchEfficiency := math.Pow(vcm.vectorizedBatchFactor, math.Log(batchCount))
	baseCost *= batchEfficiency

	// Memory pressure
	estimatedMemory := vcm.estimateVectorizedFilterMemoryUsage(inputRows, expressionComplexity)
	if estimatedMemory > memoryAvailable {
		pressureRatio := float64(estimatedMemory) / float64(memoryAvailable)
		baseCost *= math.Pow(vcm.memoryPressureFactor, pressureRatio-1)
	}

	return baseCost
}

// EstimateScalarFilterCost estimates cost for scalar filter operation
func (vcm *VectorizedCostModel) EstimateScalarFilterCost(inputRows int64, selectivity float64, expressionComplexity int) float64 {
	complexityFactor := 1.0 + 0.5*float64(expressionComplexity-1) // Scalar is more affected by complexity
	return float64(inputRows) * vcm.scalarRowCost * complexityFactor
}

// ShouldUseVectorizedScan determines if vectorized scan is more cost-effective
func (vcm *VectorizedCostModel) ShouldUseVectorizedScan(rowCount int64, memoryAvailable int64) bool {
	if !vcm.enableVectorizedExecution {
		return false
	}

	vectorizedCost := vcm.EstimateVectorizedScanCost(rowCount, memoryAvailable)
	scalarCost := vcm.EstimateScalarScanCost(rowCount)

	return vectorizedCost < scalarCost
}

// ShouldUseVectorizedFilter determines if vectorized filter is more cost-effective
func (vcm *VectorizedCostModel) ShouldUseVectorizedFilter(inputRows int64, selectivity float64, expressionComplexity int, memoryAvailable int64) bool {
	if !vcm.enableVectorizedExecution {
		return false
	}

	vectorizedCost := vcm.EstimateVectorizedFilterCost(inputRows, selectivity, expressionComplexity, memoryAvailable)
	scalarCost := vcm.EstimateScalarFilterCost(inputRows, selectivity, expressionComplexity)

	return vectorizedCost < scalarCost
}

// GetExpressionComplexity estimates complexity score for an expression
func (vcm *VectorizedCostModel) GetExpressionComplexity(expr Expression) int {
	switch e := expr.(type) {
	case *ColumnRef:
		return 1
	case *Literal:
		return 1
	case *BinaryOp:
		leftComplexity := vcm.GetExpressionComplexity(e.Left)
		rightComplexity := vcm.GetExpressionComplexity(e.Right)
		operatorComplexity := vcm.getOperatorComplexity(e.Operator)
		return leftComplexity + rightComplexity + operatorComplexity
	case *UnaryOp:
		return vcm.GetExpressionComplexity(e.Expr) + 1
	case *FunctionCall:
		// Function calls are typically more expensive
		baseComplexity := 3
		for _, arg := range e.Args {
			baseComplexity += vcm.GetExpressionComplexity(arg)
		}
		return baseComplexity
	default:
		return 2 // Default complexity for unknown expressions
	}
}

// getOperatorComplexity returns complexity score for binary operators
func (vcm *VectorizedCostModel) getOperatorComplexity(op BinaryOperator) int {
	switch op {
	case OpAdd, OpSubtract:
		return 1
	case OpMultiply:
		return 2
	case OpDivide:
		return 3 // Division is more expensive
	case OpEqual, OpNotEqual, OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		return 1
	case OpAnd, OpOr:
		return 1
	default:
		return 2
	}
}

// estimateVectorizedMemoryUsage estimates memory usage for vectorized scan
func (vcm *VectorizedCostModel) estimateVectorizedMemoryUsage(rowCount int64) int64 {
	// Estimate based on batch size and typical row size
	batchSize := int64(vcm.vectorizedBatchSize)
	avgRowSize := int64(128) // Estimated average row size in bytes

	// Memory for input batch + output batch + null bitmaps
	batchMemory := batchSize * avgRowSize * 2 // Input + output
	bitmapMemory := batchSize / 8             // Null bitmaps (1 bit per value)

	return batchMemory + bitmapMemory
}

// estimateVectorizedFilterMemoryUsage estimates memory usage for vectorized filter
func (vcm *VectorizedCostModel) estimateVectorizedFilterMemoryUsage(inputRows int64, expressionComplexity int) int64 {
	baseMemory := vcm.estimateVectorizedMemoryUsage(inputRows)

	// Complex expressions need intermediate results
	complexityFactor := 1.0 + 0.5*float64(expressionComplexity-1)

	return int64(float64(baseMemory) * complexityFactor)
}

// VectorizedPlanChoice represents a choice between vectorized and scalar execution
type VectorizedPlanChoice struct {
	UseVectorized  bool
	VectorizedCost float64
	ScalarCost     float64
	Reason         string
	MemoryEstimate int64
	BatchCount     int64
}

// ChooseExecutionMode determines the best execution mode for an operation
func (vcm *VectorizedCostModel) ChooseExecutionMode(
	operationType string,
	rowCount int64,
	selectivity float64,
	expressionComplexity int,
	memoryAvailable int64,
) *VectorizedPlanChoice {
	var vectorizedCost, scalarCost float64
	var memoryEstimate int64

	switch operationType {
	case "scan":
		vectorizedCost = vcm.EstimateVectorizedScanCost(rowCount, memoryAvailable)
		scalarCost = vcm.EstimateScalarScanCost(rowCount)
		memoryEstimate = vcm.estimateVectorizedMemoryUsage(rowCount)
	case "filter":
		vectorizedCost = vcm.EstimateVectorizedFilterCost(rowCount, selectivity, expressionComplexity, memoryAvailable)
		scalarCost = vcm.EstimateScalarFilterCost(rowCount, selectivity, expressionComplexity)
		memoryEstimate = vcm.estimateVectorizedFilterMemoryUsage(rowCount, expressionComplexity)
	default:
		return &VectorizedPlanChoice{
			UseVectorized:  false,
			VectorizedCost: math.Inf(1),
			ScalarCost:     scalarCost,
			Reason:         "Unsupported operation type for vectorization",
		}
	}

	useVectorized := vectorizedCost < scalarCost
	reason := ""

	if !vcm.enableVectorizedExecution {
		useVectorized = false
		reason = "Vectorized execution disabled by configuration"
	} else if rowCount < int64(vcm.vectorizedFallbackThreshold) {
		useVectorized = false
		reason = "Row count below vectorization threshold"
	} else if memoryEstimate > memoryAvailable {
		if vectorizedCost < scalarCost*1.5 { // Still use if only 50% more expensive
			reason = "Vectorized chosen despite memory pressure"
		} else {
			useVectorized = false
			reason = "Memory pressure too high for vectorization"
		}
	} else if useVectorized {
		_ = (scalarCost - vectorizedCost) / scalarCost * 100
		reason = "Vectorized execution more efficient for this workload"
	} else {
		reason = "Scalar execution more efficient for this workload"
	}

	batchCount := int64(math.Ceil(float64(rowCount) / float64(vcm.vectorizedBatchSize)))

	return &VectorizedPlanChoice{
		UseVectorized:  useVectorized,
		VectorizedCost: vectorizedCost,
		ScalarCost:     scalarCost,
		Reason:         reason,
		MemoryEstimate: memoryEstimate,
		BatchCount:     batchCount,
	}
}
