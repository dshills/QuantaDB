package executor

import (
	"fmt"
	"log"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// VectorizedExprEvaluatorWithFallback wraps an expression evaluator with fallback support
type VectorizedExprEvaluatorWithFallback struct {
	expr               planner.Expression
	vectorizedEval     VectorizedExprEvaluator
	supportsVectorized bool
	fallbackCount      int64
}

// NewVectorizedExprEvaluatorWithFallback creates an evaluator with fallback capability
func NewVectorizedExprEvaluatorWithFallback(expr planner.Expression) *VectorizedExprEvaluatorWithFallback {
	evaluator := &VectorizedExprEvaluatorWithFallback{
		expr: expr,
	}

	// Try to create vectorized evaluator
	if vectorizedEval, supported := tryCreateVectorizedEvaluator(expr); supported {
		evaluator.vectorizedEval = vectorizedEval
		evaluator.supportsVectorized = true
	} else {
		evaluator.supportsVectorized = false
	}

	return evaluator
}

// EvalVector evaluates the expression on a batch, falling back to row-at-a-time if needed
func (e *VectorizedExprEvaluatorWithFallback) EvalVector(batch *VectorizedBatch) (*Vector, error) {
	// Try vectorized evaluation first
	if e.supportsVectorized && e.vectorizedEval != nil {
		result, err := e.vectorizedEval.EvalVector(batch)
		if err == nil {
			return result, nil
		}

		// Log fallback for monitoring
		if e.fallbackCount == 0 {
			log.Printf("Vectorized evaluation failed, falling back to row-at-a-time for expression: %v", e.expr)
		}
		e.fallbackCount++
	}

	// Fallback to row-at-a-time evaluation
	return e.evalRowAtATime(batch)
}

// evalRowAtATime performs row-at-a-time evaluation on a batch
func (e *VectorizedExprEvaluatorWithFallback) evalRowAtATime(batch *VectorizedBatch) (*Vector, error) {
	resultVector := NewVector(types.Boolean, batch.RowCount)
	resultVector.Length = batch.RowCount

	// Create a temporary row for evaluation
	row := &Row{
		Values: make([]types.Value, len(batch.Schema.Columns)),
	}

	// Evaluate expression for each row
	for i := 0; i < batch.RowCount; i++ {
		// Extract row from batch
		for colIdx, vec := range batch.Vectors {
			row.Values[colIdx] = extractValueFromVector(vec, i)
		}

		// Build and evaluate expression
		evaluator, err := buildExprEvaluator(e.expr)
		if err != nil {
			return nil, fmt.Errorf("failed to build evaluator: %w", err)
		}

		result, err := evaluator.Eval(row, nil)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}

		// Set result in vector
		if result.IsNull() {
			resultVector.SetNull(i)
		} else if boolVal, ok := result.Data.(bool); ok {
			resultVector.BoolData[i] = boolVal
		} else {
			return nil, fmt.Errorf("expression did not return boolean at row %d", i)
		}
	}

	return resultVector, nil
}

// tryCreateVectorizedEvaluator attempts to create a vectorized evaluator for an expression
func tryCreateVectorizedEvaluator(expr planner.Expression) (VectorizedExprEvaluator, bool) {
	switch e := expr.(type) {
	case *planner.BinaryOp:
		// Check if operator is supported
		if isVectorizedOperatorSupported(e.Operator) {
			leftEval, leftOk := tryCreateVectorizedEvaluator(e.Left)
			rightEval, rightOk := tryCreateVectorizedEvaluator(e.Right)
			if leftOk && rightOk {
				// Determine result type based on operator
				resultType := e.Left.DataType()
				if isComparisonOp(e.Operator) {
					resultType = types.Boolean
				}
				return NewVectorizedBinaryOpEvaluator(leftEval, rightEval, e.Operator, resultType), true
			}
		}
		return nil, false

	case *planner.ColumnRef:
		// For column ref, we need the column index which isn't available here
		// This would need to be resolved during planning
		return nil, false

	case *planner.Literal:
		return NewVectorizedLiteralEvaluator(e.Value), true

	case *planner.UnaryOp:
		// Unary operators not yet supported in vectorized execution
		return nil, false

	case *planner.FunctionCall:
		// User-defined functions not supported in vectorized mode
		return nil, false

	case *planner.CaseExpr:
		// CASE expressions not yet vectorized
		return nil, false

	case *planner.SubqueryExpr:
		// Subqueries cannot be vectorized
		return nil, false

	default:
		// Unknown expression type
		return nil, false
	}
}

// isVectorizedOperatorSupported checks if an operator supports vectorized execution
func isVectorizedOperatorSupported(op planner.BinaryOperator) bool {
	switch op {
	case planner.OpAdd, planner.OpSubtract, planner.OpMultiply, planner.OpDivide,
		planner.OpEqual, planner.OpNotEqual, planner.OpLess, planner.OpLessEqual,
		planner.OpGreater, planner.OpGreaterEqual, planner.OpAnd, planner.OpOr:
		return true
	default:
		return false
	}
}

// isComparisonOp checks if an operator is a comparison operator
func isComparisonOp(op planner.BinaryOperator) bool {
	switch op {
	case planner.OpEqual, planner.OpNotEqual, planner.OpLess, planner.OpLessEqual,
		planner.OpGreater, planner.OpGreaterEqual:
		return true
	default:
		return false
	}
}

// extractValueFromVector extracts a single value from a vector at the given index
func extractValueFromVector(vector *Vector, index int) types.Value {
	if vector.IsNull(index) {
		return types.NewNullValue()
	}

	var data interface{}
	switch vector.DataType {
	case types.Integer:
		data = vector.Int32Data[index]
	case types.BigInt:
		data = vector.Int64Data[index]
	case types.Float:
		data = vector.Float32Data[index]
	case types.Double:
		data = vector.Float64Data[index]
	case types.Boolean:
		data = vector.BoolData[index]
	case types.Text, types.Varchar(0):
		data = vector.StringData[index]
	case types.Bytea:
		data = vector.BytesData[index]
	default:
		// Handle other types
		data = fmt.Sprintf("unsupported type %v", vector.DataType)
	}

	return types.NewValue(data)
}

// VectorizedFilterOperatorWithFallback enhances the filter operator with fallback support
type VectorizedFilterOperatorWithFallback struct {
	*VectorizedFilterOperator
	fallbackEvaluator *VectorizedExprEvaluatorWithFallback
	stats             VectorizedStats
}

// VectorizedStats tracks vectorized execution statistics
type VectorizedStats struct {
	VectorizedBatches int64
	FallbackBatches   int64
	TotalRows         int64
	FilteredRows      int64
}

// NewVectorizedFilterOperatorWithFallback creates a filter operator with fallback support
func NewVectorizedFilterOperatorWithFallback(child VectorizedOperator, predicate planner.Expression) *VectorizedFilterOperatorWithFallback {
	evaluator := NewVectorizedExprEvaluatorWithFallback(predicate)

	baseFilter := &VectorizedFilterOperator{
		baseOperator: baseOperator{schema: child.Schema()},
		child:        child,
		predicate:    evaluator,
		batch:        NewVectorizedBatch(child.Schema(), VectorSize),
	}

	return &VectorizedFilterOperatorWithFallback{
		VectorizedFilterOperator: baseFilter,
		fallbackEvaluator:        evaluator,
	}
}

// GetStatistics returns execution statistics
func (vf *VectorizedFilterOperatorWithFallback) GetStatistics() VectorizedStats {
	stats := vf.stats
	if vf.fallbackEvaluator.fallbackCount > 0 {
		stats.FallbackBatches = vf.fallbackEvaluator.fallbackCount
	}
	return stats
}

// NextBatch processes the next batch with statistics tracking
func (vf *VectorizedFilterOperatorWithFallback) NextBatch() (*VectorizedBatch, error) {
	batch, err := vf.VectorizedFilterOperator.NextBatch()
	if err != nil {
		return nil, err
	}

	if batch != nil {
		vf.stats.TotalRows += int64(batch.RowCount)
		if vf.fallbackEvaluator.supportsVectorized {
			vf.stats.VectorizedBatches++
		} else {
			vf.stats.FallbackBatches++
		}

		// Count filtered rows
		filteredCount := 0
		for i := 0; i < batch.RowCount; i++ {
			if batch.Vectors[0].Selection != nil && batch.Vectors[0].SelLength > 0 {
				filteredCount = batch.Vectors[0].SelLength
				break
			} else {
				filteredCount = batch.RowCount
			}
		}
		vf.stats.FilteredRows += int64(filteredCount)
	}

	return batch, nil
}
