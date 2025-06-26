package executor

import (
	"cmp"
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// VectorizedBinaryOpEvaluator evaluates binary operations on vectors
type VectorizedBinaryOpEvaluator struct {
	left     VectorizedExprEvaluator
	right    VectorizedExprEvaluator
	operator planner.BinaryOperator
	dataType types.DataType
}

// NewVectorizedBinaryOpEvaluator creates a new vectorized binary operator
func NewVectorizedBinaryOpEvaluator(left, right VectorizedExprEvaluator, op planner.BinaryOperator, dataType types.DataType) *VectorizedBinaryOpEvaluator {
	return &VectorizedBinaryOpEvaluator{
		left:     left,
		right:    right,
		operator: op,
		dataType: dataType,
	}
}

// EvalVector evaluates the binary operation on a batch
func (vb *VectorizedBinaryOpEvaluator) EvalVector(batch *VectorizedBatch) (*Vector, error) {
	// Evaluate left and right operands
	leftVec, err := vb.left.EvalVector(batch)
	if err != nil {
		return nil, err
	}

	rightVec, err := vb.right.EvalVector(batch)
	if err != nil {
		return nil, err
	}

	// Create result vector
	var result *Vector

	switch vb.operator {
	case planner.OpEqual, planner.OpNotEqual, planner.OpLess, planner.OpLessEqual,
		planner.OpGreater, planner.OpGreaterEqual:
		// Comparison operations return boolean
		result = NewVector(types.Boolean, batch.RowCount)
		err = vb.evalComparison(leftVec, rightVec, result, batch.RowCount)

	case planner.OpAdd, planner.OpSubtract, planner.OpMultiply, planner.OpDivide:
		// Arithmetic operations return same type as operands
		result = NewVector(vb.dataType, batch.RowCount)
		err = vb.evalArithmetic(leftVec, rightVec, result, batch.RowCount)

	case planner.OpAnd, planner.OpOr:
		// Logical operations on boolean vectors
		result = NewVector(types.Boolean, batch.RowCount)
		err = vb.evalLogical(leftVec, rightVec, result, batch.RowCount)

	default:
		return nil, fmt.Errorf("unsupported vectorized operator: %v", vb.operator)
	}

	if err != nil {
		return nil, err
	}

	result.Length = batch.RowCount
	return result, nil
}

// evalComparison evaluates comparison operations
func (vb *VectorizedBinaryOpEvaluator) evalComparison(left, right, result *Vector, length int) error {
	// Handle different data types
	switch left.DataType {
	case types.Integer:
		return vb.evalComparisonInt32(left.Int32Data, right.Int32Data, result.BoolData, left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	case types.BigInt:
		return vb.evalComparisonInt64(left.Int64Data, right.Int64Data, result.BoolData, left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	case types.Float:
		return vb.evalComparisonFloat32(left.Float32Data, right.Float32Data, result.BoolData, left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	case types.Double:
		return vb.evalComparisonFloat64(left.Float64Data, right.Float64Data, result.BoolData, left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	default:
		return fmt.Errorf("unsupported type for vectorized comparison: %v", left.DataType)
	}
}

// evalComparisonInt64 performs vectorized comparison on int64 values
func (vb *VectorizedBinaryOpEvaluator) evalComparisonInt64(left, right []int64, result []bool, leftNull, rightNull, resultNull []uint64, length int) error {
	switch vb.operator {
	case planner.OpEqual:
		VectorEqualsInt64(result, left, right, length)
	case planner.OpNotEqual:
		for i := 0; i < length; i++ {
			result[i] = left[i] != right[i]
		}
	case planner.OpLess:
		VectorLessThanInt64(result, left, right, length)
	case planner.OpLessEqual:
		for i := 0; i < length; i++ {
			result[i] = left[i] <= right[i]
		}
	case planner.OpGreater:
		VectorGreaterThanInt64(result, left, right, length)
	case planner.OpGreaterEqual:
		for i := 0; i < length; i++ {
			result[i] = left[i] >= right[i]
		}
	}

	// Handle nulls - if either operand is null, result is null
	vb.propagateNulls(leftNull, rightNull, resultNull, length)

	return nil
}

// evalComparisonInt32 performs vectorized comparison on int32 values
func (vb *VectorizedBinaryOpEvaluator) evalComparisonInt32(left, right []int32, result []bool, leftNull, rightNull, resultNull []uint64, length int) error {
	evalComparisonGeneric(left, right, result, length, vb.operator)
	vb.propagateNulls(leftNull, rightNull, resultNull, length)
	return nil
}

// evalComparisonFloat64 performs vectorized comparison on float64 values
func (vb *VectorizedBinaryOpEvaluator) evalComparisonFloat64(left, right []float64, result []bool, leftNull, rightNull, resultNull []uint64, length int) error {
	evalComparisonGeneric(left, right, result, length, vb.operator)
	vb.propagateNulls(leftNull, rightNull, resultNull, length)
	return nil
}

// evalComparisonFloat32 performs vectorized comparison on float32 values
func (vb *VectorizedBinaryOpEvaluator) evalComparisonFloat32(left, right []float32, result []bool, leftNull, rightNull, resultNull []uint64, length int) error {
	evalComparisonGeneric(left, right, result, length, vb.operator)
	vb.propagateNulls(leftNull, rightNull, resultNull, length)
	return nil
}

// evalArithmetic evaluates arithmetic operations
func (vb *VectorizedBinaryOpEvaluator) evalArithmetic(left, right, result *Vector, length int) error {
	switch result.DataType {
	case types.Integer:
		return vb.evalArithmeticInt32(left.Int32Data, right.Int32Data, result.Int32Data, left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	case types.BigInt:
		return vb.evalArithmeticInt64(left.Int64Data, right.Int64Data, result.Int64Data, left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	case types.Float:
		return vb.evalArithmeticFloat32(left.Float32Data, right.Float32Data, result.Float32Data, left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	case types.Double:
		return vb.evalArithmeticFloat64(left.Float64Data, right.Float64Data, result.Float64Data, left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	default:
		return fmt.Errorf("unsupported type for vectorized arithmetic: %v", result.DataType)
	}
}

// evalArithmeticInt64 performs vectorized arithmetic on int64 values
func (vb *VectorizedBinaryOpEvaluator) evalArithmeticInt64(left, right, result []int64, leftNull, rightNull, resultNull []uint64, length int) error {
	switch vb.operator {
	case planner.OpAdd:
		VectorAddInt64(result, left, right, length)
	case planner.OpSubtract:
		for i := 0; i < length; i++ {
			result[i] = left[i] - right[i]
		}
	case planner.OpMultiply:
		VectorMultiplyInt64(result, left, right, length)
	case planner.OpDivide:
		// First propagate existing nulls
		vb.propagateNulls(leftNull, rightNull, resultNull, length)

		for i := 0; i < length; i++ {
			if right[i] == 0 {
				// Set null for division by zero
				byteIdx := i / 64
				bitIdx := uint(i % 64)
				resultNull[byteIdx] |= (1 << bitIdx)
			} else {
				result[i] = left[i] / right[i]
			}
		}
	}

	// Note: propagateNulls was called above for OpDivide case
	if vb.operator != planner.OpDivide {
		vb.propagateNulls(leftNull, rightNull, resultNull, length)
	}
	return nil
}

// evalArithmeticInt32 performs vectorized arithmetic on int32 values
func (vb *VectorizedBinaryOpEvaluator) evalArithmeticInt32(left, right, result []int32, leftNull, rightNull, resultNull []uint64, length int) error {
	switch vb.operator {
	case planner.OpAdd:
		for i := 0; i < length; i++ {
			result[i] = left[i] + right[i]
		}
	case planner.OpSubtract:
		for i := 0; i < length; i++ {
			result[i] = left[i] - right[i]
		}
	case planner.OpMultiply:
		for i := 0; i < length; i++ {
			result[i] = left[i] * right[i]
		}
	case planner.OpDivide:
		// First propagate existing nulls
		vb.propagateNulls(leftNull, rightNull, resultNull, length)

		for i := 0; i < length; i++ {
			if right[i] == 0 {
				// Set null for division by zero
				byteIdx := i / 64
				bitIdx := uint(i % 64)
				resultNull[byteIdx] |= (1 << bitIdx)
			} else {
				result[i] = left[i] / right[i]
			}
		}
	}

	// Note: propagateNulls was called above for OpDivide case
	if vb.operator != planner.OpDivide {
		vb.propagateNulls(leftNull, rightNull, resultNull, length)
	}
	return nil
}

// evalArithmeticFloat64 performs vectorized arithmetic on float64 values
func (vb *VectorizedBinaryOpEvaluator) evalArithmeticFloat64(left, right, result []float64, leftNull, rightNull, resultNull []uint64, length int) error {
	switch vb.operator {
	case planner.OpAdd:
		VectorAddFloat64(result, left, right, length)
	case planner.OpSubtract:
		for i := 0; i < length; i++ {
			result[i] = left[i] - right[i]
		}
	case planner.OpMultiply:
		VectorMultiplyFloat64(result, left, right, length)
	case planner.OpDivide:
		for i := 0; i < length; i++ {
			result[i] = left[i] / right[i]
		}
	}

	vb.propagateNulls(leftNull, rightNull, resultNull, length)
	return nil
}

// evalArithmeticFloat32 performs vectorized arithmetic on float32 values
func (vb *VectorizedBinaryOpEvaluator) evalArithmeticFloat32(left, right, result []float32, leftNull, rightNull, resultNull []uint64, length int) error {
	switch vb.operator {
	case planner.OpAdd:
		for i := 0; i < length; i++ {
			result[i] = left[i] + right[i]
		}
	case planner.OpSubtract:
		for i := 0; i < length; i++ {
			result[i] = left[i] - right[i]
		}
	case planner.OpMultiply:
		for i := 0; i < length; i++ {
			result[i] = left[i] * right[i]
		}
	case planner.OpDivide:
		for i := 0; i < length; i++ {
			result[i] = left[i] / right[i]
		}
	}

	vb.propagateNulls(leftNull, rightNull, resultNull, length)
	return nil
}

// evalLogical evaluates logical operations on boolean vectors
func (vb *VectorizedBinaryOpEvaluator) evalLogical(left, right, result *Vector, length int) error {
	switch vb.operator {
	case planner.OpAnd:
		for i := 0; i < length; i++ {
			result.BoolData[i] = left.BoolData[i] && right.BoolData[i]
		}
	case planner.OpOr:
		for i := 0; i < length; i++ {
			result.BoolData[i] = left.BoolData[i] || right.BoolData[i]
		}
	default:
		return fmt.Errorf("unsupported logical operator: %v", vb.operator)
	}

	vb.propagateNulls(left.NullBitmap, right.NullBitmap, result.NullBitmap, length)
	return nil
}

// propagateNulls sets result to null if either operand is null
func (vb *VectorizedBinaryOpEvaluator) propagateNulls(leftNull, rightNull, resultNull []uint64, length int) {
	// Vectorized null propagation
	numWords := (length + 63) / 64
	for i := 0; i < numWords; i++ {
		resultNull[i] = leftNull[i] | rightNull[i]
	}
}

// VectorizedColumnRefEvaluator evaluates column references on vectors
type VectorizedColumnRefEvaluator struct {
	columnIndex int
	columnName  string
}

// NewVectorizedColumnRefEvaluator creates a new vectorized column reference
func NewVectorizedColumnRefEvaluator(columnName string, columnIndex int) *VectorizedColumnRefEvaluator {
	return &VectorizedColumnRefEvaluator{
		columnIndex: columnIndex,
		columnName:  columnName,
	}
}

// EvalVector returns the vector for the referenced column
func (vc *VectorizedColumnRefEvaluator) EvalVector(batch *VectorizedBatch) (*Vector, error) {
	if vc.columnIndex < 0 || vc.columnIndex >= len(batch.Vectors) {
		return nil, fmt.Errorf("column index %d out of range", vc.columnIndex)
	}

	// Return the column vector directly (no copy needed)
	return batch.Vectors[vc.columnIndex], nil
}

// VectorizedLiteralEvaluator evaluates literal values on vectors
type VectorizedLiteralEvaluator struct {
	value    types.Value
	dataType types.DataType
}

// NewVectorizedLiteralEvaluator creates a new vectorized literal
func NewVectorizedLiteralEvaluator(value types.Value) *VectorizedLiteralEvaluator {
	return &VectorizedLiteralEvaluator{
		value:    value,
		dataType: value.Type(),
	}
}

// EvalVector returns a vector filled with the literal value
func (vl *VectorizedLiteralEvaluator) EvalVector(batch *VectorizedBatch) (*Vector, error) {
	result := NewVector(vl.dataType, batch.RowCount)
	result.Length = batch.RowCount

	// Fill the vector with the literal value
	if vl.value.IsNull() {
		// Set all values to null
		for i := 0; i < batch.RowCount; i++ {
			result.SetNull(i)
		}
	} else {
		// Fill with the literal value
		switch vl.dataType {
		case types.Integer:
			val := vl.value.Data.(int32)
			for i := 0; i < batch.RowCount; i++ {
				result.Int32Data[i] = val
			}
		case types.BigInt:
			val := vl.value.Data.(int64)
			for i := 0; i < batch.RowCount; i++ {
				result.Int64Data[i] = val
			}
		case types.Float:
			val := vl.value.Data.(float32)
			for i := 0; i < batch.RowCount; i++ {
				result.Float32Data[i] = val
			}
		case types.Double:
			val := vl.value.Data.(float64)
			for i := 0; i < batch.RowCount; i++ {
				result.Float64Data[i] = val
			}
		case types.Boolean:
			val := vl.value.Data.(bool)
			for i := 0; i < batch.RowCount; i++ {
				result.BoolData[i] = val
			}
		case types.Text, types.Varchar(0):
			val := vl.value.Data.(string)
			for i := 0; i < batch.RowCount; i++ {
				result.StringData[i] = val
			}
		default:
			return nil, fmt.Errorf("unsupported literal type for vectorization: %v", vl.dataType)
		}
	}

	return result, nil
}

// evalComparisonGeneric is a generic function for vectorized comparisons using ordered types
func evalComparisonGeneric[T cmp.Ordered](left, right []T, result []bool, length int, operator planner.BinaryOperator) {
	switch operator {
	case planner.OpEqual:
		for i := 0; i < length; i++ {
			result[i] = left[i] == right[i]
		}
	case planner.OpNotEqual:
		for i := 0; i < length; i++ {
			result[i] = left[i] != right[i]
		}
	case planner.OpLess:
		for i := 0; i < length; i++ {
			result[i] = left[i] < right[i]
		}
	case planner.OpLessEqual:
		for i := 0; i < length; i++ {
			result[i] = left[i] <= right[i]
		}
	case planner.OpGreater:
		for i := 0; i < length; i++ {
			result[i] = left[i] > right[i]
		}
	case planner.OpGreaterEqual:
		for i := 0; i < length; i++ {
			result[i] = left[i] >= right[i]
		}
	}
}
