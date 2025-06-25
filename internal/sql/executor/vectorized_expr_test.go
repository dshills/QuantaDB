package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/assert"
)

func TestVectorizedBinaryOpEvaluator(t *testing.T) {
	schema := &Schema{
		Columns: []Column{
			{Name: "a", Type: types.Integer},
			{Name: "b", Type: types.Integer},
			{Name: "c", Type: types.Double},
			{Name: "d", Type: types.Double},
		},
	}
	
	// Create test batch
	batch := NewVectorizedBatch(schema, 5)
	batch.RowCount = 5
	
	// Initialize data
	batch.Vectors[0].Int32Data = []int32{10, 20, 30, 40, 50}
	batch.Vectors[0].Length = 5
	batch.Vectors[1].Int32Data = []int32{5, 15, 30, 35, 60}
	batch.Vectors[1].Length = 5
	batch.Vectors[2].Float64Data = []float64{1.5, 2.5, 3.5, 4.5, 5.5}
	batch.Vectors[2].Length = 5
	batch.Vectors[3].Float64Data = []float64{0.5, 0.5, 0.5, 0.5, 0.5}
	batch.Vectors[3].Length = 5
	
	t.Run("Arithmetic_Addition_Int32", func(t *testing.T) {
		leftEval := NewVectorizedColumnRefEvaluator("a", 0)
		rightEval := NewVectorizedColumnRefEvaluator("b", 1)
		
		addEval := NewVectorizedBinaryOpEvaluator(leftEval, rightEval, planner.OpAdd, types.Integer)
		result, err := addEval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, types.Integer, result.DataType)
		assert.Equal(t, 5, result.Length)
		
		expected := []int32{15, 35, 60, 75, 110}
		assert.Equal(t, expected, result.Int32Data[:5])
	})
	
	t.Run("Arithmetic_Subtraction_Float64", func(t *testing.T) {
		leftEval := NewVectorizedColumnRefEvaluator("c", 2)
		rightEval := NewVectorizedColumnRefEvaluator("d", 3)
		
		subEval := NewVectorizedBinaryOpEvaluator(leftEval, rightEval, planner.OpSubtract, types.Double)
		result, err := subEval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, types.Double, result.DataType)
		assert.Equal(t, 5, result.Length)
		
		expected := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
		for i := 0; i < 5; i++ {
			assert.InDelta(t, expected[i], result.Float64Data[i], 0.001)
		}
	})
	
	t.Run("Comparison_Equals_Int32", func(t *testing.T) {
		leftEval := NewVectorizedColumnRefEvaluator("a", 0)
		rightEval := NewVectorizedColumnRefEvaluator("b", 1)
		
		eqEval := NewVectorizedBinaryOpEvaluator(leftEval, rightEval, planner.OpEqual, types.Integer)
		result, err := eqEval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, types.Boolean, result.DataType)
		assert.Equal(t, 5, result.Length)
		
		expected := []bool{false, false, true, false, false}
		assert.Equal(t, expected, result.BoolData[:5])
	})
	
	t.Run("Comparison_GreaterThan_Float64", func(t *testing.T) {
		leftEval := NewVectorizedColumnRefEvaluator("c", 2)
		rightEval := NewVectorizedLiteralEvaluator(types.NewValue(3.0))
		
		gtEval := NewVectorizedBinaryOpEvaluator(leftEval, rightEval, planner.OpGreater, types.Double)
		result, err := gtEval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, types.Boolean, result.DataType)
		assert.Equal(t, 5, result.Length)
		
		expected := []bool{false, false, true, true, true}
		assert.Equal(t, expected, result.BoolData[:5])
	})
	
	t.Run("Logical_And", func(t *testing.T) {
		// Create boolean batch
		boolBatch := NewVectorizedBatch(&Schema{
			Columns: []Column{
				{Name: "x", Type: types.Boolean},
				{Name: "y", Type: types.Boolean},
			},
		}, 4)
		boolBatch.RowCount = 4
		boolBatch.Vectors[0].BoolData = []bool{true, true, false, false}
		boolBatch.Vectors[0].Length = 4
		boolBatch.Vectors[1].BoolData = []bool{true, false, true, false}
		boolBatch.Vectors[1].Length = 4
		
		leftEval := NewVectorizedColumnRefEvaluator("x", 0)
		rightEval := NewVectorizedColumnRefEvaluator("y", 1)
		
		andEval := NewVectorizedBinaryOpEvaluator(leftEval, rightEval, planner.OpAnd, types.Boolean)
		result, err := andEval.EvalVector(boolBatch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, types.Boolean, result.DataType)
		assert.Equal(t, 4, result.Length)
		
		expected := []bool{true, false, false, false}
		assert.Equal(t, expected, result.BoolData[:4])
	})
	
	t.Run("Null_Propagation", func(t *testing.T) {
		// Set some nulls
		batch.Vectors[0].SetNull(1)
		batch.Vectors[1].SetNull(3)
		
		leftEval := NewVectorizedColumnRefEvaluator("a", 0)
		rightEval := NewVectorizedColumnRefEvaluator("b", 1)
		
		addEval := NewVectorizedBinaryOpEvaluator(leftEval, rightEval, planner.OpAdd, types.Integer)
		result, err := addEval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		
		// Check that nulls propagated correctly
		assert.False(t, result.IsNull(0)) // Neither null
		assert.True(t, result.IsNull(1))  // Left is null
		assert.False(t, result.IsNull(2)) // Neither null
		assert.True(t, result.IsNull(3))  // Right is null
		assert.False(t, result.IsNull(4)) // Neither null
	})
	
	t.Run("Division_By_Zero", func(t *testing.T) {
		// Create batch with zeros
		zeroBatch := NewVectorizedBatch(&Schema{
			Columns: []Column{
				{Name: "num", Type: types.Integer},
				{Name: "den", Type: types.Integer},
			},
		}, 3)
		zeroBatch.RowCount = 3
		zeroBatch.Vectors[0].Int32Data = []int32{10, 20, 30}
		zeroBatch.Vectors[0].Length = 3
		zeroBatch.Vectors[1].Int32Data = []int32{5, 0, 10}
		zeroBatch.Vectors[1].Length = 3
		
		leftEval := NewVectorizedColumnRefEvaluator("num", 0)
		rightEval := NewVectorizedColumnRefEvaluator("den", 1)
		
		divEval := NewVectorizedBinaryOpEvaluator(leftEval, rightEval, planner.OpDivide, types.Integer)
		result, err := divEval.EvalVector(zeroBatch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		
		// Check results
		assert.Equal(t, int32(2), result.Int32Data[0])
		assert.True(t, result.IsNull(1)) // Division by zero results in null
		assert.Equal(t, int32(3), result.Int32Data[2])
	})
}

func TestVectorizedColumnRefEvaluator(t *testing.T) {
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
		},
	}
	
	batch := NewVectorizedBatch(schema, 3)
	batch.RowCount = 3
	batch.Vectors[0].Int32Data = []int32{1, 2, 3}
	batch.Vectors[0].Length = 3
	batch.Vectors[1].StringData = []string{"Alice", "Bob", "Charlie"}
	batch.Vectors[1].Length = 3
	
	t.Run("ValidColumnRef", func(t *testing.T) {
		eval := NewVectorizedColumnRefEvaluator("id", 0)
		result, err := eval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, batch.Vectors[0], result) // Should return the same vector
	})
	
	t.Run("InvalidColumnIndex", func(t *testing.T) {
		eval := NewVectorizedColumnRefEvaluator("invalid", 5)
		result, err := eval.EvalVector(batch)
		
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "column index 5 out of range")
	})
}

func TestVectorizedLiteralEvaluator(t *testing.T) {
	schema := &Schema{Columns: []Column{{Name: "dummy", Type: types.Integer}}}
	batch := NewVectorizedBatch(schema, 5)
	batch.RowCount = 5
	
	t.Run("IntegerLiteral", func(t *testing.T) {
		eval := NewVectorizedLiteralEvaluator(types.NewValue(int32(42)))
		result, err := eval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, types.Integer, result.DataType)
		assert.Equal(t, 5, result.Length)
		
		// All values should be 42
		for i := 0; i < 5; i++ {
			assert.Equal(t, int32(42), result.Int32Data[i])
			assert.False(t, result.IsNull(i))
		}
	})
	
	t.Run("StringLiteral", func(t *testing.T) {
		eval := NewVectorizedLiteralEvaluator(types.NewValue("hello"))
		result, err := eval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, types.Text, result.DataType)
		assert.Equal(t, 5, result.Length)
		
		// All values should be "hello"
		for i := 0; i < 5; i++ {
			assert.Equal(t, "hello", result.StringData[i])
			assert.False(t, result.IsNull(i))
		}
	})
	
	t.Run("NullLiteral", func(t *testing.T) {
		nullVal := types.NewNullValue()
		
		eval := NewVectorizedLiteralEvaluator(nullVal)
		result, err := eval.EvalVector(batch)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, types.Unknown, result.DataType)
		assert.Equal(t, 5, result.Length)
		
		// All values should be null
		for i := 0; i < 5; i++ {
			assert.True(t, result.IsNull(i))
		}
	})
}

func BenchmarkVectorizedExpressions(b *testing.B) {
	schema := &Schema{
		Columns: []Column{
			{Name: "a", Type: types.BigInt},
			{Name: "b", Type: types.BigInt},
			{Name: "c", Type: types.BigInt},
		},
	}
	
	batch := NewVectorizedBatch(schema, VectorSize)
	batch.RowCount = VectorSize
	
	// Initialize with test data
	for i := 0; i < VectorSize; i++ {
		batch.Vectors[0].Int64Data[i] = int64(i)
		batch.Vectors[1].Int64Data[i] = int64(i * 2)
		batch.Vectors[2].Int64Data[i] = int64(i * 3)
	}
	batch.Vectors[0].Length = VectorSize
	batch.Vectors[1].Length = VectorSize
	batch.Vectors[2].Length = VectorSize
	
	b.Run("SimpleAddition", func(b *testing.B) {
		leftEval := NewVectorizedColumnRefEvaluator("a", 0)
		rightEval := NewVectorizedColumnRefEvaluator("b", 1)
		addEval := NewVectorizedBinaryOpEvaluator(leftEval, rightEval, planner.OpAdd, types.BigInt)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := addEval.EvalVector(batch)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("ComplexExpression", func(b *testing.B) {
		// (a + b) * c
		aEval := NewVectorizedColumnRefEvaluator("a", 0)
		bEval := NewVectorizedColumnRefEvaluator("b", 1)
		cEval := NewVectorizedColumnRefEvaluator("c", 2)
		
		addEval := NewVectorizedBinaryOpEvaluator(aEval, bEval, planner.OpAdd, types.BigInt)
		mulEval := NewVectorizedBinaryOpEvaluator(addEval, cEval, planner.OpMultiply, types.BigInt)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := mulEval.EvalVector(batch)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	
	b.Run("ComparisonChain", func(b *testing.B) {
		// a < b AND b < c
		aEval := NewVectorizedColumnRefEvaluator("a", 0)
		bEval := NewVectorizedColumnRefEvaluator("b", 1)
		cEval := NewVectorizedColumnRefEvaluator("c", 2)
		
		lt1Eval := NewVectorizedBinaryOpEvaluator(aEval, bEval, planner.OpLess, types.BigInt)
		lt2Eval := NewVectorizedBinaryOpEvaluator(bEval, cEval, planner.OpLess, types.BigInt)
		andEval := NewVectorizedBinaryOpEvaluator(lt1Eval, lt2Eval, planner.OpAnd, types.Boolean)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := andEval.EvalVector(batch)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}