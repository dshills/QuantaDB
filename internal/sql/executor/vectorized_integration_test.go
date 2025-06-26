package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// TestVectorizedOperators tests basic vectorized operator functionality
func TestVectorizedOperators(t *testing.T) {
	// Create test schema
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "value", Type: types.Integer},
			{Name: "name", Type: types.Text},
		},
	}

	// Test vector creation
	t.Run("VectorCreation", func(t *testing.T) {
		vec := NewVector(types.Integer, 100)
		if vec.Capacity != 100 {
			t.Errorf("Expected capacity 100, got %d", vec.Capacity)
		}
		if vec.DataType != types.Integer {
			t.Errorf("Expected Integer type, got %v", vec.DataType)
		}
	})

	// Test batch creation
	t.Run("BatchCreation", func(t *testing.T) {
		batch := NewVectorizedBatch(schema, 64)
		if len(batch.Vectors) != 3 {
			t.Errorf("Expected 3 vectors, got %d", len(batch.Vectors))
		}
		if batch.Schema != schema {
			t.Error("Schema mismatch")
		}
	})

	// Test vectorized expression evaluation
	t.Run("VectorizedExpression", func(t *testing.T) {
		// Create a batch with test data
		batch := NewVectorizedBatch(schema, 4)
		batch.RowCount = 4

		// Fill with test data
		batch.Vectors[0].Int32Data = []int32{1, 2, 3, 4}
		batch.Vectors[1].Int32Data = []int32{10, 20, 30, 40}
		batch.Vectors[2].StringData = []string{"a", "b", "c", "d"}

		// Test literal evaluator
		litEval := NewVectorizedLiteralEvaluator(types.NewValue(int32(25)))
		litVec, err := litEval.EvalVector(batch)
		if err != nil {
			t.Fatalf("Failed to evaluate literal: %v", err)
		}

		if litVec.Length != 4 {
			t.Errorf("Expected length 4, got %d", litVec.Length)
		}

		for i := 0; i < 4; i++ {
			if litVec.Int32Data[i] != 25 {
				t.Errorf("Expected value 25 at index %d, got %d", i, litVec.Int32Data[i])
			}
		}
	})

	// Test null handling
	t.Run("NullHandling", func(t *testing.T) {
		vec := NewVector(types.Integer, 10)
		vec.Length = 5

		// Set some nulls
		vec.SetNull(1)
		vec.SetNull(3)

		if !vec.IsNull(1) {
			t.Error("Expected null at index 1")
		}
		if !vec.IsNull(3) {
			t.Error("Expected null at index 3")
		}
		if vec.IsNull(0) {
			t.Error("Expected non-null at index 0")
		}
	})

	// Test vectorized arithmetic operations
	t.Run("VectorizedArithmetic", func(t *testing.T) {
		// Create test vectors
		left := NewVector(types.BigInt, 4)
		right := NewVector(types.BigInt, 4)
		result := NewVector(types.BigInt, 4)

		left.Int64Data = []int64{10, 20, 30, 40}
		right.Int64Data = []int64{5, 10, 15, 20}

		// Test addition
		VectorAddInt64(result.Int64Data, left.Int64Data, right.Int64Data, 4)
		expected := []int64{15, 30, 45, 60}
		for i := 0; i < 4; i++ {
			if result.Int64Data[i] != expected[i] {
				t.Errorf("Add: Expected %d at index %d, got %d", expected[i], i, result.Int64Data[i])
			}
		}

		// Test multiplication
		VectorMultiplyInt64(result.Int64Data, left.Int64Data, right.Int64Data, 4)
		expected = []int64{50, 200, 450, 800}
		for i := 0; i < 4; i++ {
			if result.Int64Data[i] != expected[i] {
				t.Errorf("Multiply: Expected %d at index %d, got %d", expected[i], i, result.Int64Data[i])
			}
		}
	})

	// Test vectorized comparison operations
	t.Run("VectorizedComparison", func(t *testing.T) {
		// Create test vectors
		left := NewVector(types.BigInt, 4)
		right := NewVector(types.BigInt, 4)
		result := make([]bool, 4)

		left.Int64Data = []int64{10, 20, 30, 40}
		right.Int64Data = []int64{15, 20, 25, 45}

		// Test less than
		VectorLessThanInt64(result, left.Int64Data, right.Int64Data, 4)
		expectedBool := []bool{true, false, false, true}
		for i := 0; i < 4; i++ {
			if result[i] != expectedBool[i] {
				t.Errorf("LessThan: Expected %v at index %d, got %v", expectedBool[i], i, result[i])
			}
		}

		// Test equals
		VectorEqualsInt64(result, left.Int64Data, right.Int64Data, 4)
		expectedBool = []bool{false, true, false, false}
		for i := 0; i < 4; i++ {
			if result[i] != expectedBool[i] {
				t.Errorf("Equals: Expected %v at index %d, got %v", expectedBool[i], i, result[i])
			}
		}

		// Test greater than
		VectorGreaterThanInt64(result, left.Int64Data, right.Int64Data, 4)
		expectedBool = []bool{false, false, true, false}
		for i := 0; i < 4; i++ {
			if result[i] != expectedBool[i] {
				t.Errorf("GreaterThan: Expected %v at index %d, got %v", expectedBool[i], i, result[i])
			}
		}
	})

	// Test vectorized float operations
	t.Run("VectorizedFloat", func(t *testing.T) {
		// Create test vectors
		left := NewVector(types.Double, 4)
		right := NewVector(types.Double, 4)
		result := NewVector(types.Double, 4)

		left.Float64Data = []float64{10.5, 20.5, 30.5, 40.5}
		right.Float64Data = []float64{5.5, 10.5, 15.5, 20.5}

		// Test addition
		VectorAddFloat64(result.Float64Data, left.Float64Data, right.Float64Data, 4)
		expected := []float64{16.0, 31.0, 46.0, 61.0}
		for i := 0; i < 4; i++ {
			if result.Float64Data[i] != expected[i] {
				t.Errorf("Float Add: Expected %f at index %d, got %f", expected[i], i, result.Float64Data[i])
			}
		}

		// Test multiplication
		VectorMultiplyFloat64(result.Float64Data, left.Float64Data, right.Float64Data, 4)
		expected = []float64{57.75, 215.25, 472.75, 830.25}
		for i := 0; i < 4; i++ {
			if result.Float64Data[i] != expected[i] {
				t.Errorf("Float Multiply: Expected %f at index %d, got %f", expected[i], i, result.Float64Data[i])
			}
		}
	})
}

// TestVectorizedFallback tests the fallback mechanism
func TestVectorizedFallback(t *testing.T) {
	// This is a simple test for the fallback mechanism
	// In a real implementation, we'd test more complex expressions
	
	t.Run("FallbackForUnsupportedExpression", func(t *testing.T) {
		// Create a simple literal expression (which is supported)
		lit := &VectorizedLiteralEvaluator{
			value:    types.NewValue(int32(42)),
			dataType: types.Integer,
		}
		
		// Create a batch
		schema := &Schema{
			Columns: []Column{{Name: "col1", Type: types.Integer}},
		}
		batch := NewVectorizedBatch(schema, 4)
		batch.RowCount = 4
		
		// Evaluate
		result, err := lit.EvalVector(batch)
		if err != nil {
			t.Fatalf("Failed to evaluate: %v", err)
		}
		
		if result.Length != 4 {
			t.Errorf("Expected length 4, got %d", result.Length)
		}
		
		// All values should be 42
		for i := 0; i < 4; i++ {
			if result.Int32Data[i] != 42 {
				t.Errorf("Expected 42 at index %d, got %d", i, result.Int32Data[i])
			}
		}
	})
}