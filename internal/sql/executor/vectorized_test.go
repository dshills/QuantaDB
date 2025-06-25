package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/assert"
)

func TestVector(t *testing.T) {
	t.Run("NewVector", func(t *testing.T) {
		// Test different data types
		tests := []struct {
			name     string
			dataType types.DataType
			capacity int
		}{
			{"Integer", types.Integer, 100},
			{"BigInt", types.BigInt, 100},
			{"Float", types.Float, 100},
			{"Double", types.Double, 100},
			{"Boolean", types.Boolean, 100},
			{"Text", types.Text, 100},
			{"Bytea", types.Bytea, 100},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				v := NewVector(tt.dataType, tt.capacity)
				assert.NotNil(t, v)
				assert.Equal(t, tt.dataType, v.DataType)
				assert.Equal(t, tt.capacity, v.Capacity)
				assert.Equal(t, 0, v.Length)
				assert.NotNil(t, v.NullBitmap)
			})
		}
	})

	t.Run("NullHandling", func(t *testing.T) {
		v := NewVector(types.Integer, 100)
		
		// Test setting null
		v.SetNull(10)
		assert.True(t, v.IsNull(10))
		assert.False(t, v.IsNull(11))
		
		// Test multiple nulls
		v.SetNull(63)
		v.SetNull(64)
		v.SetNull(65)
		assert.True(t, v.IsNull(63))
		assert.True(t, v.IsNull(64))
		assert.True(t, v.IsNull(65))
		assert.False(t, v.IsNull(62))
		assert.False(t, v.IsNull(66))
	})

	t.Run("Reset", func(t *testing.T) {
		v := NewVector(types.Integer, 100)
		v.Length = 50
		v.SetNull(10)
		v.SetNull(20)
		
		v.Reset()
		assert.Equal(t, 0, v.Length)
		assert.False(t, v.IsNull(10))
		assert.False(t, v.IsNull(20))
	})
}

func TestVectorizedBatch(t *testing.T) {
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
			{Name: "score", Type: types.Double},
		},
	}

	t.Run("NewVectorizedBatch", func(t *testing.T) {
		batch := NewVectorizedBatch(schema, 1024)
		assert.NotNil(t, batch)
		assert.Equal(t, schema, batch.Schema)
		assert.Equal(t, 3, len(batch.Vectors))
		assert.Equal(t, 0, batch.RowCount)
		
		// Check vector types
		assert.Equal(t, types.Integer, batch.Vectors[0].DataType)
		assert.Equal(t, types.Text, batch.Vectors[1].DataType)
		assert.Equal(t, types.Double, batch.Vectors[2].DataType)
	})

	t.Run("Reset", func(t *testing.T) {
		batch := NewVectorizedBatch(schema, 1024)
		batch.RowCount = 100
		batch.Vectors[0].Length = 100
		batch.Vectors[1].Length = 100
		batch.Vectors[2].Length = 100
		
		batch.Reset()
		assert.Equal(t, 0, batch.RowCount)
		assert.Equal(t, 0, batch.Vectors[0].Length)
		assert.Equal(t, 0, batch.Vectors[1].Length)
		assert.Equal(t, 0, batch.Vectors[2].Length)
	})
}

func TestVectorizedArithmetic(t *testing.T) {
	t.Run("VectorAddInt64", func(t *testing.T) {
		left := []int64{1, 2, 3, 4, 5}
		right := []int64{10, 20, 30, 40, 50}
		result := make([]int64, 5)
		
		VectorAddInt64(result, left, right, 5)
		
		expected := []int64{11, 22, 33, 44, 55}
		assert.Equal(t, expected, result)
	})

	t.Run("VectorAddFloat64", func(t *testing.T) {
		left := []float64{1.5, 2.5, 3.5}
		right := []float64{0.5, 0.5, 0.5}
		result := make([]float64, 3)
		
		VectorAddFloat64(result, left, right, 3)
		
		expected := []float64{2.0, 3.0, 4.0}
		assert.InDelta(t, expected[0], result[0], 0.001)
		assert.InDelta(t, expected[1], result[1], 0.001)
		assert.InDelta(t, expected[2], result[2], 0.001)
	})

	t.Run("VectorMultiplyInt64", func(t *testing.T) {
		left := []int64{2, 3, 4, 5}
		right := []int64{10, 10, 10, 10}
		result := make([]int64, 4)
		
		VectorMultiplyInt64(result, left, right, 4)
		
		expected := []int64{20, 30, 40, 50}
		assert.Equal(t, expected, result)
	})

	t.Run("VectorMultiplyFloat64", func(t *testing.T) {
		left := []float64{1.5, 2.5, 3.5}
		right := []float64{2.0, 2.0, 2.0}
		result := make([]float64, 3)
		
		VectorMultiplyFloat64(result, left, right, 3)
		
		expected := []float64{3.0, 5.0, 7.0}
		assert.InDelta(t, expected[0], result[0], 0.001)
		assert.InDelta(t, expected[1], result[1], 0.001)
		assert.InDelta(t, expected[2], result[2], 0.001)
	})
}

func TestVectorizedComparisons(t *testing.T) {
	t.Run("VectorEqualsInt64", func(t *testing.T) {
		left := []int64{1, 2, 3, 4, 5}
		right := []int64{1, 0, 3, 0, 5}
		result := make([]bool, 5)
		
		VectorEqualsInt64(result, left, right, 5)
		
		expected := []bool{true, false, true, false, true}
		assert.Equal(t, expected, result)
	})

	t.Run("VectorGreaterThanInt64", func(t *testing.T) {
		left := []int64{5, 4, 3, 2, 1}
		right := []int64{1, 2, 3, 4, 5}
		result := make([]bool, 5)
		
		VectorGreaterThanInt64(result, left, right, 5)
		
		expected := []bool{true, true, false, false, false}
		assert.Equal(t, expected, result)
	})

	t.Run("VectorLessThanInt64", func(t *testing.T) {
		left := []int64{1, 2, 3, 4, 5}
		right := []int64{5, 4, 3, 2, 1}
		result := make([]bool, 5)
		
		VectorLessThanInt64(result, left, right, 5)
		
		expected := []bool{true, true, false, false, false}
		assert.Equal(t, expected, result)
	})
}

func TestVectorizedCapabilities(t *testing.T) {
	caps := GetVectorizedCapabilities()
	
	assert.True(t, caps["basic_arithmetic"])
	assert.True(t, caps["comparisons"])
	assert.True(t, caps["null_handling"])
	assert.True(t, caps["simd_optimized"])
	assert.True(t, caps["batch_size"])
}

// Benchmark tests to measure performance improvements
func BenchmarkVectorizedAddition(b *testing.B) {
	size := 1024
	left := make([]int64, size)
	right := make([]int64, size)
	result := make([]int64, size)
	
	// Initialize data
	for i := 0; i < size; i++ {
		left[i] = int64(i)
		right[i] = int64(i * 2)
	}
	
	b.Run("Vectorized", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			VectorAddInt64(result, left, right, size)
		}
	})
	
	b.Run("Scalar", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < size; j++ {
				result[j] = left[j] + right[j]
			}
		}
	})
}

func BenchmarkVectorizedComparison(b *testing.B) {
	size := 1024
	left := make([]int64, size)
	right := make([]int64, size)
	result := make([]bool, size)
	
	// Initialize data
	for i := 0; i < size; i++ {
		left[i] = int64(i)
		right[i] = int64(i % 2)
	}
	
	b.Run("Vectorized", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			VectorEqualsInt64(result, left, right, size)
		}
	})
	
	b.Run("Scalar", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < size; j++ {
				result[j] = left[j] == right[j]
			}
		}
	})
}

func BenchmarkVectorizedFilter(b *testing.B) {
	size := 1024
	data := make([]int64, size)
	filter := make([]bool, size)
	output := make([]int64, size)
	
	// Initialize data with alternating pattern
	for i := 0; i < size; i++ {
		data[i] = int64(i)
		filter[i] = i%2 == 0
	}
	
	b.Run("Vectorized", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			outputIdx := 0
			for j := 0; j < size; j++ {
				if filter[j] {
					output[outputIdx] = data[j]
					outputIdx++
				}
			}
		}
	})
	
	b.Run("BranchlessFilter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			outputIdx := 0
			for j := 0; j < size; j++ {
				// Branchless version using conditional move
				mask := int64(0)
				if filter[j] {
					mask = -1
				}
				output[outputIdx] = data[j] & mask
				outputIdx += int((mask & 1))
			}
		}
	})
}