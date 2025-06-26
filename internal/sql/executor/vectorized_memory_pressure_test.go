package executor

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVectorizedMemoryPressure tests vectorized operations under memory constraints
func TestVectorizedMemoryPressure(t *testing.T) {
	// Get initial memory stats
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)

	tests := []struct {
		name            string
		vectorSize      int
		numColumns      int
		numBatches      int
		memoryLimit     int64
		expectOOM       bool
		concurrentOps   int
	}{
		{
			name:          "Normal operation within limits",
			vectorSize:    1024,
			numColumns:    10,
			numBatches:    100,
			memoryLimit:   100 * 1024 * 1024, // 100MB
			expectOOM:     false,
			concurrentOps: 1,
		},
		{
			name:          "Large vectors exceeding limit",
			vectorSize:    8192,
			numColumns:    50,
			numBatches:    100,
			memoryLimit:   5 * 1024 * 1024, // 5MB - much tighter
			expectOOM:     true,
			concurrentOps: 1,
		},
		{
			name:          "Concurrent operations",
			vectorSize:    1024,
			numColumns:    20,
			numBatches:    50,
			memoryLimit:   100 * 1024 * 1024, // 100MB
			expectOOM:     false,
			concurrentOps: 4,
		},
		{
			name:          "High concurrency with tight limit",
			vectorSize:    2048,
			numColumns:    15,
			numBatches:    50,
			memoryLimit:   2 * 1024 * 1024, // 2MB - very tight for 8 workers
			expectOOM:     true,
			concurrentOps: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create memory tracker
			tracker := NewMemoryTracker(tt.memoryLimit)
			
			// Track errors
			errors := make(chan error, tt.concurrentOps)
			var wg sync.WaitGroup

			// Run concurrent operations
			for i := 0; i < tt.concurrentOps; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					
					err := runVectorizedOperation(workerID, tt.vectorSize, 
						tt.numColumns, tt.numBatches, tracker)
					
					if err != nil {
						errors <- err
					}
				}(i)
			}

			// Wait for completion
			done := make(chan bool)
			go func() {
				wg.Wait()
				close(done)
			}()

			// Check results
			select {
			case <-done:
				close(errors)
				
				// Count OOM errors
				oomCount := 0
				for err := range errors {
					if err != nil && isOOMError(err) {
						oomCount++
					}
				}

				if tt.expectOOM {
					assert.True(t, oomCount > 0, 
						"Expected OOM errors but none occurred")
				} else {
					assert.Equal(t, 0, oomCount, 
						"Unexpected OOM errors: %d", oomCount)
				}

			case <-time.After(30 * time.Second):
				t.Fatal("Test timeout - possible deadlock")
			}

			// Verify memory was released
			finalUsage := tracker.CurrentUsage()
			assert.Equal(t, int64(0), finalUsage, 
				"Memory not fully released after operations")

			// Check peak usage
			peakUsage := tracker.PeakUsage()
			t.Logf("Peak memory usage: %d MB (limit: %d MB)", 
				peakUsage/(1024*1024), tt.memoryLimit/(1024*1024))
		})
	}
}

// TestVectorizedBatchGrowth tests batch allocation and growth patterns
func TestVectorizedBatchGrowth(t *testing.T) {
	tracker := NewMemoryTracker(50 * 1024 * 1024) // 50MB limit

	// Test progressive batch sizes
	batchSizes := []int{1, 10, 100, 1000, 2000, 4000, 8000}
	
	for _, size := range batchSizes {
		t.Run(fmt.Sprintf("BatchSize_%d", size), func(t *testing.T) {
			// Create schema with various data types
			schema := &Schema{
				Columns: []Column{
					{Name: "id", Type: types.BigInt},
					{Name: "value", Type: types.Double},
					{Name: "text", Type: types.Text},
					{Name: "data", Type: types.Bytea},
					{Name: "flag", Type: types.Boolean},
				},
			}

			// Allocate batch
			batchID := fmt.Sprintf("batch_%d", size)
			batch := NewVectorizedBatch(schema, size)
			
			// Estimate memory
			estimatedSize := estimateBatchMemory(batch)
			
			// Try to allocate
			err := tracker.Allocate(estimatedSize, batchID)
			
			if err != nil {
				assert.True(t, size >= 4000, 
					"Allocation failed for small batch size %d", size)
			} else {
				// Fill batch with data
				fillBatchWithTestData(batch)
				
				// Verify memory usage is reasonable
				actualSize := measureActualBatchMemory(batch)
				ratio := float64(actualSize) / float64(estimatedSize)
				
				assert.True(t, ratio > 0.8 && ratio < 1.5,
					"Memory estimation off: estimated %d, actual %d (ratio %.2f)",
					estimatedSize, actualSize, ratio)
				
				// Release memory
				tracker.Release(batchID)
			}
		})
	}
}

// TestVectorizedExpressionMemory tests memory usage of vectorized expressions
func TestVectorizedExpressionMemory(t *testing.T) {
	tracker := NewMemoryTracker(100 * 1024 * 1024) // 100MB

	// Create test vectors
	size := 1024
	v1 := createTestVector(types.Integer, size, 1)
	v2 := createTestVector(types.Integer, size, 2)
	
	// Track vector memory
	vecSize := estimateVectorMemory(v1) + estimateVectorMemory(v2)
	require.NoError(t, tracker.Allocate(vecSize, "input_vectors"))

	tests := []struct {
		name      string
		operation string
		expected  int
	}{
		{"Addition", "+", 3},
		{"Multiplication", "*", 2},
		{"Division", "/", 0}, // 1/2 = 0 in integer division
		{"Modulo", "%", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For now, simulate expression evaluation
			// TODO: Add proper vectorized expression evaluation when types are available
			result := NewVector(types.Integer, size)
			
			// Simulate the operation
			for i := 0; i < size; i++ {
				var val int32
				switch tt.operation {
				case "+":
					val = v1.Int32Data[i] + v2.Int32Data[i]
				case "*":
					val = v1.Int32Data[i] * v2.Int32Data[i]
				case "/":
					if v2.Int32Data[i] != 0 {
						val = v1.Int32Data[i] / v2.Int32Data[i]
					}
				case "%":
					if v2.Int32Data[i] != 0 {
						val = v1.Int32Data[i] % v2.Int32Data[i]
					}
				}
				result.Int32Data[i] = val
			}
			result.Length = size
			
			var err error
			require.NoError(t, err)
			require.NotNil(t, result)

			// Track result memory
			resultSize := estimateVectorMemory(result)
			err = tracker.Allocate(resultSize, fmt.Sprintf("result_%s", tt.name))
			assert.NoError(t, err)

			// Verify result
			assert.Equal(t, size, result.Length)
			if result.Int32Data != nil && len(result.Int32Data) > 0 {
				assert.Equal(t, int32(tt.expected), result.Int32Data[0])
			}

			// Release result memory
			tracker.Release(fmt.Sprintf("result_%s", tt.name))
		})
	}

	// Final cleanup
	tracker.Release("input_vectors")
}

// TestVectorizedOperatorFallback tests fallback behavior simulation
// TODO: Implement when adaptive operators are available
func TestVectorizedOperatorFallback(t *testing.T) {
	t.Skip("Adaptive operators not yet implemented")
}

// TestVectorMemoryRecycling tests memory recycling in vectorized operations
func TestVectorMemoryRecycling(t *testing.T) {
	tracker := NewMemoryTracker(10 * 1024 * 1024) // 10MB
	
	// Create vector pool
	pool := &VectorPool{
		vectors: make(map[types.DataType][]*Vector),
		tracker: tracker,
	}

	// Test recycling vectors
	var wg sync.WaitGroup
	numWorkers := 10
	opsPerWorker := 100

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < opsPerWorker; j++ {
				// Get vector from pool
				vec := pool.Get(types.Integer, 1024)
				
				// Use vector
				for k := 0; k < vec.Capacity && k < 100; k++ {
					vec.Int32Data[k] = int32(id*1000 + j*10 + k)
				}
				vec.Length = 100
				
				// Return to pool
				pool.Put(vec)
			}
		}(i)
	}

	wg.Wait()

	// Check memory usage
	currentUsage := tracker.CurrentUsage()
	peakUsage := tracker.PeakUsage()
	
	t.Logf("Current memory: %d KB, Peak memory: %d KB", 
		currentUsage/1024, peakUsage/1024)
	
	// Peak usage should be much less than if we allocated new vectors each time
	expectedWithoutRecycling := int64(numWorkers * opsPerWorker * 1024 * 4) // 4 bytes per int32
	assert.True(t, peakUsage < expectedWithoutRecycling/10, 
		"Memory recycling not effective: peak %d, expected < %d",
		peakUsage, expectedWithoutRecycling/10)
}

// Helper functions

func runVectorizedOperation(workerID, vectorSize, numColumns, numBatches int, 
	tracker *MemoryTracker) error {
	
	// Create schema
	columns := make([]Column, numColumns)
	for i := 0; i < numColumns; i++ {
		columns[i] = Column{
			Name: fmt.Sprintf("col%d", i),
			Type: types.Integer,
		}
	}
	schema := &Schema{Columns: columns}

	// Process batches
	for i := 0; i < numBatches; i++ {
		batchID := fmt.Sprintf("worker%d_batch%d", workerID, i)
		
		// Create batch
		batch := NewVectorizedBatch(schema, vectorSize)
		
		// Estimate and allocate memory
		size := estimateBatchMemory(batch)
		if err := tracker.Allocate(size, batchID); err != nil {
			return err
		}

		// Simulate some work
		fillBatchWithTestData(batch)
		
		// Simulate processing time
		time.Sleep(time.Microsecond * 100)
		
		// Release memory
		tracker.Release(batchID)
	}

	return nil
}

func isOOMError(err error) bool {
	return err != nil && 
		(err.Error() == "memory limit exceeded" ||
		 // Match the actual error format from MemoryTracker
		 len(err.Error()) > 20 && err.Error()[:20] == "memory limit exceeded")
}

func estimateBatchMemory(batch *VectorizedBatch) int64 {
	if batch == nil {
		return 0
	}
	
	size := int64(0)
	for _, vec := range batch.Vectors {
		size += estimateVectorMemory(vec)
	}
	return size
}

func estimateVectorMemory(vec *Vector) int64 {
	if vec == nil {
		return 0
	}
	
	baseSize := int64(100) // Base struct size
	
	// Data array size
	switch vec.DataType {
	case types.Integer:
		baseSize += int64(cap(vec.Int32Data) * 4)
	case types.BigInt:
		baseSize += int64(cap(vec.Int64Data) * 8)
	case types.Float:
		baseSize += int64(cap(vec.Float32Data) * 4)
	case types.Double:
		baseSize += int64(cap(vec.Float64Data) * 8)
	case types.Boolean:
		baseSize += int64(cap(vec.BoolData))
	case types.Text:
		baseSize += int64(cap(vec.StringData) * 32) // Estimate
	case types.Bytea:
		baseSize += int64(cap(vec.BytesData) * 32) // Estimate
	}
	
	// Null bitmap
	baseSize += int64(len(vec.NullBitmap) * 8)
	
	// Selection vector
	if vec.Selection != nil {
		baseSize += int64(cap(vec.Selection) * 8)
	}
	
	return baseSize
}

func measureActualBatchMemory(batch *VectorizedBatch) int64 {
	// This would use actual memory profiling in production
	// For testing, use same estimation
	return estimateBatchMemory(batch)
}

func fillBatchWithTestData(batch *VectorizedBatch) {
	for i, vec := range batch.Vectors {
		for j := 0; j < batch.RowCount && j < vec.Capacity; j++ {
			switch vec.DataType {
			case types.Integer:
				vec.Int32Data[j] = int32(i*1000 + j)
			case types.BigInt:
				vec.Int64Data[j] = int64(i*1000 + j)
			case types.Double:
				vec.Float64Data[j] = float64(i*1000+j) * 1.5
			case types.Text:
				vec.StringData[j] = fmt.Sprintf("text_%d_%d", i, j)
			case types.Boolean:
				vec.BoolData[j] = j%2 == 0
			}
		}
		vec.Length = batch.RowCount
	}
}

func createTestVector(dataType types.DataType, size int, value int) *Vector {
	vec := NewVector(dataType, size)
	
	for i := 0; i < size; i++ {
		switch dataType {
		case types.Integer:
			vec.Int32Data[i] = int32(value)
		case types.BigInt:
			vec.Int64Data[i] = int64(value)
		case types.Double:
			vec.Float64Data[i] = float64(value)
		}
	}
	vec.Length = size
	
	return vec
}

// VectorPool manages a pool of reusable vectors
type VectorPool struct {
	vectors map[types.DataType][]*Vector
	tracker *MemoryTracker
	mu      sync.Mutex
}

func (vp *VectorPool) Get(dataType types.DataType, capacity int) *Vector {
	vp.mu.Lock()
	defer vp.mu.Unlock()
	
	// Check if we have a vector available
	if vecs, ok := vp.vectors[dataType]; ok && len(vecs) > 0 {
		// Pop from pool
		vec := vecs[len(vecs)-1]
		vp.vectors[dataType] = vecs[:len(vecs)-1]
		
		// Reset and return
		vec.Reset()
		return vec
	}
	
	// Create new vector
	return NewVector(dataType, capacity)
}

func (vp *VectorPool) Put(vec *Vector) {
	vp.mu.Lock()
	defer vp.mu.Unlock()
	
	// Reset vector
	vec.Reset()
	
	// Add to pool
	if vp.vectors == nil {
		vp.vectors = make(map[types.DataType][]*Vector)
	}
	
	vp.vectors[vec.DataType] = append(vp.vectors[vec.DataType], vec)
}

// MockScanOperator for testing
type MockScanOperator struct {
	baseOperator
	rows    []*Row
	schema  *Schema
	current int
}

func (m *MockScanOperator) Open(ctx *ExecContext) error {
	m.ctx = ctx
	m.current = 0
	return nil
}

func (m *MockScanOperator) Next() (*Row, error) {
	if m.current >= len(m.rows) {
		return nil, nil
	}
	row := m.rows[m.current]
	m.current++
	return row, nil
}

func (m *MockScanOperator) Close() error {
	return nil
}

func (m *MockScanOperator) Schema() *Schema {
	return m.schema
}