package executor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// MemoryTracker tracks memory usage for query execution
type MemoryTracker struct {
	limit       int64 // Memory limit in bytes
	current     int64 // Current memory usage
	peak        int64 // Peak memory usage
	allocations sync.Map // Track individual allocations
}

// NewMemoryTracker creates a new memory tracker
func NewMemoryTracker(limit int64) *MemoryTracker {
	return &MemoryTracker{
		limit: limit,
	}
}

// Allocate requests memory allocation
func (mt *MemoryTracker) Allocate(size int64, id string) error {
	// Check if allocation would exceed limit
	newUsage := atomic.AddInt64(&mt.current, size)
	if mt.limit > 0 && newUsage > mt.limit {
		// Rollback allocation
		atomic.AddInt64(&mt.current, -size)
		return fmt.Errorf("memory limit exceeded: requested %d bytes, limit %d bytes", size, mt.limit)
	}
	
	// Track allocation
	mt.allocations.Store(id, size)
	
	// Update peak usage
	for {
		peak := atomic.LoadInt64(&mt.peak)
		if newUsage <= peak || atomic.CompareAndSwapInt64(&mt.peak, peak, newUsage) {
			break
		}
	}
	
	return nil
}

// Release releases previously allocated memory
func (mt *MemoryTracker) Release(id string) {
	if value, ok := mt.allocations.LoadAndDelete(id); ok {
		size := value.(int64)
		atomic.AddInt64(&mt.current, -size)
	}
}

// CurrentUsage returns current memory usage
func (mt *MemoryTracker) CurrentUsage() int64 {
	return atomic.LoadInt64(&mt.current)
}

// PeakUsage returns peak memory usage
func (mt *MemoryTracker) PeakUsage() int64 {
	return atomic.LoadInt64(&mt.peak)
}

// SetLimit updates the memory limit
func (mt *MemoryTracker) SetLimit(limit int64) {
	atomic.StoreInt64(&mt.limit, limit)
}

// Reset resets usage statistics
func (mt *MemoryTracker) Reset() {
	atomic.StoreInt64(&mt.current, 0)
	atomic.StoreInt64(&mt.peak, 0)
	mt.allocations.Range(func(key, value interface{}) bool {
		mt.allocations.Delete(key)
		return true
	})
}

// memoryTrackingOperator wraps an operator with memory tracking
type memoryTrackingOperator struct {
	child   Operator
	tracker *MemoryTracker
	id      string
}

// Schema returns the operator schema
func (mto *memoryTrackingOperator) Schema() *Schema {
	return mto.child.Schema()
}

// Open initializes the operator
func (mto *memoryTrackingOperator) Open(ctx *ExecContext) error {
	// Estimate memory usage based on schema
	estimatedSize := estimateOperatorMemory(mto.child)
	mto.id = fmt.Sprintf("op_%p", mto)
	
	if err := mto.tracker.Allocate(estimatedSize, mto.id); err != nil {
		return err
	}
	
	return mto.child.Open(ctx)
}

// Next returns the next row
func (mto *memoryTrackingOperator) Next() (*Row, error) {
	return mto.child.Next()
}

// Close releases resources
func (mto *memoryTrackingOperator) Close() error {
	mto.tracker.Release(mto.id)
	return mto.child.Close()
}

// VectorizedMemoryTracker tracks memory for vectorized operations
type VectorizedMemoryTracker struct {
	*MemoryTracker
	batchAllocations sync.Map // Track batch-specific allocations
}

// NewVectorizedMemoryTracker creates a memory tracker for vectorized operations
func NewVectorizedMemoryTracker(limit int64) *VectorizedMemoryTracker {
	return &VectorizedMemoryTracker{
		MemoryTracker: NewMemoryTracker(limit),
	}
}

// AllocateBatch allocates memory for a vectorized batch
func (vmt *VectorizedMemoryTracker) AllocateBatch(batchSize int, schema *Schema) (string, error) {
	// Calculate batch memory requirements
	size := calculateBatchMemory(batchSize, schema)
	batchID := fmt.Sprintf("batch_%d", time.Now().UnixNano())
	
	if err := vmt.Allocate(size, batchID); err != nil {
		return "", err
	}
	
	vmt.batchAllocations.Store(batchID, size)
	return batchID, nil
}

// ReleaseBatch releases memory for a batch
func (vmt *VectorizedMemoryTracker) ReleaseBatch(batchID string) {
	if value, ok := vmt.batchAllocations.LoadAndDelete(batchID); ok {
		_ = value.(int64) // size might be used for logging later
		vmt.Release(batchID)
	}
}

// calculateBatchMemory estimates memory for a vectorized batch
func calculateBatchMemory(batchSize int, schema *Schema) int64 {
	var totalSize int64
	
	for _, col := range schema.Columns {
		// Base type size
		typeSize := getTypeSize(col.Type)
		columnSize := int64(batchSize) * typeSize
		
		// Add overhead for null bitmap
		nullBitmapSize := int64((batchSize + 63) / 64 * 8) // 1 bit per value, rounded to 8 bytes
		
		// Add overhead for variable-length types
		if isVariableLength(col.Type) {
			// Estimate average size for strings/bytes
			columnSize = int64(batchSize) * 64 // Assume 64 bytes average
		}
		
		totalSize += columnSize + nullBitmapSize
	}
	
	// Add overhead for selection vector and metadata
	totalSize += int64(batchSize) * 4 // Selection vector (int32)
	totalSize += 1024                  // Metadata overhead
	
	return totalSize
}

// getTypeSize returns the size in bytes for a fixed-size type
func getTypeSize(dataType types.DataType) int64 {
	switch dataType {
	case types.Boolean:
		return 1
	case types.Integer:
		return 4
	case types.BigInt:
		return 8
	case types.Float:
		return 4
	case types.Double:
		return 8
	case types.Date, types.Timestamp:
		return 8
	default:
		// Variable length or unknown
		return 0
	}
}

// isVariableLength checks if a type is variable length
func isVariableLength(dataType types.DataType) bool {
	switch dataType {
	case types.Text, types.Varchar(0), types.Bytea:
		return true
	default:
		return false
	}
}

// estimateOperatorMemory estimates memory usage for an operator
func estimateOperatorMemory(op Operator) int64 {
	// Basic estimation based on schema and operator type
	schema := op.Schema()
	if schema == nil {
		return 1024 * 1024 // Default 1MB
	}
	
	// Estimate row size
	var rowSize int64
	for _, col := range schema.Columns {
		if size := getTypeSize(col.Type); size > 0 {
			rowSize += size
		} else {
			rowSize += 64 // Estimate for variable types
		}
	}
	
	// Estimate based on operator type
	switch op.(type) {
	case *ScanOperator, *VectorizedScanOperator:
		// Scan operators buffer rows
		return rowSize * 1000 // Buffer for ~1000 rows
	case *HashJoinOperator:
		// Hash joins need to store hash table
		return rowSize * 10000 // Estimate 10K rows in hash table
	case *SortOperator:
		// Sort may need to buffer all rows
		return rowSize * 100000 // Estimate 100K rows
	default:
		// Default estimation
		return rowSize * 100 // Buffer for ~100 rows
	}
}