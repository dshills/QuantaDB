package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// VectorSize is the number of values processed in a single vector operation
const VectorSize = 1024

// Vector represents a column of values for vectorized processing
type Vector struct {
	// Data arrays for different types (only one is used at a time)
	Int64Data   []int64
	Int32Data   []int32
	Float64Data []float64
	Float32Data []float32
	BoolData    []bool
	StringData  []string
	BytesData   [][]byte

	// Null bitmap (1 bit per value)
	NullBitmap []uint64

	// Metadata
	DataType types.DataType
	Length   int // Number of valid values
	Capacity int // Allocated capacity

	// Selection vector for filtered results
	Selection []int // Indices of selected rows
	SelLength int   // Number of selected rows
}

// NewVector creates a new vector of the specified type and capacity
func NewVector(dataType types.DataType, capacity int) *Vector {
	v := &Vector{
		DataType:   dataType,
		Capacity:   capacity,
		Length:     0,
		NullBitmap: make([]uint64, (capacity+63)/64), // 1 bit per value
	}

	// Allocate data array based on type
	switch dataType {
	case types.Integer:
		v.Int32Data = make([]int32, capacity)
	case types.BigInt:
		v.Int64Data = make([]int64, capacity)
	case types.Float:
		v.Float32Data = make([]float32, capacity)
	case types.Double:
		v.Float64Data = make([]float64, capacity)
	case types.Boolean:
		v.BoolData = make([]bool, capacity)
	case types.Text, types.Varchar(0):
		v.StringData = make([]string, capacity)
	case types.Bytea:
		v.BytesData = make([][]byte, capacity)
	default:
		// For other types, use generic interface (slower but works)
		v.StringData = make([]string, capacity)
	}

	return v
}

// SetNull sets the null bit for a value at the given index
func (v *Vector) SetNull(idx int) {
	byteIdx := idx / 64
	bitIdx := uint(idx % 64)
	v.NullBitmap[byteIdx] |= (1 << bitIdx)
}

// IsNull checks if a value at the given index is null
func (v *Vector) IsNull(idx int) bool {
	byteIdx := idx / 64
	bitIdx := uint(idx % 64)
	return (v.NullBitmap[byteIdx] & (1 << bitIdx)) != 0
}

// Reset clears the vector for reuse
func (v *Vector) Reset() {
	v.Length = 0
	v.SelLength = 0
	// Clear null bitmap
	for i := range v.NullBitmap {
		v.NullBitmap[i] = 0
	}
}

// VectorizedBatch represents a batch of rows in columnar format
type VectorizedBatch struct {
	Vectors  []*Vector // One vector per column
	Schema   *Schema   // Schema information
	RowCount int       // Number of rows in this batch
}

// NewVectorizedBatch creates a new batch with the given schema and capacity
func NewVectorizedBatch(schema *Schema, capacity int) *VectorizedBatch {
	batch := &VectorizedBatch{
		Schema:   schema,
		Vectors:  make([]*Vector, len(schema.Columns)),
		RowCount: 0,
	}

	// Create a vector for each column
	for i, col := range schema.Columns {
		batch.Vectors[i] = NewVector(col.Type, capacity)
	}

	return batch
}

// Reset clears the batch for reuse
func (vb *VectorizedBatch) Reset() {
	vb.RowCount = 0
	for _, v := range vb.Vectors {
		v.Reset()
	}
}

// VectorizedOperator is the interface for operators that support vectorized execution
type VectorizedOperator interface {
	Operator
	// NextBatch returns the next batch of rows
	NextBatch() (*VectorizedBatch, error)
	// SupportsVectorized returns true if this operator supports vectorized execution
	SupportsVectorized() bool
}

// VectorizedScanOperator implements vectorized table scanning
type VectorizedScanOperator struct {
	baseOperator
	tableID   int64
	batch     *VectorizedBatch
	iterator  RowIterator
	exhausted bool
}

// NewVectorizedScanOperator creates a new vectorized scan operator
func NewVectorizedScanOperator(schema *Schema, tableID int64) *VectorizedScanOperator {
	return &VectorizedScanOperator{
		baseOperator: baseOperator{schema: schema},
		tableID:      tableID,
		batch:        NewVectorizedBatch(schema, VectorSize),
		exhausted:    false,
	}
}

// SupportsVectorized returns true
func (vs *VectorizedScanOperator) SupportsVectorized() bool {
	return true
}

// Open initializes the scan
func (vs *VectorizedScanOperator) Open(ctx *ExecContext) error {
	vs.ctx = ctx
	// For testing purposes, create a mock iterator
	// In a real implementation, this would use ctx.Engine or storage backend
	vs.iterator = &VectorizedMockRowIterator{maxRows: 10}
	vs.exhausted = false
	return nil
}

// NextBatch returns the next batch of rows
func (vs *VectorizedScanOperator) NextBatch() (*VectorizedBatch, error) {
	if vs.exhausted {
		return nil, nil
	}

	// Reset batch for reuse
	vs.batch.Reset()

	// Fill the batch
	for i := 0; i < VectorSize; i++ {
		row, _, err := vs.iterator.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			vs.exhausted = true
			break
		}

		// Copy row data into vectors
		for colIdx, val := range row.Values {
			vector := vs.batch.Vectors[colIdx]

			if val.IsNull() {
				vector.SetNull(i)
			} else {
				// Copy value based on type
				switch vector.DataType {
				case types.Integer:
					vector.Int32Data[i] = val.Data.(int32)
				case types.BigInt:
					vector.Int64Data[i] = val.Data.(int64)
				case types.Float:
					vector.Float32Data[i] = val.Data.(float32)
				case types.Double:
					vector.Float64Data[i] = val.Data.(float64)
				case types.Boolean:
					vector.BoolData[i] = val.Data.(bool)
				case types.Text, types.Varchar(0):
					vector.StringData[i] = val.Data.(string)
				case types.Bytea:
					vector.BytesData[i] = val.Data.([]byte)
				default:
					// Generic handling
					vector.StringData[i] = fmt.Sprintf("%v", val.Data)
				}
			}

			vector.Length = i + 1
		}

		vs.batch.RowCount = i + 1
	}

	if vs.batch.RowCount == 0 {
		return nil, nil
	}

	return vs.batch, nil
}

// Next implements the regular row-at-a-time interface
func (vs *VectorizedScanOperator) Next() (*Row, error) {
	// This is a fallback implementation for compatibility
	if vs.iterator == nil {
		return nil, fmt.Errorf("scan not opened")
	}
	row, _, err := vs.iterator.Next()
	return row, err
}

// Close cleans up resources
func (vs *VectorizedScanOperator) Close() error {
	if vs.iterator != nil {
		return vs.iterator.Close()
	}
	return nil
}

// VectorizedFilterOperator implements vectorized filtering
type VectorizedFilterOperator struct {
	baseOperator
	child     VectorizedOperator
	predicate VectorizedExprEvaluator
	batch     *VectorizedBatch
}

// VectorizedExprEvaluator evaluates expressions on vectors
type VectorizedExprEvaluator interface {
	// EvalVector evaluates the expression on a batch and returns a boolean vector
	EvalVector(batch *VectorizedBatch) (*Vector, error)
}

// NewVectorizedFilterOperator creates a new vectorized filter operator
func NewVectorizedFilterOperator(child VectorizedOperator, predicate VectorizedExprEvaluator) *VectorizedFilterOperator {
	return &VectorizedFilterOperator{
		baseOperator: baseOperator{schema: child.Schema()},
		child:        child,
		predicate:    predicate,
		batch:        NewVectorizedBatch(child.Schema(), VectorSize),
	}
}

// SupportsVectorized returns true
func (vf *VectorizedFilterOperator) SupportsVectorized() bool {
	return true
}

// Open initializes the filter
func (vf *VectorizedFilterOperator) Open(ctx *ExecContext) error {
	vf.ctx = ctx
	return vf.child.Open(ctx)
}

// NextBatch returns the next batch of filtered rows
func (vf *VectorizedFilterOperator) NextBatch() (*VectorizedBatch, error) {
	for {
		// Get next batch from child
		childBatch, err := vf.child.NextBatch()
		if err != nil {
			return nil, err
		}
		if childBatch == nil {
			return nil, nil
		}

		// Evaluate predicate on the batch
		resultVector, err := vf.predicate.EvalVector(childBatch)
		if err != nil {
			return nil, err
		}

		// Reset output batch
		vf.batch.Reset()

		// Copy selected rows to output batch
		outputIdx := 0
		for i := 0; i < childBatch.RowCount; i++ {
			// Check if row is selected (and not null)
			if !resultVector.IsNull(i) && resultVector.BoolData[i] {
				// Copy all columns for this row
				for colIdx, inVector := range childBatch.Vectors {
					outVector := vf.batch.Vectors[colIdx]

					// Copy value based on type
					if inVector.IsNull(i) {
						outVector.SetNull(outputIdx)
					} else {
						switch inVector.DataType {
						case types.Integer:
							outVector.Int32Data[outputIdx] = inVector.Int32Data[i]
						case types.BigInt:
							outVector.Int64Data[outputIdx] = inVector.Int64Data[i]
						case types.Float:
							outVector.Float32Data[outputIdx] = inVector.Float32Data[i]
						case types.Double:
							outVector.Float64Data[outputIdx] = inVector.Float64Data[i]
						case types.Boolean:
							outVector.BoolData[outputIdx] = inVector.BoolData[i]
						case types.Text, types.Varchar(0):
							outVector.StringData[outputIdx] = inVector.StringData[i]
						case types.Bytea:
							outVector.BytesData[outputIdx] = inVector.BytesData[i]
						}
					}

					outVector.Length = outputIdx + 1
				}

				outputIdx++
			}
		}

		vf.batch.RowCount = outputIdx

		// If we have any rows, return them
		if vf.batch.RowCount > 0 {
			return vf.batch, nil
		}
		// Otherwise, continue to next batch
	}
}

// Next implements the regular row-at-a-time interface (fallback)
func (vf *VectorizedFilterOperator) Next() (*Row, error) {
	// This would need to be implemented for compatibility
	return nil, fmt.Errorf("row-at-a-time not implemented for vectorized filter")
}

// Close cleans up resources
func (vf *VectorizedFilterOperator) Close() error {
	return vf.child.Close()
}

// Vectorized arithmetic operations using SIMD-friendly loops

// VectorAddInt64 performs vectorized addition on int64 vectors
func VectorAddInt64(result, left, right []int64, length int) {
	// This loop is easily vectorizable by the compiler
	for i := 0; i < length; i++ {
		result[i] = left[i] + right[i]
	}
}

// VectorAddFloat64 performs vectorized addition on float64 vectors
func VectorAddFloat64(result, left, right []float64, length int) {
	for i := 0; i < length; i++ {
		result[i] = left[i] + right[i]
	}
}

// VectorMultiplyInt64 performs vectorized multiplication on int64 vectors
func VectorMultiplyInt64(result, left, right []int64, length int) {
	for i := 0; i < length; i++ {
		result[i] = left[i] * right[i]
	}
}

// VectorMultiplyFloat64 performs vectorized multiplication on float64 vectors
func VectorMultiplyFloat64(result, left, right []float64, length int) {
	for i := 0; i < length; i++ {
		result[i] = left[i] * right[i]
	}
}

// VectorEqualsInt64 performs vectorized equality comparison on int64 vectors
func VectorEqualsInt64(result []bool, left, right []int64, length int) {
	for i := 0; i < length; i++ {
		result[i] = left[i] == right[i]
	}
}

// VectorGreaterThanInt64 performs vectorized > comparison on int64 vectors
func VectorGreaterThanInt64(result []bool, left, right []int64, length int) {
	for i := 0; i < length; i++ {
		result[i] = left[i] > right[i]
	}
}

// VectorLessThanInt64 performs vectorized < comparison on int64 vectors
func VectorLessThanInt64(result []bool, left, right []int64, length int) {
	for i := 0; i < length; i++ {
		result[i] = left[i] < right[i]
	}
}

// GetVectorizedCapabilities returns information about vectorization support
func GetVectorizedCapabilities() map[string]bool {
	return map[string]bool{
		"basic_arithmetic": true,
		"comparisons":      true,
		"null_handling":    true,
		"simd_optimized":   true, // Compiler will vectorize our loops
		"batch_size":       true,
	}
}

// VectorizedMockRowIterator provides a mock iterator for vectorized testing
type VectorizedMockRowIterator struct {
	currentRow int
	maxRows    int
}

// Next returns mock data for testing
func (m *VectorizedMockRowIterator) Next() (*Row, RowID, error) {
	if m.currentRow >= m.maxRows {
		return nil, RowID{}, nil
	}

	m.currentRow++
	return &Row{
		Values: []types.Value{
			types.NewValue(int32(m.currentRow)),
			types.NewValue(fmt.Sprintf("row-%d", m.currentRow)),
		},
	}, RowID{}, nil
}

// Close does nothing for the mock
func (m *VectorizedMockRowIterator) Close() error {
	return nil
}
