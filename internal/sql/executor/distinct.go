package executor

import (
	"fmt"
	"hash/fnv"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// DistinctOperator implements the DISTINCT operation by removing duplicate rows.
type DistinctOperator struct {
	baseOperator
	child       Operator
	seen        map[uint64]struct{} // Set of hash values for seen rows
	buffer      []*Row              // Buffer for all unique rows
	bufferIndex int                 // Current position in buffer during iteration
	initialized bool
}

// NewDistinctOperator creates a new distinct operator.
func NewDistinctOperator(child Operator) *DistinctOperator {
	// Use child's schema since DISTINCT doesn't change the schema
	return &DistinctOperator{
		baseOperator: baseOperator{
			schema: child.Schema(),
		},
		child: child,
	}
}

// Open initializes the distinct operator.
func (d *DistinctOperator) Open(ctx *ExecContext) error {
	d.ctx = ctx
	d.seen = make(map[uint64]struct{})
	d.buffer = make([]*Row, 0)
	d.bufferIndex = 0
	d.initialized = false

	// Open child operator
	if err := d.child.Open(ctx); err != nil {
		return fmt.Errorf("failed to open child operator: %w", err)
	}

	// Process all input rows and collect unique ones
	if err := d.collectUniqueRows(); err != nil {
		return fmt.Errorf("failed to collect unique rows: %w", err)
	}

	d.initialized = true
	return nil
}

// collectUniqueRows reads all rows from child and stores unique ones.
func (d *DistinctOperator) collectUniqueRows() error {
	for {
		row, err := d.child.Next()
		if err != nil {
			return fmt.Errorf("error reading from child: %w", err)
		}
		if row == nil {
			break // End of input
		}

		// Compute hash of the row
		hash := d.hashRow(row)

		// Check if we've seen this row before
		if _, exists := d.seen[hash]; !exists {
			// First time seeing this row - add to buffer
			d.seen[hash] = struct{}{}
			
			// Make a copy of the row to avoid data corruption
			rowCopy := &Row{
				Values: make([]types.Value, len(row.Values)),
			}
			copy(rowCopy.Values, row.Values)
			
			d.buffer = append(d.buffer, rowCopy)
		}
	}

	return nil
}

// hashRow computes a hash value for a row based on all its column values.
func (d *DistinctOperator) hashRow(row *Row) uint64 {
	hasher := fnv.New64()
	
	for i, val := range row.Values {
		if i > 0 {
			// Add separator between values to avoid collisions
			hasher.Write([]byte{0xFF})
		}
		writeValueToHasher(hasher, val)
	}
	
	return hasher.Sum64()
}

// Next returns the next unique row.
func (d *DistinctOperator) Next() (*Row, error) {
	if !d.initialized {
		return nil, fmt.Errorf("distinct operator not initialized")
	}

	if d.bufferIndex >= len(d.buffer) {
		return nil, nil // EOF
	}

	row := d.buffer[d.bufferIndex]
	d.bufferIndex++

	if d.ctx != nil && d.ctx.Stats != nil {
		d.ctx.Stats.RowsReturned++
	}

	return row, nil
}

// Close cleans up the distinct operator.
func (d *DistinctOperator) Close() error {
	d.seen = nil
	d.buffer = nil
	return d.child.Close()
}

// The writeValueToHasher function is already defined in hash.go