package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// BitmapHeapScanOperator fetches rows from storage using a bitmap of row IDs.
// This is the final step in index intersection, converting bitmap to actual rows.
type BitmapHeapScanOperator struct {
	baseOperator
	table        *catalog.Table
	bitmapSource Operator // BitmapAnd, BitmapOr, or BitmapIndexScan
	storage      StorageBackend
	rowIDs       []RowID
	position     int
	isOpen       bool
}

// NewBitmapHeapScanOperator creates a new bitmap heap scan operator.
func NewBitmapHeapScanOperator(
	table *catalog.Table,
	bitmapSource Operator,
	storage StorageBackend,
) *BitmapHeapScanOperator {
	// Build schema from table columns
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name:     col.Name,
			Type:     col.DataType,
			Nullable: col.IsNullable,
		}
	}

	return &BitmapHeapScanOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:        table,
		bitmapSource: bitmapSource,
		storage:      storage,
	}
}

// Open initializes the bitmap heap scan.
func (op *BitmapHeapScanOperator) Open(ctx *ExecContext) error {
	op.ctx = ctx

	// Open the bitmap source
	if err := op.bitmapSource.Open(ctx); err != nil {
		return fmt.Errorf("failed to open bitmap source: %w", err)
	}

	// Get the bitmap and convert to row ID slice
	bitmapOp, ok := op.bitmapSource.(BitmapOperator)
	if !ok {
		return fmt.Errorf("invalid bitmap source type")
	}
	bitmap := bitmapOp.GetBitmap()

	op.rowIDs = bitmap.ToSlice()
	op.position = 0
	op.isOpen = true

	return nil
}

// Next returns the next row from the bitmap heap scan.
func (op *BitmapHeapScanOperator) Next() (*Row, error) {
	if !op.isOpen {
		return nil, fmt.Errorf("bitmap heap scan operator not open")
	}

	// Check if we have more row IDs
	if op.position >= len(op.rowIDs) {
		return nil, nil //nolint:nilnil // EOF - standard iterator pattern
	}

	// Get current row ID
	rowID := op.rowIDs[op.position]
	op.position++

	// Fetch the actual row from storage
	row, err := op.storage.GetRow(op.table.ID, rowID, op.ctx.SnapshotTS)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch row from storage: %w", err)
	}

	// Update statistics
	if op.ctx.Stats != nil {
		op.ctx.Stats.RowsRead++
	}

	return row, nil
}

// Close cleans up the bitmap heap scan.
func (op *BitmapHeapScanOperator) Close() error {
	if op.bitmapSource != nil {
		if err := op.bitmapSource.Close(); err != nil {
			return err
		}
	}

	op.isOpen = false
	op.rowIDs = nil
	op.position = 0

	return nil
}
