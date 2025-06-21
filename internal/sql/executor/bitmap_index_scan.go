package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
)

// BitmapIndexScanOperator scans an index and builds a bitmap of matching row IDs.
// This is used as the first step in index intersection operations.
type BitmapIndexScanOperator struct {
	baseOperator
	table      *catalog.Table
	index      *catalog.Index
	indexImpl  index.Index
	indexMgr   *index.Manager
	startKey   types.Value
	endKey     types.Value
	bitmap     *Bitmap
	isOpen     bool
	keyEncoder *index.KeyEncoder
}

// NewBitmapIndexScanOperator creates a new bitmap index scan operator.
func NewBitmapIndexScanOperator(
	table *catalog.Table,
	indexMeta *catalog.Index,
	indexMgr *index.Manager,
	startKey, endKey types.Value,
) *BitmapIndexScanOperator {
	schema := &Schema{
		Columns: []Column{{Name: "bitmap", Type: types.Unknown}}, // Bitmap is internal
	}

	return &BitmapIndexScanOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:      table,
		index:      indexMeta,
		indexMgr:   indexMgr,
		startKey:   startKey,
		endKey:     endKey,
		bitmap:     NewBitmap(),
		keyEncoder: &index.KeyEncoder{},
	}
}

// Open initializes the bitmap index scan.
func (op *BitmapIndexScanOperator) Open(ctx *ExecContext) error {
	op.ctx = ctx

	// Get the actual index implementation
	var err error
	op.indexImpl, err = op.indexMgr.GetIndex(op.table.SchemaName, op.table.TableName, op.index.Name)
	if err != nil {
		return fmt.Errorf("failed to get index implementation: %w", err)
	}

	// Build the bitmap by scanning the index
	if err := op.buildBitmap(); err != nil {
		return fmt.Errorf("failed to build bitmap: %w", err)
	}

	op.isOpen = true
	return nil
}

// buildBitmap scans the index and populates the bitmap with matching row IDs.
func (op *BitmapIndexScanOperator) buildBitmap() error {
	// Encode keys for range scan
	var startKeyBytes, endKeyBytes []byte
	var err error

	// Check if we have start/end keys based on their type
	if op.startKey != (types.Value{}) {
		startKeyBytes, err = op.keyEncoder.EncodeValue(op.startKey)
		if err != nil {
			return fmt.Errorf("failed to encode start key: %w", err)
		}
	}

	if op.endKey != (types.Value{}) {
		endKeyBytes, err = op.keyEncoder.EncodeValue(op.endKey)
		if err != nil {
			return fmt.Errorf("failed to encode end key: %w", err)
		}
	}

	// Scan the index range using the Range method
	entries, err := op.indexImpl.Range(startKeyBytes, endKeyBytes)
	if err != nil {
		return fmt.Errorf("index range scan failed: %w", err)
	}

	// Add all matching row IDs to the bitmap
	for _, entry := range entries {
		rowID, err := op.decodeRowID(entry.RowID)
		if err != nil {
			return fmt.Errorf("failed to decode row ID: %w", err)
		}
		op.bitmap.Add(rowID)
	}

	// Update statistics
	if op.ctx.Stats != nil {
		// IndexScans field may not exist, just increment RowsRead for now
		op.ctx.Stats.RowsRead += int64(op.bitmap.Count())
	}

	return nil
}

// GetBitmap returns the constructed bitmap.
// This is used by BitmapAnd/BitmapOr operators.
func (op *BitmapIndexScanOperator) GetBitmap() *Bitmap {
	return op.bitmap
}

// Next is not used for bitmap scans as they don't return rows directly.
func (op *BitmapIndexScanOperator) Next() (*Row, error) {
	return nil, fmt.Errorf("bitmap index scan does not support Next() - use GetBitmap()")
}

// Close cleans up the bitmap index scan.
func (op *BitmapIndexScanOperator) Close() error {
	op.isOpen = false
	op.bitmap.Clear()
	return nil
}

// decodeRowID converts an index entry row ID to an executor RowID.
func (op *BitmapIndexScanOperator) decodeRowID(value []byte) (RowID, error) {
	if len(value) != 6 {
		return RowID{}, fmt.Errorf("invalid row ID length: expected 6, got %d", len(value))
	}

	// Decode PageID (4 bytes, little endian)
	pageID := uint32(value[0]) | uint32(value[1])<<8 | uint32(value[2])<<16 | uint32(value[3])<<24

	// Decode SlotID (2 bytes, little endian)
	slotID := uint16(value[4]) | uint16(value[5])<<8

	return RowID{
		PageID: storage.PageID(pageID),
		SlotID: slotID,
	}, nil
}
