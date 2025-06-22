package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/storage"
)

// indexScanHelper provides common functionality for index scan operations.
type indexScanHelper struct {
	storage       StorageBackend
	predicateEval ExprEvaluator
	ctx           *ExecContext
	tableID       int64
}

// newIndexScanHelper creates a new helper for index scan operations.
func newIndexScanHelper(storage StorageBackend, predicateEval ExprEvaluator, ctx *ExecContext, tableID int64) *indexScanHelper {
	return &indexScanHelper{
		storage:       storage,
		predicateEval: predicateEval,
		ctx:           ctx,
		tableID:       tableID,
	}
}

// processIndexEntry processes a single index entry and returns the corresponding row if it matches predicates.
// Returns:
// - row: the fetched row if it matches all predicates
// - skip: true if this entry should be skipped (predicate failed)
// - err: any error that occurred
func (h *indexScanHelper) processIndexEntry(entry index.IndexEntry) (*Row, bool, error) {
	// Convert index entry RowID to storage RowID
	rowID, err := decodeRowID(entry.RowID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decode row ID from index entry: %w", err)
	}

	// Fetch the actual row from storage
	row, err := h.storage.GetRow(h.tableID, rowID, h.ctx.SnapshotTS)
	if err != nil {
		return nil, false, fmt.Errorf("failed to fetch row from storage: %w", err)
	}

	// Evaluate pushed predicates if any
	if h.predicateEval != nil {
		// Evaluate the predicate
		result, err := h.predicateEval.Eval(row, h.ctx)
		if err != nil {
			return nil, false, fmt.Errorf("failed to evaluate pushed predicate: %w", err)
		}

		// Skip this row if predicate is false or NULL
		boolResult, err := result.AsBool()
		if err != nil || !boolResult {
			// Skip to next row
			return nil, true, nil
		}
	}

	// Update statistics
	if h.ctx.Stats != nil {
		h.ctx.Stats.RowsRead++
	}

	return row, false, nil
}

// decodeRowID converts an index entry value to a RowID.
// This is used by multiple index scan operators.
func decodeRowID(value []byte) (RowID, error) {
	// For now, assume the value directly contains the RowID
	// This is a simplified implementation - in a real system, you might need
	// more sophisticated encoding/decoding based on your storage format

	if len(value) < 6 { // PageID (4 bytes) + SlotID (2 bytes)
		return RowID{}, fmt.Errorf("invalid row ID value length: %d", len(value))
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
