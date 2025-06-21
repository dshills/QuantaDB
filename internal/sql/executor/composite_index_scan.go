package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
)

// CompositeIndexScanOperator executes composite (multi-column) index scans.
type CompositeIndexScanOperator struct {
	baseOperator
	table       *catalog.Table
	index       *catalog.Index
	indexImpl   index.Index
	indexMgr    *index.Manager
	storage     StorageBackend
	startValues []types.Value
	endValues   []types.Value
	entries     []index.IndexEntry
	position    int
	keyEncoder  *index.KeyEncoder
	isOpen      bool
}

// NewCompositeIndexScanOperator creates a new composite index scan operator.
func NewCompositeIndexScanOperator(
	table *catalog.Table,
	indexMeta *catalog.Index,
	indexMgr *index.Manager,
	storage StorageBackend,
	startValues, endValues []types.Value,
) *CompositeIndexScanOperator {
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

	return &CompositeIndexScanOperator{
		baseOperator: baseOperator{schema: schema},
		table:        table,
		index:        indexMeta,
		indexMgr:     indexMgr,
		storage:      storage,
		startValues:  startValues,
		endValues:    endValues,
		entries:      nil,
		position:     0,
		keyEncoder:   &index.KeyEncoder{},
		isOpen:       false,
	}
}

// Open initializes the composite index scan operator.
func (op *CompositeIndexScanOperator) Open(ctx *ExecContext) error {
	if op.isOpen {
		return fmt.Errorf("composite index scan operator already open")
	}

	op.ctx = ctx

	// Get the actual index implementation
	var err error
	op.indexImpl, err = op.indexMgr.GetIndex(op.table.SchemaName, op.table.TableName, op.index.Name)
	if err != nil {
		return fmt.Errorf("failed to get index implementation: %w", err)
	}

	// Encode composite keys for range scan
	var startKeyBytes, endKeyBytes []byte

	if len(op.startValues) > 0 {
		startKeyBytes, err = op.keyEncoder.EncodeMultiColumn(op.startValues)
		if err != nil {
			return fmt.Errorf("failed to encode start composite key: %w", err)
		}
	}

	if len(op.endValues) > 0 {
		endKeyBytes, err = op.keyEncoder.EncodeMultiColumn(op.endValues)
		if err != nil {
			return fmt.Errorf("failed to encode end composite key: %w", err)
		}
	}

	// Perform index range scan
	if startKeyBytes != nil && endKeyBytes != nil {
		// Range scan
		op.entries, err = op.indexImpl.Range(startKeyBytes, endKeyBytes)
		if err != nil {
			return fmt.Errorf("failed to perform composite index range scan: %w", err)
		}
	} else if startKeyBytes != nil {
		// Scan from start key to end
		op.entries, err = op.indexImpl.Range(startKeyBytes, nil)
		if err != nil {
			return fmt.Errorf("failed to perform composite index scan from start: %w", err)
		}
	} else if endKeyBytes != nil {
		// Scan from beginning to end key
		op.entries, err = op.indexImpl.Range(nil, endKeyBytes)
		if err != nil {
			return fmt.Errorf("failed to perform composite index scan to end: %w", err)
		}
	} else {
		// Full index scan
		op.entries, err = op.indexImpl.Range(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to perform full composite index scan: %w", err)
		}
	}

	op.isOpen = true
	op.position = 0

	return nil
}

// Next returns the next row from the composite index scan.
func (op *CompositeIndexScanOperator) Next() (*Row, error) {
	if !op.isOpen {
		return nil, fmt.Errorf("composite index scan operator not open")
	}

	// Check if we have more entries
	if op.position >= len(op.entries) {
		return nil, nil // nolint:nilnil // EOF - standard iterator pattern
	}

	// Get current index entry
	entry := op.entries[op.position]
	op.position++

	// Convert index entry RowID to storage RowID
	rowID, err := op.decodeRowID(entry.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode row ID from composite index entry: %w", err)
	}

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

// Close cleans up the composite index scan operator.
func (op *CompositeIndexScanOperator) Close() error {
	op.isOpen = false
	op.entries = nil
	op.position = 0
	return nil
}

// Schema returns the output schema of the composite index scan.
func (op *CompositeIndexScanOperator) Schema() *Schema {
	return op.schema
}

// decodeRowID converts an index entry value to a RowID.
func (op *CompositeIndexScanOperator) decodeRowID(value []byte) (RowID, error) {
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
