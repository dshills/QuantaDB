package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
)

// IndexScanOperator executes index scans to retrieve rows.
type IndexScanOperator struct {
	baseOperator
	table       *catalog.Table
	index       *catalog.Index
	indexImpl   index.Index
	indexMgr    *index.Manager
	storage     StorageBackend
	startKey    planner.Expression
	endKey      planner.Expression
	entries     []index.IndexEntry
	position    int
	keyEncoder  *index.KeyEncoder
	isOpen      bool
}

// NewIndexScanOperator creates a new index scan operator.
func NewIndexScanOperator(
	table *catalog.Table,
	indexMeta *catalog.Index,
	indexMgr *index.Manager,
	storage StorageBackend,
	startKey, endKey planner.Expression,
) *IndexScanOperator {
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

	return &IndexScanOperator{
		baseOperator: baseOperator{schema: schema},
		table:        table,
		index:        indexMeta,
		indexMgr:     indexMgr,
		storage:      storage,
		startKey:     startKey,
		endKey:       endKey,
		entries:      nil,
		position:     0,
		keyEncoder:   &index.KeyEncoder{},
		isOpen:       false,
	}
}

// Open initializes the index scan operator.
func (op *IndexScanOperator) Open(ctx *ExecContext) error {
	if op.isOpen {
		return fmt.Errorf("index scan operator already open")
	}

	op.ctx = ctx

	// Get the actual index implementation
	var err error
	op.indexImpl, err = op.indexMgr.GetIndex(op.table.SchemaName, op.table.TableName, op.index.Name)
	if err != nil {
		return fmt.Errorf("failed to get index implementation: %w", err)
	}

	// Evaluate start and end key expressions
	var startKeyBytes, endKeyBytes []byte

	if op.startKey != nil {
		startValue, err := op.evaluateExpression(op.startKey)
		if err != nil {
			return fmt.Errorf("failed to evaluate start key: %w", err)
		}
		startKeyBytes, err = op.keyEncoder.EncodeValue(startValue)
		if err != nil {
			return fmt.Errorf("failed to encode start key: %w", err)
		}
	}

	if op.endKey != nil {
		endValue, err := op.evaluateExpression(op.endKey)
		if err != nil {
			return fmt.Errorf("failed to evaluate end key: %w", err)
		}
		endKeyBytes, err = op.keyEncoder.EncodeValue(endValue)
		if err != nil {
			return fmt.Errorf("failed to encode end key: %w", err)
		}
	}

	// Perform index range scan
	if startKeyBytes != nil && endKeyBytes != nil {
		// Range scan
		op.entries, err = op.indexImpl.Range(startKeyBytes, endKeyBytes)
		if err != nil {
			return fmt.Errorf("failed to perform index range scan: %w", err)
		}
	} else if startKeyBytes != nil {
		// Scan from start key to end
		op.entries, err = op.indexImpl.Range(startKeyBytes, nil)
		if err != nil {
			return fmt.Errorf("failed to perform index scan from start: %w", err)
		}
	} else if endKeyBytes != nil {
		// Scan from beginning to end key
		op.entries, err = op.indexImpl.Range(nil, endKeyBytes)
		if err != nil {
			return fmt.Errorf("failed to perform index scan to end: %w", err)
		}
	} else {
		// Full index scan
		op.entries, err = op.indexImpl.Range(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to perform full index scan: %w", err)
		}
	}

	op.isOpen = true
	op.position = 0

	return nil
}

// Next returns the next row from the index scan.
func (op *IndexScanOperator) Next() (*Row, error) {
	if !op.isOpen {
		return nil, fmt.Errorf("index scan operator not open")
	}

	// Check if we have more entries
	if op.position >= len(op.entries) {
		return nil, nil // No more rows
	}

	// Get current index entry
	entry := op.entries[op.position]
	op.position++

	// Convert index entry RowID to storage RowID
	// The index entry RowID should contain the row location
	rowID, err := op.decodeRowID(entry.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode row ID from index entry: %w", err)
	}

	// Fetch the actual row from storage
	row, err := op.storage.GetRow(op.table.ID, rowID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch row from storage: %w", err)
	}

	// Update statistics
	if op.ctx.Stats != nil {
		op.ctx.Stats.RowsRead++
		// Note: BytesRead not tracked for index scans since we don't have direct access to row size
	}

	return row, nil
}

// Close cleans up the index scan operator.
func (op *IndexScanOperator) Close() error {
	op.isOpen = false
	op.entries = nil
	op.position = 0
	return nil
}

// Schema returns the output schema of the index scan.
func (op *IndexScanOperator) Schema() *Schema {
	return op.schema
}

// evaluateExpression evaluates a planner expression to get a value.
func (op *IndexScanOperator) evaluateExpression(expr planner.Expression) (types.Value, error) {
	switch e := expr.(type) {
	case *planner.Literal:
		return e.Value, nil
	case *planner.ColumnRef:
		// For index scans, column references in start/end keys should not occur
		// since they represent constant values from the WHERE clause
		return types.Value{}, fmt.Errorf("column references not supported in index key expressions")
	default:
		return types.Value{}, fmt.Errorf("unsupported expression type in index key: %T", expr)
	}
}

// decodeRowID converts an index entry value to a RowID.
func (op *IndexScanOperator) decodeRowID(value []byte) (RowID, error) {
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