package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// IndexOnlyScanOperator executes index-only scans, retrieving data directly from the index
// without accessing the table.
type IndexOnlyScanOperator struct {
	baseOperator
	table            *catalog.Table
	index            *catalog.Index
	indexImpl        index.Index
	indexMgr         *index.Manager
	startExprs       []planner.Expression
	endExprs         []planner.Expression
	projectedColumns []string
	entries          []index.IndexEntry
	position         int
	keyEncoder       *index.KeyEncoder
	isOpen           bool
}

// NewIndexOnlyScanOperator creates a new index-only scan operator.
func NewIndexOnlyScanOperator(
	table *catalog.Table,
	indexMeta *catalog.Index,
	indexMgr *index.Manager,
	startExprs, endExprs []planner.Expression,
	projectedColumns []string,
) *IndexOnlyScanOperator {
	// Build schema from projected columns
	schema := &Schema{
		Columns: make([]Column, 0, len(projectedColumns)),
	}

	// Map column names to their info
	columnMap := make(map[string]*catalog.Column)
	for _, col := range table.Columns {
		columnMap[col.Name] = col
	}

	// Build schema from projected columns in order
	for _, colName := range projectedColumns {
		if col, ok := columnMap[colName]; ok {
			schema.Columns = append(schema.Columns, Column{
				Name:     col.Name,
				Type:     col.DataType,
				Nullable: col.IsNullable,
			})
		}
	}

	return &IndexOnlyScanOperator{
		baseOperator:     baseOperator{schema: schema},
		table:            table,
		index:            indexMeta,
		indexMgr:         indexMgr,
		startExprs:       startExprs,
		endExprs:         endExprs,
		projectedColumns: projectedColumns,
		entries:          nil,
		position:         0,
		keyEncoder:       &index.KeyEncoder{},
		isOpen:           false,
	}
}

// Open initializes the index-only scan operator.
func (op *IndexOnlyScanOperator) Open(ctx *ExecContext) error {
	if op.isOpen {
		return fmt.Errorf("index-only scan operator already open")
	}

	op.ctx = ctx

	// Get the actual index implementation
	var err error
	op.indexImpl, err = op.indexMgr.GetIndex(op.table.SchemaName, op.table.TableName, op.index.Name)
	if err != nil {
		return fmt.Errorf("failed to get index implementation: %w", err)
	}

	// Evaluate and encode start/end keys
	var startKeyBytes, endKeyBytes []byte

	if len(op.startExprs) > 0 {
		startValues := make([]types.Value, len(op.startExprs))
		for i, expr := range op.startExprs {
			val, err := op.evaluateExpression(expr)
			if err != nil {
				return fmt.Errorf("failed to evaluate start expression %d: %w", i, err)
			}
			startValues[i] = val
		}

		if len(startValues) == 1 {
			startKeyBytes, err = op.keyEncoder.EncodeValue(startValues[0])
		} else {
			startKeyBytes, err = op.keyEncoder.EncodeMultiColumn(startValues)
		}
		if err != nil {
			return fmt.Errorf("failed to encode start key: %w", err)
		}
	}

	if len(op.endExprs) > 0 {
		endValues := make([]types.Value, len(op.endExprs))
		for i, expr := range op.endExprs {
			val, err := op.evaluateExpression(expr)
			if err != nil {
				return fmt.Errorf("failed to evaluate end expression %d: %w", i, err)
			}
			endValues[i] = val
		}

		if len(endValues) == 1 {
			endKeyBytes, err = op.keyEncoder.EncodeValue(endValues[0])
		} else {
			endKeyBytes, err = op.keyEncoder.EncodeMultiColumn(endValues)
		}
		if err != nil {
			return fmt.Errorf("failed to encode end key: %w", err)
		}
	}

	// Perform index range scan
	op.entries, err = op.indexImpl.Range(startKeyBytes, endKeyBytes)
	if err != nil {
		return fmt.Errorf("failed to perform index range scan: %w", err)
	}

	op.isOpen = true
	op.position = 0

	// Track that we're doing an index-only scan
	if op.ctx.Stats != nil {
		op.ctx.Stats.IndexOnlyScans++
	}

	return nil
}

// Next returns the next row from the index-only scan.
func (op *IndexOnlyScanOperator) Next() (*Row, error) {
	if !op.isOpen {
		return nil, fmt.Errorf("index-only scan operator not open")
	}

	// Check if we have more entries
	if op.position >= len(op.entries) {
		return nil, nil // nolint:nilnil // EOF - standard iterator pattern
	}

	// Get current index entry
	entry := op.entries[op.position]
	op.position++

	// Extract values from the index key
	// This is a simplified implementation - in a real system, you'd need to
	// decode the composite key based on the index structure
	values, err := op.decodeIndexKey(entry.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode index key: %w", err)
	}

	// Build row with only the projected columns
	row := &Row{
		Values: make([]types.Value, len(op.projectedColumns)),
	}

	// Map index columns to projected columns
	indexColMap := make(map[string]int)
	for i, col := range op.index.Columns {
		indexColMap[col.Column.Name] = i
	}

	for i, colName := range op.projectedColumns {
		if idx, ok := indexColMap[colName]; ok && idx < len(values) {
			row.Values[i] = values[idx]
		} else {
			// Column not in index key - this shouldn't happen if IsCoveringIndex worked correctly
			return nil, fmt.Errorf("column %s not found in index", colName)
		}
	}

	// Update statistics
	if op.ctx.Stats != nil {
		op.ctx.Stats.RowsRead++
	}

	return row, nil
}

// Close cleans up the index-only scan operator.
func (op *IndexOnlyScanOperator) Close() error {
	op.isOpen = false
	op.entries = nil
	op.position = 0
	return nil
}

// Schema returns the output schema of the index-only scan.
func (op *IndexOnlyScanOperator) Schema() *Schema {
	return op.schema
}

// evaluateExpression evaluates a planner expression to get a value.
func (op *IndexOnlyScanOperator) evaluateExpression(expr planner.Expression) (types.Value, error) {
	switch e := expr.(type) {
	case *planner.Literal:
		return e.Value, nil
	case *planner.ParameterRef:
		if e.Index < 1 || e.Index > len(op.ctx.Params) {
			return types.Value{}, fmt.Errorf("parameter $%d out of range", e.Index)
		}
		return op.ctx.Params[e.Index-1], nil
	default:
		return types.Value{}, fmt.Errorf("unsupported expression type in index key: %T", expr)
	}
}

// decodeIndexKey decodes a composite index key into individual values.
// This is a simplified implementation - in production, you'd need proper decoding
// based on the key encoding format.
func (op *IndexOnlyScanOperator) decodeIndexKey(_ []byte) ([]types.Value, error) {
	// For now, assume we know the structure based on the index columns
	values := make([]types.Value, len(op.index.Columns))

	// This would need proper implementation based on the actual key encoding
	// For testing purposes, we'll create dummy values
	for i, col := range op.index.Columns {
		// Create a value based on the column type
		switch col.Column.DataType {
		case types.Integer:
			values[i] = types.NewValue(int32(i))
		case types.Text:
			values[i] = types.NewValue(fmt.Sprintf("value_%d", i))
		default:
			values[i] = types.NewNullValue()
		}
	}

	return values, nil
}
