package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// IndexScanOperator executes index scans to retrieve rows.
type IndexScanOperator struct {
	baseOperator
	table            *catalog.Table
	index            *catalog.Index
	indexImpl        index.Index
	indexMgr         *index.Manager
	storage          StorageBackend
	startKey         planner.Expression
	endKey           planner.Expression
	pushedPredicates planner.Expression // Additional predicates to evaluate
	predicateEval    ExprEvaluator      // Pre-built evaluator for pushed predicates
	entries          []index.IndexEntry
	position         int
	keyEncoder       *index.KeyEncoder
	isOpen           bool
	scanHelper       *indexScanHelper
}

// NewIndexScanOperator creates a new index scan operator.
func NewIndexScanOperator(
	table *catalog.Table,
	indexMeta *catalog.Index,
	indexMgr *index.Manager,
	storage StorageBackend,
	startKey, endKey planner.Expression,
) *IndexScanOperator {
	return NewIndexScanOperatorWithPredicates(table, indexMeta, indexMgr, storage, startKey, endKey, nil)
}

// NewIndexScanOperatorWithPredicates creates a new index scan operator with pushed predicates.
func NewIndexScanOperatorWithPredicates(
	table *catalog.Table,
	indexMeta *catalog.Index,
	indexMgr *index.Manager,
	storage StorageBackend,
	startKey, endKey planner.Expression,
	pushedPredicates planner.Expression,
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
		baseOperator:     baseOperator{schema: schema},
		table:            table,
		index:            indexMeta,
		indexMgr:         indexMgr,
		storage:          storage,
		startKey:         startKey,
		endKey:           endKey,
		pushedPredicates: pushedPredicates,
		entries:          nil,
		position:         0,
		keyEncoder:       &index.KeyEncoder{},
		isOpen:           false,
	}
}

// Open initializes the index scan operator.
func (op *IndexScanOperator) Open(ctx *ExecContext) error {
	if op.isOpen {
		return fmt.Errorf("index scan operator already open")
	}

	op.ctx = ctx

	// Build predicate evaluator once
	if op.pushedPredicates != nil {
		var err error
		op.predicateEval, err = buildExprEvaluator(op.pushedPredicates)
		if err != nil {
			return fmt.Errorf("failed to build predicate evaluator: %w", err)
		}
	}

	// Create scan helper
	op.scanHelper = newIndexScanHelper(op.storage, op.predicateEval, ctx, op.table.ID)

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

	// Loop until we find a matching row or reach EOF
	for op.position < len(op.entries) {
		// Get current index entry
		entry := op.entries[op.position]
		op.position++

		// Process the index entry using the helper
		row, skip, err := op.scanHelper.processIndexEntry(entry)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}

		return row, nil
	}

	// EOF - no more entries
	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
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
	case *planner.ParameterRef:
		// Handle parameter references
		if e.Index < 1 || e.Index > len(op.ctx.Params) {
			return types.Value{}, fmt.Errorf("parameter $%d out of range (have %d parameters)", e.Index, len(op.ctx.Params))
		}
		return op.ctx.Params[e.Index-1], nil
	default:
		return types.Value{}, fmt.Errorf("unsupported expression type in index key: %T", expr)
	}
}
