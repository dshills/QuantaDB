package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CompositeIndexScanOperator executes composite (multi-column) index scans.
type CompositeIndexScanOperator struct {
	baseOperator
	table            *catalog.Table
	index            *catalog.Index
	indexImpl        index.Index
	indexMgr         *index.Manager
	storage          StorageBackend
	startValues      []types.Value
	endValues        []types.Value
	pushedPredicates planner.Expression // Additional predicates to evaluate
	predicateEval    ExprEvaluator      // Pre-built evaluator for pushed predicates
	entries          []index.IndexEntry
	position         int
	keyEncoder       *index.KeyEncoder
	isOpen           bool
	scanHelper       *indexScanHelper
}

// NewCompositeIndexScanOperator creates a new composite index scan operator.
func NewCompositeIndexScanOperator(
	table *catalog.Table,
	indexMeta *catalog.Index,
	indexMgr *index.Manager,
	storage StorageBackend,
	startValues, endValues []types.Value,
) *CompositeIndexScanOperator {
	return NewCompositeIndexScanOperatorWithPredicates(table, indexMeta, indexMgr, storage, startValues, endValues, nil)
}

// NewCompositeIndexScanOperatorWithPredicates creates a new composite index scan operator with pushed predicates.
func NewCompositeIndexScanOperatorWithPredicates(
	table *catalog.Table,
	indexMeta *catalog.Index,
	indexMgr *index.Manager,
	storage StorageBackend,
	startValues, endValues []types.Value,
	pushedPredicates planner.Expression,
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
		baseOperator:     baseOperator{schema: schema},
		table:            table,
		index:            indexMeta,
		indexMgr:         indexMgr,
		storage:          storage,
		startValues:      startValues,
		endValues:        endValues,
		pushedPredicates: pushedPredicates,
		entries:          nil,
		position:         0,
		keyEncoder:       &index.KeyEncoder{},
		isOpen:           false,
	}
}

// Open initializes the composite index scan operator.
func (op *CompositeIndexScanOperator) Open(ctx *ExecContext) error {
	if op.isOpen {
		return fmt.Errorf("composite index scan operator already open")
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
