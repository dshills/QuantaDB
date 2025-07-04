package executor

import (
	"context"
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Operator is the base interface for all execution operators.
type Operator interface {
	// Open initializes the operator.
	Open(ctx *ExecContext) error
	// Next returns the next row or nil when done.
	Next() (*Row, error)
	// Close cleans up resources.
	Close() error
	// Schema returns the output schema.
	Schema() *Schema
}

// baseOperator provides common functionality for operators.
type baseOperator struct {
	schema *Schema
	ctx    *ExecContext
	stats  *OperatorStats
	timer  *StatsTimer
}

func (o *baseOperator) Schema() *Schema {
	return o.schema
}

// initStats initializes statistics collection for this operator
func (o *baseOperator) initStats(estimatedRows int64) {
	if o.ctx != nil && o.ctx.CollectStats {
		o.stats = &OperatorStats{
			EstimatedRows: estimatedRows,
			ExtraInfo:     make(map[string]string),
		}
		o.timer = NewStatsTimer(o.stats)
	}
}

// recordRow records that a row was produced
func (o *baseOperator) recordRow() {
	if o.timer != nil {
		o.timer.RecordRow()
	}
}

// finishStats finalizes statistics collection
func (o *baseOperator) finishStats() {
	if o.timer != nil {
		o.timer.Stop()

		// Record buffer statistics if available
		if o.ctx.BufferStats != nil && o.stats != nil {
			o.stats.PagesHit = o.ctx.BufferStats.GetHits()
			o.stats.PagesRead = o.ctx.BufferStats.GetMisses()
		}

		// Note: The actual operator implementation should call StatsCollector
		// when appropriate (usually in Close() or when returning EOF)
	}
}

// ScanOperator reads rows from a table.
// DEPRECATED: This operator uses the key-value engine directly and does not support MVCC.
// Use StorageScanOperator for MVCC-aware table scans.
type ScanOperator struct {
	baseOperator
	table     *catalog.Table
	iterator  engine.Iterator
	rowCount  int64
	rowFormat *RowFormat
	keyFormat *RowKeyFormat
}

// NewScanOperator creates a new scan operator.
// DEPRECATED: Use NewStorageScanOperator for MVCC support.
// This operator bypasses MVCC visibility checks and should not be used in production.
func NewScanOperator(table *catalog.Table, ctx *ExecContext) *ScanOperator {
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

	return &ScanOperator{
		baseOperator: baseOperator{
			schema: schema,
			ctx:    ctx,
		},
		table:     table,
		rowFormat: NewRowFormat(schema),
		keyFormat: &RowKeyFormat{
			TableName:  table.TableName,
			SchemaName: table.SchemaName,
		},
	}
}

// Open initializes the scan.
func (s *ScanOperator) Open(ctx *ExecContext) error {
	s.ctx = ctx

	// Create table key prefix
	tableKey := fmt.Sprintf("table:%s:%s:", s.table.SchemaName, s.table.TableName)

	// Create iterator for table scan
	var err error
	// Use engine scan directly
	// Note: For transactional scans, use StorageScanOperator with MVCC support instead
	// TODO: Pass proper context through - using TODO for now as ExecContext doesn't have request context
	s.iterator, err = ctx.Engine.Scan(context.TODO(), []byte(tableKey), nil)

	if err != nil {
		return fmt.Errorf("failed to create scan iterator: %w", err)
	}

	return nil
}

// Next returns the next row.
func (s *ScanOperator) Next() (*Row, error) {
	if s.iterator == nil {
		return nil, fmt.Errorf("scan not opened")
	}

	// Keep scanning until we find a valid row key
	for s.iterator.Next() {
		key := s.iterator.Key()

		if key == nil {
			continue
		}

		// Check if this is a row key for our table
		if !s.keyFormat.IsRowKey(key) {
			continue
		}

		var value []byte
		var err error

		// If we have an MVCC transaction, read through it to get proper versioning
		if s.ctx.Txn != nil {
			value, err = s.ctx.Txn.Get(key)
			if err != nil {
				// Skip keys that are not visible in this transaction
				if err == engine.ErrKeyNotFound {
					continue
				}
				return nil, fmt.Errorf("failed to read key %s through transaction: %w", string(key), err)
			}
		} else {
			// Direct engine read
			value = s.iterator.Value()
			if value == nil {
				continue
			}
		}

		// Update statistics
		s.rowCount++
		if s.ctx.Stats != nil {
			s.ctx.Stats.RowsRead++
			s.ctx.Stats.BytesRead += int64(len(key) + len(value))
		}

		// Deserialize the row
		row, err := s.rowFormat.Deserialize(value)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize row: %w", err)
		}

		return row, nil
	}

	return nil, nil // nolint:nilnil // EOF
}

// Close cleans up the scan.
func (s *ScanOperator) Close() error {
	if s.iterator != nil {
		if err := s.iterator.Close(); err != nil {
			return fmt.Errorf("failed to close iterator: %w", err)
		}
		s.iterator = nil
	}
	return nil
}

// FilterOperator filters rows based on a predicate.
type FilterOperator struct {
	baseOperator
	child     Operator
	predicate ExprEvaluator
}

// NewFilterOperator creates a new filter operator.
func NewFilterOperator(child Operator, predicate ExprEvaluator) *FilterOperator {
	return &FilterOperator{
		baseOperator: baseOperator{
			schema: child.Schema(),
		},
		child:     child,
		predicate: predicate,
	}
}

// Open initializes the filter.
func (f *FilterOperator) Open(ctx *ExecContext) error {
	f.ctx = ctx
	return f.child.Open(ctx)
}

// Next returns the next matching row.
func (f *FilterOperator) Next() (*Row, error) {
	for {
		// Get next row from child
		row, err := f.child.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil // nolint:nilnil // EOF
		}

		// Evaluate predicate
		result, err := f.predicate.Eval(row, f.ctx)
		if err != nil {
			return nil, fmt.Errorf("predicate evaluation failed: %w", err)
		}

		// Check if row matches
		if result.IsNull() {
			continue // NULL is treated as false
		}

		if b, ok := result.Data.(bool); ok && b {
			if f.ctx.Stats != nil {
				f.ctx.Stats.RowsReturned++
			}
			return row, nil
		}
	}
}

// Close cleans up the filter.
func (f *FilterOperator) Close() error {
	return f.child.Close()
}

// ProjectOperator projects columns and evaluates expressions.
type ProjectOperator struct {
	baseOperator
	child       Operator
	projections []ExprEvaluator
}

// NewProjectOperator creates a new projection operator.
func NewProjectOperator(child Operator, projections []ExprEvaluator, schema *Schema) *ProjectOperator {
	return &ProjectOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		child:       child,
		projections: projections,
	}
}

// Open initializes the projection.
func (p *ProjectOperator) Open(ctx *ExecContext) error {
	p.ctx = ctx
	return p.child.Open(ctx)
}

// Next returns the next projected row.
func (p *ProjectOperator) Next() (*Row, error) {
	// Get next row from child
	row, err := p.child.Next()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil // nolint:nilnil // EOF
	}

	// Create projected row
	projectedRow := &Row{
		Values: make([]types.Value, len(p.projections)),
	}

	// Evaluate each projection
	for i, proj := range p.projections {
		// For EXISTS expressions, set up correlation context
		// Other subqueries should not get correlation context unless they are marked as correlated
		var evalCtx *ExecContext
		if _, isExists := proj.(*existsEvaluator); isExists {
			// Create a new context with correlation schema for EXISTS
			ctxCopy := *p.ctx
			if ctxCopy.CorrelationSchema == nil {
				ctxCopy.CorrelationSchema = p.child.Schema()
			}
			evalCtx = &ctxCopy
		} else {
			evalCtx = p.ctx
		}

		value, err := proj.Eval(row, evalCtx)
		if err != nil {
			return nil, fmt.Errorf("projection %d failed: %w", i, err)
		}
		projectedRow.Values[i] = value
	}

	if p.ctx.Stats != nil {
		p.ctx.Stats.RowsReturned++
	}

	return projectedRow, nil
}

// Close cleans up the projection.
func (p *ProjectOperator) Close() error {
	return p.child.Close()
}

// LimitOperator implements LIMIT and OFFSET.
type LimitOperator struct {
	baseOperator
	child    Operator
	limit    int64
	offset   int64
	rowCount int64
}

// NewLimitOperator creates a new limit operator.
func NewLimitOperator(child Operator, limit, offset int64) *LimitOperator {
	return &LimitOperator{
		baseOperator: baseOperator{
			schema: child.Schema(),
		},
		child:  child,
		limit:  limit,
		offset: offset,
	}
}

// Open initializes the limit.
func (l *LimitOperator) Open(ctx *ExecContext) error {
	l.ctx = ctx
	l.rowCount = 0
	return l.child.Open(ctx)
}

// Next returns the next row within the limit.
func (l *LimitOperator) Next() (*Row, error) {
	// Skip offset rows
	for l.rowCount < l.offset {
		row, err := l.child.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil // nolint:nilnil // EOF before reaching offset
		}
		l.rowCount++
	}

	// Check if we've reached the limit
	if l.limit >= 0 && l.rowCount >= l.offset+l.limit {
		return nil, nil // nolint:nilnil // EOF due to limit
	}

	// Get next row
	row, err := l.child.Next()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil // nolint:nilnil // EOF
	}

	l.rowCount++

	if l.ctx.Stats != nil {
		l.ctx.Stats.RowsReturned++
	}

	return row, nil
}

// Close cleans up the limit.
func (l *LimitOperator) Close() error {
	return l.child.Close()
}
