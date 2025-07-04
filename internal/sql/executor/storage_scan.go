package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// StorageScanOperator reads rows from disk-based storage
type StorageScanOperator struct {
	baseOperator
	table      *catalog.Table
	tableAlias string
	storage    StorageBackend
	iterator   RowIterator
	rowCount   int64
}

// NewStorageScanOperator creates a new storage-based scan operator
func NewStorageScanOperator(table *catalog.Table, storage StorageBackend) *StorageScanOperator {
	return NewStorageScanOperatorWithAlias(table, "", storage)
}

// NewStorageScanOperatorWithAlias creates a new storage-based scan operator with table alias
func NewStorageScanOperatorWithAlias(table *catalog.Table, alias string, storage StorageBackend) *StorageScanOperator {
	// Build schema from table columns
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	// Use alias if provided, otherwise use table name
	tableAlias := alias
	if tableAlias == "" {
		tableAlias = table.TableName
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name:       col.Name,
			Type:       col.DataType,
			Nullable:   col.IsNullable,
			TableName:  table.TableName,
			TableAlias: tableAlias,
		}
	}

	return &StorageScanOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:      table,
		tableAlias: tableAlias,
		storage:    storage,
	}
}

// Open initializes the scan
func (s *StorageScanOperator) Open(ctx *ExecContext) error {
	s.ctx = ctx

	// Initialize statistics collection
	// TODO: Get estimated rows from table statistics
	s.initStats(1000) // Default estimate

	// Set transaction ID on storage backend if available
	if ctx.Txn != nil {
		s.storage.SetTransactionID(uint64(ctx.Txn.ID()))
	}

	// Create iterator for table scan
	// Pass the snapshot timestamp from the execution context
	var err error
	s.iterator, err = s.storage.ScanTable(s.table.ID, ctx.SnapshotTS)
	if err != nil {
		return fmt.Errorf("failed to create scan iterator: %w", err)
	}

	return nil
}

// Next returns the next row
func (s *StorageScanOperator) Next() (*Row, error) {
	if s.iterator == nil {
		return nil, fmt.Errorf("scan not opened")
	}

	// Get next row from storage
	row, _, err := s.iterator.Next()
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	if row == nil {
		// Finalize statistics on EOF
		s.finishStats()
		// Report stats to collector
		if s.ctx != nil && s.ctx.StatsCollector != nil && s.stats != nil {
			s.ctx.StatsCollector(s, s.stats)
		}
		return nil, nil // nolint:nilnil // EOF - standard iterator pattern
	}

	// Update statistics
	s.rowCount++
	if s.ctx.Stats != nil {
		s.ctx.Stats.RowsRead++
	}

	// Record row for performance stats
	s.recordRow()

	return row, nil
}

// Close cleans up the scan
func (s *StorageScanOperator) Close() error {
	// Ensure stats are finalized
	s.finishStats()
	// Report stats to collector if not already done
	if s.ctx != nil && s.ctx.StatsCollector != nil && s.stats != nil {
		s.ctx.StatsCollector(s, s.stats)
	}

	if s.iterator != nil {
		if err := s.iterator.Close(); err != nil {
			return fmt.Errorf("failed to close iterator: %w", err)
		}
		s.iterator = nil
	}
	return nil
}
