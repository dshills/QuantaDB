package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// StorageScanOperator reads rows from disk-based storage
type StorageScanOperator struct {
	baseOperator
	table    *catalog.Table
	storage  StorageBackend
	iterator RowIterator
	rowCount int64
}

// NewStorageScanOperator creates a new storage-based scan operator
func NewStorageScanOperator(table *catalog.Table, storage StorageBackend) *StorageScanOperator {
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

	return &StorageScanOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:   table,
		storage: storage,
	}
}

// Open initializes the scan
func (s *StorageScanOperator) Open(ctx *ExecContext) error {
	s.ctx = ctx

	// Set transaction ID on storage backend if available
	if ctx.Txn != nil {
		s.storage.SetTransactionID(uint64(ctx.Txn.ID()))
	}

	// Create iterator for table scan
	var err error
	s.iterator, err = s.storage.ScanTable(s.table.ID)
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
		return nil, nil // nolint:nilnil // EOF - standard iterator pattern
	}

	// Update statistics
	s.rowCount++
	if s.ctx.Stats != nil {
		s.ctx.Stats.RowsRead++
	}

	return row, nil
}

// Close cleans up the scan
func (s *StorageScanOperator) Close() error {
	if s.iterator != nil {
		if err := s.iterator.Close(); err != nil {
			return fmt.Errorf("failed to close iterator: %w", err)
		}
		s.iterator = nil
	}
	return nil
}
