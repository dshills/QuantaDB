package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// DeleteOperator executes DELETE statements
type DeleteOperator struct {
	baseOperator
	table            *catalog.Table
	storage          StorageBackend
	indexMgr         *index.Manager          // Index manager for updating indexes
	whereClause      parser.Expression
	rowsDeleted      int64
	statsMaintenance catalog.StatsMaintenance // Optional statistics maintenance
	cascadeHandler   *CascadeDeleteHandler    // Handles cascade deletes
}

// NewDeleteOperator creates a new delete operator
func NewDeleteOperator(table *catalog.Table, storage StorageBackend, whereClause parser.Expression) *DeleteOperator {
	// Schema for DELETE result (affected rows)
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "rows_affected",
				Type:     types.Integer,
				Nullable: false,
			},
		},
	}

	return &DeleteOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:       table,
		storage:     storage,
		whereClause: whereClause,
	}
}

// SetIndexManager sets the index manager for the operator
func (d *DeleteOperator) SetIndexManager(indexMgr *index.Manager) {
	d.indexMgr = indexMgr
}

// Open initializes the delete operation
func (d *DeleteOperator) Open(ctx *ExecContext) error {
	d.ctx = ctx
	d.rowsDeleted = 0

	// Set transaction ID on storage backend if available
	if ctx.Txn != nil {
		d.storage.SetTransactionID(uint64(ctx.Txn.ID()))
	}

	// Scan the table to find rows to delete
	iterator, err := d.storage.ScanTable(d.table.ID, ctx.SnapshotTS)
	if err != nil {
		return fmt.Errorf("failed to scan table: %w", err)
	}
	defer iterator.Close()

	// Prepare columns slice once if we have a WHERE clause
	var columns []catalog.Column
	if d.whereClause != nil {
		columns = make([]catalog.Column, len(d.table.Columns))
		for i, col := range d.table.Columns {
			columns[i] = *col
		}
	}

	// Process each row
	for {
		row, rowID, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
		if row == nil {
			break // End of table
		}

		// Check WHERE clause if present
		if d.whereClause != nil {
			evalCtx := newEvalContext(row, columns, d.ctx.Params)

			match, err := evaluateExpression(d.whereClause, evalCtx)
			if err != nil {
				return fmt.Errorf("failed to evaluate WHERE clause: %w", err)
			}

			// Skip rows that don't match
			// Check if the value is true (treat NULL and false as non-match)
			if match.IsNull() || match.Data == false {
				continue
			}
		}

		// Validate constraints if validator is available
		if d.ctx.ConstraintValidator != nil {
			if err := d.ctx.ConstraintValidator.ValidateDelete(d.table, row); err != nil {
				return fmt.Errorf("constraint violation: %w", err)
			}
		}

		// Handle cascade deletes if cascade handler is available
		if d.cascadeHandler == nil && d.ctx.ConstraintValidator != nil {
			// Create cascade handler on first use
			if validator, ok := d.ctx.ConstraintValidator.(*SimpleConstraintValidator); ok {
				d.cascadeHandler = NewCascadeDeleteHandler(d.ctx.Catalog, d.storage, validator, d.ctx)
			}
		}

		if d.cascadeHandler != nil {
			// Process cascade deletes (this will handle CASCADE, SET NULL, SET DEFAULT)
			if err := d.cascadeHandler.ProcessDelete(d.table, row, rowID); err != nil {
				return fmt.Errorf("cascade delete failed: %w", err)
			}
		}

		// Delete from indexes if index manager is available
		if d.indexMgr != nil {
			// Convert row values to map format expected by index manager
			rowMap := make(map[string]types.Value, len(d.table.Columns))
			for colIdx, col := range d.table.Columns {
				rowMap[col.Name] = row.Values[colIdx]
			}

			// Delete from all indexes for this table
			if err := d.indexMgr.DeleteFromIndexes(d.table.SchemaName, d.table.TableName, rowMap); err != nil {
				return fmt.Errorf("failed to delete from indexes: %w", err)
			}
		}

		// Delete the row from storage (mark as tombstone)
		err = d.storage.DeleteRow(d.table.ID, rowID)
		if err != nil {
			return fmt.Errorf("failed to delete row: %w", err)
		}

		d.rowsDeleted++
	}

	// Update statistics
	if d.ctx.Stats != nil {
		d.ctx.Stats.RowsReturned = 1 // One result row with count
	}

	return nil
}

// Next returns the result (number of rows deleted)
func (d *DeleteOperator) Next() (*Row, error) {
	// DELETE returns a single row with the count of affected rows
	if d.rowsDeleted >= 0 {
		result := &Row{
			Values: []types.Value{
				types.NewValue(d.rowsDeleted),
			},
		}
		d.rowsDeleted = -1 // Ensure we only return once
		return result, nil
	}

	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
}

// Close cleans up resources
func (d *DeleteOperator) Close() error {
	// Trigger statistics maintenance if rows were deleted
	if d.rowsDeleted > 0 && d.statsMaintenance != nil {
		catalog.StatsMaintenanceHook(d.statsMaintenance, d.table.ID, catalog.ChangeDelete, d.rowsDeleted)
	}
	return nil
}

// SetStatsMaintenance sets the statistics maintenance handler
func (d *DeleteOperator) SetStatsMaintenance(maintenance catalog.StatsMaintenance) {
	d.statsMaintenance = maintenance
}
