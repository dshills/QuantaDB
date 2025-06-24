package executor

import (
	"fmt"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// UpdateOperator executes UPDATE statements
type UpdateOperator struct {
	baseOperator
	table            *catalog.Table
	storage          StorageBackend
	indexMgr         *index.Manager // Index manager for updating indexes
	assignments      []parser.Assignment
	whereClause      parser.Expression
	rowsUpdated      int64
	statsMaintenance catalog.StatsMaintenance // Optional statistics maintenance
}

// NewUpdateOperator creates a new update operator
func NewUpdateOperator(table *catalog.Table, storage StorageBackend, assignments []parser.Assignment, whereClause parser.Expression) *UpdateOperator {
	// Schema for UPDATE result (affected rows)
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "rows_affected",
				Type:     types.Integer,
				Nullable: false,
			},
		},
	}

	return &UpdateOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:       table,
		storage:     storage,
		assignments: assignments,
		whereClause: whereClause,
	}
}

// SetIndexManager sets the index manager for the operator
func (u *UpdateOperator) SetIndexManager(indexMgr *index.Manager) {
	u.indexMgr = indexMgr
}

// Open initializes the update operation
func (u *UpdateOperator) Open(ctx *ExecContext) error {
	u.ctx = ctx
	u.rowsUpdated = 0

	// Set transaction ID on storage backend if available
	if ctx.Txn != nil {
		u.storage.SetTransactionID(uint64(ctx.Txn.ID()))
	}

	// Scan the table to find rows to update
	iterator, err := u.storage.ScanTable(u.table.ID, ctx.SnapshotTS)
	if err != nil {
		return fmt.Errorf("failed to scan table: %w", err)
	}
	defer iterator.Close()

	// Process each row
	// Collect all rows first to avoid issues with concurrent modification
	var rowsToUpdate []struct {
		row   *Row
		rowID RowID
	}

	for {
		row, rowID, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
		if row == nil {
			break // End of table
		}

		rowsToUpdate = append(rowsToUpdate, struct {
			row   *Row
			rowID RowID
		}{row: row, rowID: rowID})
	}

	// Prepare columns slice once if we have a WHERE clause
	var columns []catalog.Column
	if u.whereClause != nil {
		columns = make([]catalog.Column, len(u.table.Columns))
		for i, col := range u.table.Columns {
			columns[i] = *col
		}
	}

	// Now process the collected rows
	for _, entry := range rowsToUpdate {
		row := entry.row
		rowID := entry.rowID

		// Check WHERE clause if present
		if u.whereClause != nil {
			evalCtx := newEvalContext(row, columns, u.ctx.Params)

			match, err := evaluateExpression(u.whereClause, evalCtx)
			if err != nil {
				return fmt.Errorf("failed to evaluate WHERE clause: %w", err)
			}

			// Skip rows that don't match
			// Check if the value is true (treat NULL and false as non-match)
			if match.IsNull() || match.Data == false {
				continue
			}
		}

		// Apply assignments to create updated row
		updatedRow := &Row{
			Values: make([]types.Value, len(row.Values)),
		}
		copy(updatedRow.Values, row.Values)

		for _, assignment := range u.assignments {
			// Find column index
			columnIndex := -1
			for i, col := range u.table.Columns {
				if col.Name == assignment.Column {
					columnIndex = i
					break
				}
			}

			if columnIndex == -1 {
				return fmt.Errorf("column '%s' not found", assignment.Column)
			}

			// Evaluate assignment expression
			evalCtx := newEvalContext(row, columns, u.ctx.Params)
			value, err := evaluateExpression(assignment.Value, evalCtx)
			if err != nil {
				return fmt.Errorf("failed to evaluate assignment for column '%s': %w", assignment.Column, err)
			}

			// Type check against column
			col := u.table.Columns[columnIndex]
			if !col.IsNullable && value.IsNull() {
				return fmt.Errorf("null value for non-nullable column '%s'", col.Name)
			}

			// Check type compatibility
			if !isTypeCompatible(col.DataType, value) {
				return fmt.Errorf("type mismatch: cannot assign %T to column '%s' of type %s",
					value.Data, col.Name, col.DataType.Name())
			}

			updatedRow.Values[columnIndex] = value
		}

		// Validate constraints if validator is available
		if u.ctx.ConstraintValidator != nil {
			if err := u.ctx.ConstraintValidator.ValidateUpdate(u.table, row, updatedRow); err != nil {
				return fmt.Errorf("constraint violation: %w", err)
			}
		}

		// Update indexes if index manager is available
		if u.indexMgr != nil {
			// First, delete old index entries
			oldRowMap := make(map[string]types.Value, len(u.table.Columns))
			for colIdx, col := range u.table.Columns {
				oldRowMap[col.Name] = row.Values[colIdx]
			}

			if err := u.indexMgr.DeleteFromIndexes(u.table.SchemaName, u.table.TableName, oldRowMap); err != nil {
				return fmt.Errorf("failed to delete old index entries: %w", err)
			}
		}

		// Update the row in storage
		err = u.storage.UpdateRow(u.table.ID, rowID, updatedRow)
		if err != nil {
			return fmt.Errorf("failed to update row: %w", err)
		}

		// Insert new index entries if index manager is available
		if u.indexMgr != nil {
			// Convert updated row values to map format
			newRowMap := make(map[string]types.Value, len(u.table.Columns))
			for colIdx, col := range u.table.Columns {
				newRowMap[col.Name] = updatedRow.Values[colIdx]
			}

			if err := u.indexMgr.InsertIntoIndexes(u.table.SchemaName, u.table.TableName, newRowMap, rowID.Bytes()); err != nil {
				// TODO: Implement proper rollback of the update operation
				// Current state: old index entries deleted, row updated, new index insert failed
				// Need to either:
				// 1. Restore old index entries and revert row update
				// 2. Use WAL to make index+storage atomic
				// 3. Implement saga pattern with compensation
				return fmt.Errorf("failed to insert new index entries: %w", err)
			}
		}

		u.rowsUpdated++
	}

	// Update statistics
	if u.ctx.Stats != nil {
		u.ctx.Stats.RowsReturned = 1 // One result row with count
	}

	return nil
}

// Next returns the result (number of rows updated)
func (u *UpdateOperator) Next() (*Row, error) {
	// UPDATE returns a single row with the count of affected rows
	if u.rowsUpdated >= 0 {
		result := &Row{
			Values: []types.Value{
				types.NewValue(u.rowsUpdated),
			},
		}
		u.rowsUpdated = -1 // Ensure we only return once
		return result, nil
	}

	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
}

// Close cleans up resources
func (u *UpdateOperator) Close() error {
	// Trigger statistics maintenance if rows were updated
	if u.rowsUpdated > 0 && u.statsMaintenance != nil {
		catalog.StatsMaintenanceHook(u.statsMaintenance, u.table.ID, catalog.ChangeUpdate, u.rowsUpdated)
	}
	return nil
}

// SetStatsMaintenance sets the statistics maintenance handler
func (u *UpdateOperator) SetStatsMaintenance(maintenance catalog.StatsMaintenance) {
	u.statsMaintenance = maintenance
}

// isTypeCompatible checks if a value is compatible with a column type
func isTypeCompatible(dataType types.DataType, value types.Value) bool {
	if value.IsNull() {
		return true // NULL is compatible with any nullable column
	}

	typeName := dataType.Name()

	switch typeName {
	case "INTEGER":
		switch value.Data.(type) {
		case int32, int, int64:
			return true
		}
	case "BIGINT":
		switch value.Data.(type) {
		case int64, int, int32:
			return true
		}
	case "SMALLINT":
		switch value.Data.(type) {
		case int16, int, int32:
			return true
		}
	case typeTEXT, typeVARCHAR, typeCHAR:
		_, ok := value.Data.(string)
		return ok
	case "BOOLEAN":
		_, ok := value.Data.(bool)
		return ok
	case "DECIMAL":
		switch value.Data.(type) {
		case float64, float32:
			return true
		}
	case "TIMESTAMP", "DATE":
		switch value.Data.(type) {
		case int64, time.Time:
			return true
		}
	}

	return false
}
