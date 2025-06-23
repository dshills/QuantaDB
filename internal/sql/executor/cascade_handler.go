package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CascadeDeleteHandler handles cascade operations for foreign key constraints
type CascadeDeleteHandler struct {
	catalog     catalog.Catalog
	storage     StorageBackend
	validator   *SimpleConstraintValidator
	ctx         *ExecContext
	deletedRows map[string]bool // Map of "tableID:rowID" to track processed rows
}

// NewCascadeDeleteHandler creates a new cascade delete handler
func NewCascadeDeleteHandler(cat catalog.Catalog, storage StorageBackend, validator *SimpleConstraintValidator, ctx *ExecContext) *CascadeDeleteHandler {
	return &CascadeDeleteHandler{
		catalog:     cat,
		storage:     storage,
		validator:   validator,
		ctx:         ctx,
		deletedRows: make(map[string]bool),
	}
}

// ProcessDelete handles cascade operations for a deleted row
func (h *CascadeDeleteHandler) ProcessDelete(table *catalog.Table, row *Row, rowID RowID) error {
	// Mark this row as deleted to prevent infinite loops
	key := fmt.Sprintf("%d:%d:%d", table.ID, rowID.PageID, rowID.SlotID)
	if h.deletedRows[key] {
		return nil // Already processed
	}
	h.deletedRows[key] = true

	// Find all tables that might reference this table
	tables, err := h.catalog.ListTables(table.SchemaName)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}

	// Process each potentially referencing table
	for _, refTable := range tables {
		if err := h.processReferencingTable(table, row, refTable); err != nil {
			return err
		}
	}

	return nil
}

// processReferencingTable handles cascade operations for a specific referencing table
func (h *CascadeDeleteHandler) processReferencingTable(deletedTable *catalog.Table, deletedRow *Row, refTable *catalog.Table) error {
	// Check each foreign key constraint in the referencing table
	for _, constraint := range refTable.Constraints {
		fk, ok := constraint.(*catalog.ForeignKeyConstraint)
		if !ok || fk.RefTableName != deletedTable.TableName {
			continue
		}

		// Find rows that reference the deleted row
		referencingRows, err := h.findReferencingRows(deletedTable, deletedRow, refTable, fk)
		if err != nil {
			return err
		}

		// Handle each referencing row based on the ON DELETE action
		for _, ref := range referencingRows {
			if err := h.handleReferentialAction(refTable, ref.row, ref.rowID, fk, fk.OnDelete); err != nil {
				return err
			}
		}
	}

	return nil
}

// referencingRow holds a row and its ID
type referencingRow struct {
	row   *Row
	rowID RowID
}

// findReferencingRows finds all rows in refTable that reference the deleted row
func (h *CascadeDeleteHandler) findReferencingRows(deletedTable *catalog.Table, deletedRow *Row, refTable *catalog.Table, fk *catalog.ForeignKeyConstraint) ([]referencingRow, error) {
	var referencingRows []referencingRow

	// Scan the referencing table
	iter, err := h.storage.ScanTable(refTable.ID, 0) // Use 0 for current snapshot
	if err != nil {
		return nil, fmt.Errorf("failed to scan table %s: %w", refTable.TableName, err)
	}
	defer iter.Close()

	for {
		row, rowID, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		if row == nil {
			break
		}

		// Check if this row references the deleted row
		if h.rowReferencesDeleted(deletedTable, deletedRow, refTable, row, fk) {
			referencingRows = append(referencingRows, referencingRow{row: row, rowID: rowID})
		}
	}

	return referencingRows, nil
}

// rowReferencesDeleted checks if a row references the deleted row through the given foreign key
func (h *CascadeDeleteHandler) rowReferencesDeleted(deletedTable *catalog.Table, deletedRow *Row, refTable *catalog.Table, refRow *Row, fk *catalog.ForeignKeyConstraint) bool {
	// Check all foreign key columns
	for i, refCol := range fk.RefColumns {
		// Get column indices
		deletedColIdx := getColumnIndex(deletedTable, refCol)
		if deletedColIdx < 0 {
			return false
		}

		fkCol := fk.Columns[i]
		fkColIdx := getColumnIndex(refTable, fkCol)
		if fkColIdx < 0 {
			return false
		}

		// Get values
		deletedVal := deletedRow.Values[deletedColIdx]
		refVal := refRow.Values[fkColIdx]

		// If the foreign key value is NULL, it doesn't reference anything
		if refVal.IsNull() {
			return false
		}

		// Compare values
		if !valuesEqual(deletedVal, refVal) {
			return false
		}
	}

	// All columns match
	return true
}

// handleReferentialAction handles a specific referential action for a row
func (h *CascadeDeleteHandler) handleReferentialAction(table *catalog.Table, row *Row, rowID RowID, fk *catalog.ForeignKeyConstraint, action catalog.ReferentialAction) error {
	switch action {
	case catalog.Cascade:
		// Delete the referencing row (recursively handle its cascades)
		if err := h.ProcessDelete(table, row, rowID); err != nil {
			return err
		}
		if err := h.storage.DeleteRow(table.ID, rowID); err != nil {
			return fmt.Errorf("failed to cascade delete row: %w", err)
		}

	case catalog.SetNull:
		// Set the foreign key columns to NULL
		updatedRow := h.setForeignKeyColumnsToNull(table, row, fk)
		if err := h.storage.UpdateRow(table.ID, rowID, updatedRow); err != nil {
			return fmt.Errorf("failed to set NULL: %w", err)
		}

	case catalog.SetDefault:
		// Set the foreign key columns to their default values
		updatedRow, err := h.setForeignKeyColumnsToDefault(table, row, fk)
		if err != nil {
			return err
		}
		if err := h.storage.UpdateRow(table.ID, rowID, updatedRow); err != nil {
			return fmt.Errorf("failed to set default: %w", err)
		}

	case catalog.Restrict, catalog.NoAction, "":
		// These should have been caught earlier, but just in case
		return fmt.Errorf("foreign key constraint %s violated: cannot delete row with existing references", fk.Name)

	default:
		return fmt.Errorf("unsupported referential action: %s", action)
	}

	return nil
}

// setForeignKeyColumnsToNull creates a new row with foreign key columns set to NULL
func (h *CascadeDeleteHandler) setForeignKeyColumnsToNull(table *catalog.Table, row *Row, fk *catalog.ForeignKeyConstraint) *Row {
	// Create a copy of the row
	newValues := make([]types.Value, len(row.Values))
	copy(newValues, row.Values)

	// Set foreign key columns to NULL
	for _, colName := range fk.Columns {
		colIdx := getColumnIndex(table, colName)
		if colIdx >= 0 {
			newValues[colIdx] = types.NewNullValue()
		}
	}

	return &Row{Values: newValues}
}

// setForeignKeyColumnsToDefault creates a new row with foreign key columns set to default values
func (h *CascadeDeleteHandler) setForeignKeyColumnsToDefault(table *catalog.Table, row *Row, fk *catalog.ForeignKeyConstraint) (*Row, error) {
	// Create a copy of the row
	newValues := make([]types.Value, len(row.Values))
	copy(newValues, row.Values)

	// Set foreign key columns to their default values
	for _, colName := range fk.Columns {
		colIdx := getColumnIndex(table, colName)
		if colIdx >= 0 && colIdx < len(table.Columns) {
			col := table.Columns[colIdx]
			if !col.DefaultValue.IsNull() {
				// Use the default value directly
				newValues[colIdx] = col.DefaultValue
			} else {
				// No default value, use NULL if allowed
				if col.IsNullable {
					newValues[colIdx] = types.NewNullValue()
				} else {
					return nil, fmt.Errorf("column %s has no default value and cannot be NULL", colName)
				}
			}
		}
	}

	return &Row{Values: newValues}, nil
}
