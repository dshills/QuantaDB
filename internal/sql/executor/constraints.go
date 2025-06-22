package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// SimpleConstraintValidator validates table constraints during DML operations.
// This is a simplified version that doesn't support cascading actions yet.
type SimpleConstraintValidator struct {
	catalog catalog.Catalog
	storage StorageBackend
	planner planner.Planner
}

// NewSimpleConstraintValidator creates a new constraint validator.
func NewSimpleConstraintValidator(cat catalog.Catalog, store StorageBackend, plan planner.Planner) *SimpleConstraintValidator {
	return &SimpleConstraintValidator{
		catalog: cat,
		storage: store,
		planner: plan,
	}
}

// ValidateInsert validates constraints for an INSERT operation.
func (cv *SimpleConstraintValidator) ValidateInsert(table *catalog.Table, row *Row) error {
	// Check foreign key constraints
	for _, constraint := range table.Constraints {
		switch c := constraint.(type) {
		case *catalog.ForeignKeyConstraint:
			if err := cv.validateForeignKey(table, row, c); err != nil {
				return err
			}
		case *catalog.CheckConstraint:
			if err := cv.validateCheck(table, row, c); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateUpdate validates constraints for an UPDATE operation.
func (cv *SimpleConstraintValidator) ValidateUpdate(table *catalog.Table, oldRow, newRow *Row) error {
	// Check foreign key constraints on the new values
	for _, constraint := range table.Constraints {
		switch c := constraint.(type) {
		case *catalog.ForeignKeyConstraint:
			if err := cv.validateForeignKey(table, newRow, c); err != nil {
				return err
			}
		case *catalog.CheckConstraint:
			if err := cv.validateCheck(table, newRow, c); err != nil {
				return err
			}
		}
	}
	
	// For now, we don't check referential integrity for tables that reference this one
	// This would require implementing CASCADE, SET NULL, etc. actions
	return nil
}

// ValidateDelete validates constraints for a DELETE operation.
func (cv *SimpleConstraintValidator) ValidateDelete(table *catalog.Table, row *Row) error {
	// For now, we just check if any rows reference this one and block the delete
	// In a full implementation, we would handle CASCADE, SET NULL, etc.
	return cv.checkReferencingTables(table, row)
}

// validateForeignKey validates a foreign key constraint.
func (cv *SimpleConstraintValidator) validateForeignKey(table *catalog.Table, row *Row, fk *catalog.ForeignKeyConstraint) error {
	// Get referenced table
	refTable, err := cv.catalog.GetTable(fk.RefTableSchema, fk.RefTableName)
	if err != nil {
		return fmt.Errorf("referenced table %s.%s not found", fk.RefTableSchema, fk.RefTableName)
	}

	// Check if all foreign key columns are NULL (which is allowed)
	allNull := true
	for _, col := range fk.Columns {
		colIdx := getColumnIndex(table, col)
		if colIdx >= 0 && !row.Values[colIdx].IsNull() {
			allNull = false
			break
		}
	}
	
	if allNull {
		return nil // NULL foreign keys are allowed
	}

	// Check if referenced row exists
	found := false
	iter, err := cv.storage.ScanTable(refTable.ID, 0) // Use 0 for current snapshot
	if err != nil {
		return fmt.Errorf("failed to scan referenced table: %w", err)
	}
	defer iter.Close()

	for {
		refRow, _, err := iter.Next()
		if err != nil {
			return fmt.Errorf("failed to scan referenced table: %w", err)
		}
		if refRow == nil {
			break
		}

		// Check if this row matches the foreign key
		matches := true
		for i, col := range fk.Columns {
			colIdx := getColumnIndex(table, col)
			if colIdx < 0 {
				return fmt.Errorf("foreign key column %s not found", col)
			}
			
			refCol := fk.RefColumns[i]
			refColIdx := getColumnIndex(refTable, refCol)
			if refColIdx < 0 {
				return fmt.Errorf("referenced column %s not found", refCol)
			}
			
			// Compare values
			val := row.Values[colIdx]
			refVal := refRow.Values[refColIdx]
			
			// Use type-specific comparison
			if !valuesEqual(val, refVal) {
				matches = false
				break
			}
		}
		
		if matches {
			found = true
			break
		}
		
	}

	if !found {
		return fmt.Errorf("foreign key constraint %s violated: referenced row not found in %s", 
			fk.Name, fk.RefTableName)
	}

	return nil
}

// validateCheck validates a CHECK constraint.
func (cv *SimpleConstraintValidator) validateCheck(table *catalog.Table, row *Row, check *catalog.CheckConstraint) error {
	// Parse the check expression
	p := parser.NewParser(check.Expression)
	expr, err := p.ParseExpression()
	if err != nil {
		return fmt.Errorf("invalid check constraint expression: %v", err)
	}

	// Create evaluation context
	columns := make([]catalog.Column, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = catalog.Column{
			Name:       col.Name,
			DataType:   col.DataType,
			IsNullable: col.IsNullable,
		}
	}
	
	evalCtx := newEvalContext(row, columns, nil)
	
	// Evaluate the expression
	result, err := evaluateExpression(expr, evalCtx)
	if err != nil {
		return fmt.Errorf("error evaluating check constraint: %v", err)
	}

	// Check if the result is true
	if !result.IsNull() {
		if b, ok := result.Data.(bool); ok && !b {
			name := check.Name
			if name == "" {
				name = "unnamed"
			}
			return fmt.Errorf("check constraint %s violated", name)
		}
	}

	return nil
}

// checkReferencingTables checks if any tables reference this row.
func (cv *SimpleConstraintValidator) checkReferencingTables(table *catalog.Table, row *Row) error {
	// Get all tables in the same schema
	tables, err := cv.catalog.ListTables(table.SchemaName)
	if err != nil {
		return err
	}

	for _, refTable := range tables {
		
		// Check each constraint in the referencing table
		for _, constraint := range refTable.Constraints {
			fk, ok := constraint.(*catalog.ForeignKeyConstraint)
			if !ok || fk.RefTableName != table.TableName {
				continue
			}
			
			// Check if any rows reference the row being deleted
			iter, err := cv.storage.ScanTable(refTable.ID, 0) // Use 0 for current snapshot
			if err != nil {
				continue
			}
			defer iter.Close()
			
			for {
				refRow, _, err := iter.Next()
				if err != nil || refRow == nil {
					break
				}
				
				// Check if this row references the deleted row
				matches := true
				for i, refCol := range fk.RefColumns {
					refColIdx := getColumnIndex(table, refCol)
					if refColIdx < 0 {
						matches = false
						break
					}
					
					col := fk.Columns[i]
					colIdx := getColumnIndex(refTable, col)
					if colIdx < 0 {
						matches = false
						break
					}
					
					// Compare values
					val := row.Values[refColIdx]
					refVal := refRow.Values[colIdx]
					
					if refVal.IsNull() {
						matches = false
						break
					}
					
					// Use type-specific comparison
					if !valuesEqual(val, refVal) {
						matches = false
						break
					}
				}
				
				if matches {
					return fmt.Errorf("foreign key constraint %s violated: rows still reference this record", fk.Name)
				}
			}
		}
	}
	
	return nil
}

func getColumnIndex(table *catalog.Table, colName string) int {
	for i, col := range table.Columns {
		if col.Name == colName {
			return i
		}
	}
	return -1
}

// valuesEqual compares two values for equality
func valuesEqual(v1, v2 types.Value) bool {
	// Handle NULL values
	if v1.IsNull() || v2.IsNull() {
		return v1.IsNull() && v2.IsNull()
	}
	
	// Type check
	if v1.Type() != v2.Type() {
		return false
	}
	
	// Compare based on type
	switch val1 := v1.Data.(type) {
	case int64:
		val2, ok := v2.Data.(int64)
		return ok && val1 == val2
	case float64:
		val2, ok := v2.Data.(float64)
		return ok && val1 == val2
	case string:
		val2, ok := v2.Data.(string)
		return ok && val1 == val2
	case bool:
		val2, ok := v2.Data.(bool)
		return ok && val1 == val2
	default:
		// For other types, use string representation
		return fmt.Sprintf("%v", v1.Data) == fmt.Sprintf("%v", v2.Data)
	}
}