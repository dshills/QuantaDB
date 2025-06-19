package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// InsertOperator executes INSERT statements
type InsertOperator struct {
	baseOperator
	table        *catalog.Table
	storage      StorageBackend
	values       [][]parser.Expression // List of value tuples to insert
	rowsInserted int64
}

// NewInsertOperator creates a new insert operator
func NewInsertOperator(table *catalog.Table, storage StorageBackend, values [][]parser.Expression) *InsertOperator {
	// Schema for INSERT result (affected rows)
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "rows_affected",
				Type:     types.Integer,
				Nullable: false,
			},
		},
	}

	return &InsertOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:   table,
		storage: storage,
		values:  values,
	}
}

// Open initializes the insert operation
func (i *InsertOperator) Open(ctx *ExecContext) error {
	i.ctx = ctx
	i.rowsInserted = 0

	// Validate that we have values to insert
	if len(i.values) == 0 {
		return fmt.Errorf("no values to insert")
	}

	// Insert each row
	for _, valueList := range i.values {
		// Validate column count
		if len(valueList) != len(i.table.Columns) {
			return fmt.Errorf("column count mismatch: expected %d, got %d",
				len(i.table.Columns), len(valueList))
		}

		// Evaluate expressions and build row
		row := &Row{
			Values: make([]types.Value, len(valueList)),
		}

		for idx, expr := range valueList {
			// For INSERT, we only support literal values for now
			literal, ok := expr.(*parser.Literal)
			if !ok {
				return fmt.Errorf("INSERT only supports literal values, got %T for column %d", expr, idx)
			}

			// Use the literal value directly
			value := literal.Value

			// Type check against column
			col := i.table.Columns[idx]
			if !col.IsNullable && value.IsNull() {
				return fmt.Errorf("null value for non-nullable column '%s'", col.Name)
			}

			// TODO: Add type compatibility checking
			row.Values[idx] = value
		}

		// Insert the row into storage
		_, err := i.storage.InsertRow(i.table.ID, row)
		if err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		i.rowsInserted++
	}

	// Update statistics
	if i.ctx.Stats != nil {
		i.ctx.Stats.RowsReturned = 1 // One result row with count
	}

	return nil
}

// Next returns the result (number of rows inserted)
func (i *InsertOperator) Next() (*Row, error) {
	// INSERT returns a single row with the count of affected rows
	if i.rowsInserted > 0 {
		result := &Row{
			Values: []types.Value{
				types.NewValue(i.rowsInserted),
			},
		}
		i.rowsInserted = 0 // Ensure we only return once
		return result, nil
	}

	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
}

// Close cleans up resources
func (i *InsertOperator) Close() error {
	// Nothing to clean up for INSERT
	return nil
}
