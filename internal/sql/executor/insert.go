package executor

import (
	"fmt"
	"math/big"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// InsertOperator executes INSERT statements
type InsertOperator struct {
	baseOperator
	table            *catalog.Table
	storage          StorageBackend
	values           [][]parser.Expression // List of value tuples to insert
	rowsInserted     int64
	statsMaintenance catalog.StatsMaintenance // Optional statistics maintenance
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

	// Set transaction ID on storage backend if available
	if ctx.Txn != nil {
		i.storage.SetTransactionID(uint64(ctx.Txn.ID()))
	}

	// Validate that we have values to insert
	if len(i.values) == 0 {
		return fmt.Errorf("no values to insert")
	}

	// Create evaluation context once for parameter evaluation
	var evalCtx *evalContext
	if ctx.Params != nil && len(ctx.Params) > 0 {
		// Convert []*catalog.Column to []catalog.Column
		columns := make([]catalog.Column, len(i.table.Columns))
		for colIdx, col := range i.table.Columns {
			columns[colIdx] = *col
		}
		evalCtx = newEvalContext(nil, columns, ctx.Params)
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
			var value types.Value

			// Handle different expression types
			switch e := expr.(type) {
			case *parser.Literal:
				// Use the literal value directly
				value = e.Value
			case *parser.ParameterRef:
				// Handle parameter references ($1, $2, etc.)
				if evalCtx == nil {
					return fmt.Errorf("no parameters available for parameter $%d", e.Index)
				}
				evalResult, err := evaluateExpression(e, evalCtx)
				if err != nil {
					return fmt.Errorf("failed to evaluate parameter $%d for column %d: %w", e.Index, idx, err)
				}
				value = evalResult
			case *parser.UnaryExpr:
				// Handle unary expressions (e.g., -123, +456)
				if e.Operator == parser.TokenMinus {
					// Only handle negative literals for now
					if literal, ok := e.Expr.(*parser.Literal); ok {
						switch v := literal.Value.Data.(type) {
						case int32:
							value = types.NewValue(-v)
						case int64:
							value = types.NewValue(-v)
						case float32:
							value = types.NewValue(-v)
						case float64:
							value = types.NewValue(-v)
						case *big.Rat:
							// Negate the rational number
							negated := new(big.Rat).Neg(v)
							value = types.NewValue(negated)
						default:
							return fmt.Errorf("cannot apply unary minus to type %T", v)
						}
					} else {
						return fmt.Errorf("unary expressions only supported for literal values, got %T", e.Expr)
					}
				} else if e.Operator == parser.TokenPlus {
					// Unary plus is a no-op
					if literal, ok := e.Expr.(*parser.Literal); ok {
						value = literal.Value
					} else {
						return fmt.Errorf("unary expressions only supported for literal values, got %T", e.Expr)
					}
				} else {
					return fmt.Errorf("unsupported unary operator: %v", e.Operator)
				}
			default:
				// For other expression types, we need to evaluate them
				// This could include function calls, operators, etc.
				// For now, we'll return an error for unsupported types
				return fmt.Errorf("INSERT only supports literal values and parameters, got %T for column %d", expr, idx)
			}

			// Type check against column
			col := i.table.Columns[idx]
			if !col.IsNullable && value.IsNull() {
				return fmt.Errorf("null value for non-nullable column '%s'", col.Name)
			}

			// Type conversion based on column type
			convertedValue, err := i.convertValueToColumnType(value, col)
			if err != nil {
				return fmt.Errorf("type conversion error for column '%s': %w", col.Name, err)
			}
			row.Values[idx] = convertedValue
		}

		// Validate constraints if validator is available
		if ctx.ConstraintValidator != nil {
			if err := ctx.ConstraintValidator.ValidateInsert(i.table, row); err != nil {
				return fmt.Errorf("constraint violation: %w", err)
			}
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

// Close cleans up resources and triggers statistics maintenance
func (i *InsertOperator) Close() error {
	// Trigger statistics maintenance if rows were inserted
	if i.rowsInserted > 0 && i.statsMaintenance != nil {
		catalog.StatsMaintenanceHook(i.statsMaintenance, i.table.ID, catalog.ChangeInsert, i.rowsInserted)
	}
	return nil
}

// SetStatsMaintenance sets the statistics maintenance handler
func (i *InsertOperator) SetStatsMaintenance(maintenance catalog.StatsMaintenance) {
	i.statsMaintenance = maintenance
}

// convertValueToColumnType converts a value to match the column's expected type
func (i *InsertOperator) convertValueToColumnType(value types.Value, col *catalog.Column) (types.Value, error) {
	if value.IsNull() {
		return value, nil
	}

	// Get the column type name
	typeName := col.DataType.Name()

	// Handle different conversions based on column type
	switch typeName {
	case "FLOAT", "REAL":
		// Convert to float32
		switch v := value.Data.(type) {
		case float32:
			return value, nil // Already correct type
		case float64:
			return types.NewValue(float32(v)), nil
		case int32:
			return types.NewValue(float32(v)), nil
		case int64:
			return types.NewValue(float32(v)), nil
		case *big.Rat:
			// Convert big.Rat to float32
			f, _ := v.Float32()
			return types.NewValue(f), nil
		default:
			return types.Value{}, fmt.Errorf("cannot convert %T to FLOAT", v)
		}

	case "DOUBLE":
		// Convert to float64
		switch v := value.Data.(type) {
		case float64:
			return value, nil // Already correct type
		case float32:
			return types.NewValue(float64(v)), nil
		case int32:
			return types.NewValue(float64(v)), nil
		case int64:
			return types.NewValue(float64(v)), nil
		case *big.Rat:
			// Convert big.Rat to float64
			f, _ := v.Float64()
			return types.NewValue(f), nil
		default:
			return types.Value{}, fmt.Errorf("cannot convert %T to DOUBLE", v)
		}

	case "INTEGER":
		// Convert to int32
		switch v := value.Data.(type) {
		case int32:
			return value, nil // Already correct type
		case int64:
			if v > int64(^uint32(0)>>1) || v < -int64(^uint32(0)>>1)-1 {
				return types.Value{}, fmt.Errorf("value %d out of range for INTEGER", v)
			}
			return types.NewValue(int32(v)), nil
		case float32:
			return types.NewValue(int32(v)), nil
		case float64:
			return types.NewValue(int32(v)), nil
		case *big.Rat:
			// Convert big.Rat to int32
			i := v.Num()
			if v.Denom().Cmp(big.NewInt(1)) != 0 {
				return types.Value{}, fmt.Errorf("cannot convert decimal %v to INTEGER", v)
			}
			if !i.IsInt64() {
				return types.Value{}, fmt.Errorf("value %v out of range for INTEGER", i)
			}
			i64 := i.Int64()
			if i64 > int64(^uint32(0)>>1) || i64 < -int64(^uint32(0)>>1)-1 {
				return types.Value{}, fmt.Errorf("value %d out of range for INTEGER", i64)
			}
			return types.NewValue(int32(i64)), nil
		default:
			return types.Value{}, fmt.Errorf("cannot convert %T to INTEGER", v)
		}

	case "BIGINT":
		// Convert to int64
		switch v := value.Data.(type) {
		case int64:
			return value, nil // Already correct type
		case int32:
			return types.NewValue(int64(v)), nil
		case float32:
			return types.NewValue(int64(v)), nil
		case float64:
			return types.NewValue(int64(v)), nil
		case *big.Rat:
			// Convert big.Rat to int64
			i := v.Num()
			if v.Denom().Cmp(big.NewInt(1)) != 0 {
				return types.Value{}, fmt.Errorf("cannot convert decimal %v to BIGINT", v)
			}
			if !i.IsInt64() {
				return types.Value{}, fmt.Errorf("value %v out of range for BIGINT", i)
			}
			return types.NewValue(i.Int64()), nil
		default:
			return types.Value{}, fmt.Errorf("cannot convert %T to BIGINT", v)
		}

	default:
		// For other types, return as-is
		return value, nil
	}
}
