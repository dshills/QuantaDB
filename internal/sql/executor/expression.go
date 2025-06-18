package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// ExprEvaluator evaluates expressions against rows.
type ExprEvaluator interface {
	// Eval evaluates the expression for the given row.
	Eval(row *Row, ctx *ExecContext) (types.Value, error)
}

// buildExprEvaluator builds an evaluator for a planner expression.
func buildExprEvaluator(expr planner.Expression) (ExprEvaluator, error) {
	return buildExprEvaluatorWithSchema(expr, nil)
}

// buildExprEvaluatorWithSchema builds an evaluator with a known schema for column resolution.
func buildExprEvaluatorWithSchema(expr planner.Expression, schema *Schema) (ExprEvaluator, error) {
	switch e := expr.(type) {
	case *planner.Literal:
		return &literalEvaluator{value: e.Value}, nil

	case *planner.ColumnRef:
		// Resolve column index if schema is provided
		columnIdx := -1
		if schema != nil {
			for i, col := range schema.Columns {
				if col.Name == e.ColumnName {
					columnIdx = i
					break
				}
			}
		}

		return &columnRefEvaluator{
			columnName: e.ColumnName,
			tableAlias: e.TableAlias,
			columnIdx:  columnIdx,
			resolved:   columnIdx >= 0,
		}, nil

	case *planner.BinaryOp:
		left, err := buildExprEvaluatorWithSchema(e.Left, schema)
		if err != nil {
			return nil, err
		}
		right, err := buildExprEvaluatorWithSchema(e.Right, schema)
		if err != nil {
			return nil, err
		}
		return &binaryOpEvaluator{
			left:     left,
			right:    right,
			operator: e.Operator,
			dataType: e.Type,
		}, nil

	case *planner.UnaryOp:
		operand, err := buildExprEvaluatorWithSchema(e.Expr, schema)
		if err != nil {
			return nil, err
		}
		return &unaryOpEvaluator{
			operand:  operand,
			operator: e.Operator,
			dataType: e.Type,
		}, nil

	case *planner.Star:
		return nil, fmt.Errorf("star expression not supported in this context")

	case *planner.FunctionCall:
		// For non-aggregate functions, we would handle them here
		// For now, return an error as we only support aggregate functions
		return nil, fmt.Errorf("non-aggregate function calls not yet supported: %s", e.Name)

	case *planner.ParameterRef:
		return &parameterRefEvaluator{
			index:    e.Index,
			dataType: e.Type,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// literalEvaluator evaluates literal values.
type literalEvaluator struct {
	value types.Value
}

func (e *literalEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	return e.value, nil
}

// columnRefEvaluator evaluates column references.
type columnRefEvaluator struct {
	columnName string
	tableAlias string
	columnIdx  int
	resolved   bool
}

func (e *columnRefEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	if !e.resolved {
		return types.NewNullValue(), fmt.Errorf("column %s not resolved", e.columnName)
	}

	if e.columnIdx < 0 || e.columnIdx >= len(row.Values) {
		return types.NewNullValue(), fmt.Errorf("column index %d out of range", e.columnIdx)
	}

	return row.Values[e.columnIdx], nil
}

// binaryOpEvaluator evaluates binary operations.
type binaryOpEvaluator struct {
	left     ExprEvaluator
	right    ExprEvaluator
	operator planner.BinaryOperator
	dataType types.DataType
}

func (e *binaryOpEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Evaluate operands
	leftVal, err := e.left.Eval(row, ctx)
	if err != nil {
		return types.NewNullValue(), err
	}

	rightVal, err := e.right.Eval(row, ctx)
	if err != nil {
		return types.NewNullValue(), err
	}

	// Handle NULL values
	if leftVal.IsNull() || rightVal.IsNull() {
		// Most operations with NULL return NULL
		// Exceptions would be IS NULL, IS NOT NULL
		return types.NewNullValue(), nil
	}

	// Evaluate based on operator
	switch e.operator {
	// Arithmetic operators
	case planner.OpAdd:
		return e.evalArithmetic(leftVal, rightVal, func(a, b interface{}) interface{} {
			switch a := a.(type) {
			case int64:
				if b, ok := b.(int64); ok {
					return a + b
				}
			case float64:
				if b, ok := b.(float64); ok {
					return a + b
				}
			}
			return nil
		})

	case planner.OpSubtract:
		return e.evalArithmetic(leftVal, rightVal, func(a, b interface{}) interface{} {
			switch a := a.(type) {
			case int64:
				if b, ok := b.(int64); ok {
					return a - b
				}
			case float64:
				if b, ok := b.(float64); ok {
					return a - b
				}
			}
			return nil
		})

	case planner.OpMultiply:
		return e.evalArithmetic(leftVal, rightVal, func(a, b interface{}) interface{} {
			switch a := a.(type) {
			case int64:
				if b, ok := b.(int64); ok {
					return a * b
				}
			case float64:
				if b, ok := b.(float64); ok {
					return a * b
				}
			}
			return nil
		})

	case planner.OpDivide:
		return e.evalArithmetic(leftVal, rightVal, func(a, b interface{}) interface{} {
			switch a := a.(type) {
			case int64:
				if b, ok := b.(int64); ok {
					if b == 0 {
						return nil // Division by zero
					}
					return a / b
				}
			case float64:
				if b, ok := b.(float64); ok {
					if b == 0 {
						return nil // Division by zero
					}
					return a / b
				}
			}
			return nil
		})

	// Comparison operators
	case planner.OpEqual:
		return e.evalComparison(leftVal, rightVal, func(cmp int) bool { return cmp == 0 })

	case planner.OpNotEqual:
		return e.evalComparison(leftVal, rightVal, func(cmp int) bool { return cmp != 0 })

	case planner.OpLess:
		return e.evalComparison(leftVal, rightVal, func(cmp int) bool { return cmp < 0 })

	case planner.OpLessEqual:
		return e.evalComparison(leftVal, rightVal, func(cmp int) bool { return cmp <= 0 })

	case planner.OpGreater:
		return e.evalComparison(leftVal, rightVal, func(cmp int) bool { return cmp > 0 })

	case planner.OpGreaterEqual:
		return e.evalComparison(leftVal, rightVal, func(cmp int) bool { return cmp >= 0 })

	// Logical operators
	case planner.OpAnd:
		left, ok1 := leftVal.Data.(bool)
		right, ok2 := rightVal.Data.(bool)
		if !ok1 || !ok2 {
			return types.NewNullValue(), fmt.Errorf("AND requires boolean operands")
		}
		return types.NewValue(left && right), nil

	case planner.OpOr:
		left, ok1 := leftVal.Data.(bool)
		right, ok2 := rightVal.Data.(bool)
		if !ok1 || !ok2 {
			return types.NewNullValue(), fmt.Errorf("OR requires boolean operands")
		}
		return types.NewValue(left || right), nil

	case planner.OpModulo:
		return e.evalArithmetic(leftVal, rightVal, func(a, b interface{}) interface{} {
			switch a := a.(type) {
			case int64:
				if b, ok := b.(int64); ok {
					if b == 0 {
						return nil // Modulo by zero
					}
					return a % b
				}
			}
			return nil
		})

	case planner.OpConcat:
		leftStr, ok1 := leftVal.Data.(string)
		rightStr, ok2 := rightVal.Data.(string)
		if !ok1 || !ok2 {
			return types.NewNullValue(), fmt.Errorf("CONCAT requires string operands")
		}
		return types.NewValue(leftStr + rightStr), nil

	case planner.OpLike, planner.OpNotLike:
		// Simple pattern matching - would need full LIKE implementation
		return types.NewValue(false), nil

	case planner.OpIn, planner.OpNotIn:
		// IN/NOT IN would need list comparison - simplified for now
		return types.NewValue(false), nil

	case planner.OpIs, planner.OpIsNot:
		// IS/IS NOT for NULL checking - simplified for now
		return types.NewValue(false), nil

	default:
		return types.NewNullValue(), fmt.Errorf("unsupported binary operator: %v", e.operator)
	}
}

// evalArithmetic evaluates arithmetic operations.
func (e *binaryOpEvaluator) evalArithmetic(left, right types.Value, op func(a, b interface{}) interface{}) (types.Value, error) {
	result := op(left.Data, right.Data)
	if result == nil {
		return types.NewNullValue(), fmt.Errorf("type mismatch in arithmetic operation")
	}
	return types.NewValue(result), nil
}

// evalComparison evaluates comparison operations.
func (e *binaryOpEvaluator) evalComparison(left, right types.Value, op func(int) bool) (types.Value, error) {
	// Simple comparison based on Go's comparable types
	// In a real implementation, we'd use the type system's Compare method
	cmp := 0

	switch l := left.Data.(type) {
	case int64:
		if r, ok := right.Data.(int64); ok {
			if l < r {
				cmp = -1
			} else if l > r {
				cmp = 1
			}
		} else {
			return types.NewNullValue(), fmt.Errorf("type mismatch in comparison")
		}

	case string:
		if r, ok := right.Data.(string); ok {
			if l < r {
				cmp = -1
			} else if l > r {
				cmp = 1
			}
		} else {
			return types.NewNullValue(), fmt.Errorf("type mismatch in comparison")
		}

	case bool:
		if r, ok := right.Data.(bool); ok {
			if !l && r {
				cmp = -1
			} else if l && !r {
				cmp = 1
			}
		} else {
			return types.NewNullValue(), fmt.Errorf("type mismatch in comparison")
		}

	default:
		return types.NewNullValue(), fmt.Errorf("unsupported type for comparison: %T", l)
	}

	return types.NewValue(op(cmp)), nil
}

// unaryOpEvaluator evaluates unary operations.
type unaryOpEvaluator struct {
	operand  ExprEvaluator
	operator planner.UnaryOperator
	dataType types.DataType
}

func (e *unaryOpEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Evaluate operand
	val, err := e.operand.Eval(row, ctx)
	if err != nil {
		return types.NewNullValue(), err
	}

	switch e.operator {
	case planner.OpNot:
		if val.IsNull() {
			return types.NewNullValue(), nil
		}
		b, ok := val.Data.(bool)
		if !ok {
			return types.NewNullValue(), fmt.Errorf("NOT requires boolean operand")
		}
		return types.NewValue(!b), nil

	case planner.OpNegate:
		if val.IsNull() {
			return types.NewNullValue(), nil
		}
		switch v := val.Data.(type) {
		case int64:
			return types.NewValue(-v), nil
		case float64:
			return types.NewValue(-v), nil
		default:
			return types.NewNullValue(), fmt.Errorf("cannot negate %T", v)
		}

	case planner.OpIsNull:
		return types.NewValue(val.IsNull()), nil

	case planner.OpIsNotNull:
		return types.NewValue(!val.IsNull()), nil

	default:
		return types.NewNullValue(), fmt.Errorf("unsupported unary operator: %v", e.operator)
	}
}

// parameterRefEvaluator evaluates parameter references ($1, $2, etc).
type parameterRefEvaluator struct {
	index    int
	dataType types.DataType
}

func (e *parameterRefEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Check if parameters are available
	if ctx == nil || ctx.Params == nil {
		return types.NewNullValue(), fmt.Errorf("no parameters available in execution context")
	}

	// Validate parameter index (1-based)
	if e.index < 1 || e.index > len(ctx.Params) {
		return types.NewNullValue(), fmt.Errorf("parameter $%d out of range (have %d parameters)", e.index, len(ctx.Params))
	}

	// Return the parameter value (convert 1-based to 0-based index)
	return ctx.Params[e.index-1], nil
}
