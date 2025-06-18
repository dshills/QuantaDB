package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// evalContext provides context for expression evaluation
type evalContext struct {
	row     *Row
	columns []catalog.Column
}

// evaluateExpression evaluates a parser expression in the context of a row
func evaluateExpression(expr parser.Expression, ctx *evalContext) (types.Value, error) {
	switch e := expr.(type) {
	case *parser.Literal:
		return e.Value, nil
		
	case *parser.Identifier:
		// Find column index
		for i, col := range ctx.columns {
			if col.Name == e.Name {
				return ctx.row.Values[i], nil
			}
		}
		return types.Value{}, fmt.Errorf("column '%s' not found", e.Name)
		
	case *parser.BinaryExpr:
		left, err := evaluateExpression(e.Left, ctx)
		if err != nil {
			return types.Value{}, err
		}
		
		right, err := evaluateExpression(e.Right, ctx)
		if err != nil {
			return types.Value{}, err
		}
		
		// Handle comparison operators
		switch e.Operator {
		case parser.TokenEqual:
			return types.NewValue(types.CompareValues(left, right) == 0), nil
		case parser.TokenNotEqual:
			return types.NewValue(types.CompareValues(left, right) != 0), nil
		case parser.TokenLess:
			return types.NewValue(types.CompareValues(left, right) < 0), nil
		case parser.TokenLessEqual:
			return types.NewValue(types.CompareValues(left, right) <= 0), nil
		case parser.TokenGreater:
			return types.NewValue(types.CompareValues(left, right) > 0), nil
		case parser.TokenGreaterEqual:
			return types.NewValue(types.CompareValues(left, right) >= 0), nil
		case parser.TokenAnd:
			// For AND, both values must be true
			leftBool, ok1 := left.Data.(bool)
			rightBool, ok2 := right.Data.(bool)
			if !ok1 || !ok2 {
				return types.Value{}, fmt.Errorf("AND requires boolean operands")
			}
			return types.NewValue(leftBool && rightBool), nil
		case parser.TokenOr:
			// For OR, at least one value must be true
			leftBool, ok1 := left.Data.(bool)
			rightBool, ok2 := right.Data.(bool)
			if !ok1 || !ok2 {
				return types.Value{}, fmt.Errorf("OR requires boolean operands")
			}
			return types.NewValue(leftBool || rightBool), nil
		default:
			return types.Value{}, fmt.Errorf("unsupported binary operator: %v", e.Operator)
		}
		
	default:
		return types.Value{}, fmt.Errorf("unsupported expression type: %T", expr)
	}
}