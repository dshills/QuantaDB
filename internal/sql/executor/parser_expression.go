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
	params  []types.Value
}

// newEvalContext creates a new evaluation context
func newEvalContext(row *Row, columns []catalog.Column, params []types.Value) *evalContext {
	return &evalContext{
		row:     row,
		columns: columns,
		params:  params,
	}
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

		// Handle NULL values in binary operations
		if left.IsNull() || right.IsNull() {
			// For most operators, NULL with anything returns NULL
			// Exception: IS NULL, IS NOT NULL (but those aren't handled here)
			return types.NewNullValue(), nil
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
		case parser.TokenPlus:
			// Handle addition
			switch l := left.Data.(type) {
			case int64:
				if r, ok := right.Data.(int64); ok {
					return types.NewValue(l + r), nil
				}
			case float64:
				if r, ok := right.Data.(float64); ok {
					return types.NewValue(l + r), nil
				}
			}
			return types.Value{}, fmt.Errorf("type mismatch in addition")
		case parser.TokenMinus:
			// Handle subtraction
			switch l := left.Data.(type) {
			case int64:
				if r, ok := right.Data.(int64); ok {
					return types.NewValue(l - r), nil
				}
			case float64:
				if r, ok := right.Data.(float64); ok {
					return types.NewValue(l - r), nil
				}
			}
			return types.Value{}, fmt.Errorf("type mismatch in subtraction")
		case parser.TokenStar:
			// Handle multiplication
			switch l := left.Data.(type) {
			case int64:
				if r, ok := right.Data.(int64); ok {
					return types.NewValue(l * r), nil
				}
			case float64:
				if r, ok := right.Data.(float64); ok {
					return types.NewValue(l * r), nil
				}
			}
			return types.Value{}, fmt.Errorf("type mismatch in multiplication")
		case parser.TokenSlash:
			// Handle division
			switch l := left.Data.(type) {
			case int64:
				if r, ok := right.Data.(int64); ok {
					if r == 0 {
						return types.Value{}, fmt.Errorf("division by zero")
					}
					// Integer division returns float for SQL compatibility
					return types.NewValue(float64(l) / float64(r)), nil
				}
			case float64:
				if r, ok := right.Data.(float64); ok {
					if r == 0 {
						return types.Value{}, fmt.Errorf("division by zero")
					}
					return types.NewValue(l / r), nil
				}
			}
			return types.Value{}, fmt.Errorf("type mismatch in division")
		case parser.TokenConcat:
			// Handle string concatenation (||)
			// SQL standard: NULL || anything = NULL
			if left.IsNull() || right.IsNull() {
				return types.NewNullValue(), nil
			}
			
			leftStr, ok1 := left.Data.(string)
			rightStr, ok2 := right.Data.(string)
			if !ok1 || !ok2 {
				return types.Value{}, fmt.Errorf("|| requires string operands")
			}
			return types.NewValue(leftStr + rightStr), nil
		default:
			return types.Value{}, fmt.Errorf("unsupported binary operator: %v", e.Operator)
		}

	case *parser.ParameterRef:
		// Handle parameter references ($1, $2, etc.)
		if e.Index < 1 || e.Index > len(ctx.params) {
			return types.Value{}, fmt.Errorf("parameter $%d out of range (have %d parameters)", e.Index, len(ctx.params))
		}
		return ctx.params[e.Index-1], nil

	case *parser.UnaryExpr:
		// Handle unary expressions like -1
		operand, err := evaluateExpression(e.Expr, ctx)
		if err != nil {
			return types.Value{}, err
		}

		switch e.Operator {
		case parser.TokenMinus:
			// Negate the value
			switch v := operand.Data.(type) {
			case int64:
				return types.NewValue(-v), nil
			case float64:
				return types.NewValue(-v), nil
			default:
				return types.Value{}, fmt.Errorf("cannot negate %T", v)
			}
		case parser.TokenNot:
			// Logical NOT
			if b, ok := operand.Data.(bool); ok {
				return types.NewValue(!b), nil
			}
			return types.Value{}, fmt.Errorf("NOT requires boolean operand")
		default:
			return types.Value{}, fmt.Errorf("unsupported unary operator: %v", e.Operator)
		}

	case *parser.ComparisonExpr:
		// Handle comparison expressions
		left, err := evaluateExpression(e.Left, ctx)
		if err != nil {
			return types.Value{}, err
		}

		right, err := evaluateExpression(e.Right, ctx)
		if err != nil {
			return types.Value{}, err
		}

		// Handle comparison operators
		cmp := types.CompareValues(left, right)
		switch e.Operator {
		case parser.TokenEqual:
			return types.NewValue(cmp == 0), nil
		case parser.TokenNotEqual:
			return types.NewValue(cmp != 0), nil
		case parser.TokenLess:
			return types.NewValue(cmp < 0), nil
		case parser.TokenLessEqual:
			return types.NewValue(cmp <= 0), nil
		case parser.TokenGreater:
			return types.NewValue(cmp > 0), nil
		case parser.TokenGreaterEqual:
			return types.NewValue(cmp >= 0), nil
		default:
			return types.Value{}, fmt.Errorf("unsupported comparison operator: %v", e.Operator)
		}

	case *parser.CaseExpr:
		// Evaluate CASE expression

		// For simple CASE, evaluate the main expression first
		var mainValue types.Value
		if e.Expr != nil {
			var err error
			mainValue, err = evaluateExpression(e.Expr, ctx)
			if err != nil {
				return types.Value{}, fmt.Errorf("failed to evaluate CASE expression: %w", err)
			}
		}

		// Evaluate WHEN clauses in order
		for _, when := range e.WhenList {
			// For simple CASE, compare mainValue with when.Condition
			// For searched CASE, evaluate when.Condition as boolean
			var matches bool

			if e.Expr != nil {
				// Simple CASE: compare mainValue with when condition
				whenValue, err := evaluateExpression(when.Condition, ctx)
				if err != nil {
					return types.Value{}, fmt.Errorf("failed to evaluate WHEN value: %w", err)
				}
				matches = types.CompareValues(mainValue, whenValue) == 0
			} else {
				// Searched CASE: evaluate condition as boolean
				condValue, err := evaluateExpression(when.Condition, ctx)
				if err != nil {
					return types.Value{}, fmt.Errorf("failed to evaluate WHEN condition: %w", err)
				}

				// Check if condition is true
				if condBool, ok := condValue.Data.(bool); ok {
					matches = condBool
				} else {
					return types.Value{}, fmt.Errorf("WHEN condition must evaluate to boolean, got %T", condValue.Data)
				}
			}

			if matches {
				// Return the result for this WHEN clause
				return evaluateExpression(when.Result, ctx)
			}
		}

		// No WHEN clause matched, return ELSE value or NULL
		if e.Else != nil {
			return evaluateExpression(e.Else, ctx)
		}

		// Return NULL if no ELSE clause
		return types.NullValue(types.Unknown), nil

	case *parser.SubstringExpr:
		// Evaluate SUBSTRING expression
		strVal, err := evaluateExpression(e.Str, ctx)
		if err != nil {
			return types.Value{}, err
		}

		if strVal.IsNull() {
			return types.NewNullValue(), nil
		}

		str, ok := strVal.Data.(string)
		if !ok {
			return types.Value{}, fmt.Errorf("SUBSTRING requires string value, got %T", strVal.Data)
		}

		// Evaluate start position
		startVal, err := evaluateExpression(e.Start, ctx)
		if err != nil {
			return types.Value{}, err
		}

		if startVal.IsNull() {
			return types.NewNullValue(), nil
		}

		var start int
		switch v := startVal.Data.(type) {
		case int64:
			start = int(v)
		case float64:
			start = int(v)
		default:
			return types.Value{}, fmt.Errorf("SUBSTRING start position must be numeric, got %T", startVal.Data)
		}

		// Convert to 0-based indexing (SQL uses 1-based)
		if start > 0 {
			start--
		} else if start < 0 {
			// Negative start means from end of string
			start = len(str) + start + 1
		} else {
			// SQL standard: start position 0 is treated as 1
			start = 0
		}

		// Ensure start is within bounds
		if start < 0 {
			start = 0
		}
		if start >= len(str) {
			return types.NewValue(""), nil
		}

		// Evaluate optional length
		var length int = len(str) - start // Default to rest of string
		if e.Length != nil {
			lengthVal, err := evaluateExpression(e.Length, ctx)
			if err != nil {
				return types.Value{}, err
			}

			if !lengthVal.IsNull() {
				switch v := lengthVal.Data.(type) {
				case int64:
					length = int(v)
				case float64:
					length = int(v)
				default:
					return types.Value{}, fmt.Errorf("SUBSTRING length must be numeric, got %T", lengthVal.Data)
				}

				if length < 0 {
					return types.Value{}, fmt.Errorf("SUBSTRING length cannot be negative")
				}
			}
		}

		// Extract substring
		end := start + length
		if end > len(str) {
			end = len(str)
		}

		return types.NewValue(str[start:end]), nil

	case *parser.InExpr:
		// Evaluate IN/NOT IN expression
		exprVal, err := evaluateExpression(e.Expr, ctx)
		if err != nil {
			return types.Value{}, err
		}

		// Handle NULL on left side
		if exprVal.IsNull() {
			return types.NewNullValue(), nil
		}

		// Handle subquery case
		if e.Subquery != nil {
			// Subquery evaluation in parser expressions requires the full
			// planner-executor pipeline. This path is typically not used
			// in production as queries go through the planner first.
			return types.Value{}, fmt.Errorf("subquery evaluation not supported in direct parser expression evaluation")
		}

		// Handle value list case
		var found bool
		var hasNull bool

		for _, valueExpr := range e.Values {
			valueVal, err := evaluateExpression(valueExpr, ctx)
			if err != nil {
				return types.Value{}, err
			}

			if valueVal.IsNull() {
				hasNull = true
				continue
			}

			// Compare values
			if types.CompareValues(exprVal, valueVal) == 0 {
				found = true
				break
			}
		}

		// SQL semantics for IN/NOT IN with NULLs:
		// - expr IN (list): TRUE if found, NULL if not found but list contains NULL, FALSE otherwise
		// - expr NOT IN (list): FALSE if found, NULL if not found but list contains NULL, TRUE otherwise

		if found {
			if e.Not {
				return types.NewValue(false), nil // Found but we want NOT IN
			} else {
				return types.NewValue(true), nil // Found and we want IN
			}
		} else {
			if hasNull {
				return types.NewNullValue(), nil // Not found but NULL present
			} else {
				if e.Not {
					return types.NewValue(true), nil // Not found and we want NOT IN
				} else {
					return types.NewValue(false), nil // Not found and we want IN
				}
			}
		}

	case *parser.ExistsExpr:
		// Evaluate EXISTS/NOT EXISTS expression
		// EXISTS always requires a subquery, and subquery evaluation in parser
		// expressions requires the full planner-executor pipeline. This path is
		// typically not used in production as queries go through the planner first.
		return types.Value{}, fmt.Errorf("EXISTS/NOT EXISTS evaluation not supported in direct parser expression evaluation")

	default:
		return types.Value{}, fmt.Errorf("unsupported expression type: %T", expr)
	}
}
