package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// evaluateBinaryOp evaluates a binary operation.
func evaluateBinaryOp(left types.Value, op parser.TokenType, right types.Value) (types.Value, error) {
	// Handle NULL values
	if left.IsNull() || right.IsNull() {
		return types.NullValue(types.Unknown), nil
	}

	switch op {
	case parser.TokenPlus:
		return addValues(left, right)
	case parser.TokenMinus:
		return subtractValues(left, right)
	case parser.TokenStar:
		return multiplyValues(left, right)
	case parser.TokenSlash:
		return divideValues(left, right)
	case parser.TokenPercent:
		return moduloValues(left, right)
	case parser.TokenAnd:
		return andValues(left, right)
	case parser.TokenOr:
		return orValues(left, right)
	default:
		return types.NullValue(types.Unknown), fmt.Errorf("unsupported binary operator: %s", op)
	}
}

// evaluateComparison evaluates a comparison operation.
func evaluateComparison(left types.Value, op parser.TokenType, right types.Value) (types.Value, error) {
	// Handle NULL values - comparisons with NULL always return NULL
	if left.IsNull() || right.IsNull() {
		return types.NullValue(types.Boolean), nil
	}

	cmp, err := left.Compare(right)
	if err != nil {
		return types.NullValue(types.Boolean), err
	}

	var result bool
	switch op {
	case parser.TokenEqual:
		result = cmp == 0
	case parser.TokenNotEqual:
		result = cmp != 0
	case parser.TokenLess:
		result = cmp < 0
	case parser.TokenLessEqual:
		result = cmp <= 0
	case parser.TokenGreater:
		result = cmp > 0
	case parser.TokenGreaterEqual:
		result = cmp >= 0
	default:
		return types.NullValue(types.Boolean), fmt.Errorf("unsupported comparison operator: %s", op)
	}

	return types.NewValue(result), nil
}

// Arithmetic operations

func addValues(left, right types.Value) (types.Value, error) {
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
	return types.NullValue(types.Unknown), fmt.Errorf("cannot add %T and %T", left.Data, right.Data)
}

func subtractValues(left, right types.Value) (types.Value, error) {
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
	return types.NullValue(types.Unknown), fmt.Errorf("cannot subtract %T and %T", left.Data, right.Data)
}

func multiplyValues(left, right types.Value) (types.Value, error) {
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
	return types.NullValue(types.Unknown), fmt.Errorf("cannot multiply %T and %T", left.Data, right.Data)
}

func divideValues(left, right types.Value) (types.Value, error) {
	switch l := left.Data.(type) {
	case int64:
		if r, ok := right.Data.(int64); ok {
			if r == 0 {
				return types.NullValue(types.Unknown), fmt.Errorf("division by zero")
			}
			return types.NewValue(l / r), nil
		}
	case float64:
		if r, ok := right.Data.(float64); ok {
			if r == 0 {
				return types.NullValue(types.Unknown), fmt.Errorf("division by zero")
			}
			return types.NewValue(l / r), nil
		}
	}
	return types.NullValue(types.Unknown), fmt.Errorf("cannot divide %T by %T", left.Data, right.Data)
}

func moduloValues(left, right types.Value) (types.Value, error) {
	switch l := left.Data.(type) {
	case int64:
		if r, ok := right.Data.(int64); ok {
			if r == 0 {
				return types.NullValue(types.Unknown), fmt.Errorf("modulo by zero")
			}
			return types.NewValue(l % r), nil
		}
	}
	return types.NullValue(types.Unknown), fmt.Errorf("cannot modulo %T by %T", left.Data, right.Data)
}

// Logical operations

func andValues(left, right types.Value) (types.Value, error) {
	l, ok1 := left.Data.(bool)
	r, ok2 := right.Data.(bool)
	if !ok1 || !ok2 {
		return types.NullValue(types.Boolean), fmt.Errorf("AND requires boolean operands")
	}
	return types.NewValue(l && r), nil
}

func orValues(left, right types.Value) (types.Value, error) {
	l, ok1 := left.Data.(bool)
	r, ok2 := right.Data.(bool)
	if !ok1 || !ok2 {
		return types.NullValue(types.Boolean), fmt.Errorf("OR requires boolean operands")
	}
	return types.NewValue(l || r), nil
}