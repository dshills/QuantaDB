package executor

import (
	"fmt"
	"math"
	"strings"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// evaluateFunction evaluates a function call expression
func evaluateFunction(fn *parser.FunctionCall, ctx *evalContext) (types.Value, error) {
	funcName := strings.ToUpper(fn.Name)

	// Evaluate all arguments first
	args := make([]types.Value, len(fn.Args))
	for i, arg := range fn.Args {
		val, err := evaluateExpression(arg, ctx)
		if err != nil {
			return types.Value{}, fmt.Errorf("error evaluating function argument %d: %w", i+1, err)
		}
		args[i] = val
	}

	// Handle functions based on name
	switch funcName {
	// String functions
	case "LENGTH", "LEN":
		return evalLength(args)
	case "UPPER", "UCASE":
		return evalUpper(args)
	case "LOWER", "LCASE":
		return evalLower(args)
	case "SUBSTRING", "SUBSTR":
		return evalSubstring(args)
	case "TRIM":
		return evalTrim(args)
	case "LTRIM":
		return evalLTrim(args)
	case "RTRIM":
		return evalRTrim(args)
	case "CONCAT":
		return evalConcat(args)

	// Numeric functions
	case "ABS":
		return evalAbs(args)
	case "ROUND":
		return evalRound(args)
	case "FLOOR":
		return evalFloor(args)
	case "CEIL", "CEILING":
		return evalCeil(args)
	case "MOD":
		return evalMod(args)
	case "POWER", "POW":
		return evalPower(args)
	case "SQRT":
		return evalSqrt(args)

	// Type conversion functions
	case "CAST":
		// CAST is handled differently in the parser
		return types.Value{}, fmt.Errorf("CAST should be handled by parser")

	// Conditional functions
	case "COALESCE":
		return evalCoalesce(args)
	case "NULLIF":
		return evalNullIf(args)

	default:
		return types.Value{}, fmt.Errorf("unsupported function: %s", funcName)
	}
}

// String functions

func evalLength(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("LENGTH requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	str, ok := args[0].Data.(string)
	if !ok {
		return types.Value{}, fmt.Errorf("LENGTH requires string argument")
	}

	return types.NewValue(int64(len(str))), nil
}

func evalUpper(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("UPPER requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	str, ok := args[0].Data.(string)
	if !ok {
		return types.Value{}, fmt.Errorf("UPPER requires string argument")
	}

	return types.NewValue(strings.ToUpper(str)), nil
}

func evalLower(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("LOWER requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	str, ok := args[0].Data.(string)
	if !ok {
		return types.Value{}, fmt.Errorf("LOWER requires string argument")
	}

	return types.NewValue(strings.ToLower(str)), nil
}

func evalSubstring(args []types.Value) (types.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return types.Value{}, fmt.Errorf("SUBSTRING requires 2 or 3 arguments, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	str, ok := args[0].Data.(string)
	if !ok {
		return types.Value{}, fmt.Errorf("SUBSTRING requires string as first argument")
	}

	// Get start position (1-based in SQL)
	if args[1].IsNull() {
		return types.NewNullValue(), nil
	}

	var start int64
	switch v := args[1].Data.(type) {
	case int64:
		start = v
	case int32:
		start = int64(v)
	default:
		return types.Value{}, fmt.Errorf("SUBSTRING requires integer as second argument")
	}

	// Convert to 0-based indexing
	if start > 0 {
		start--
	} else if start < 0 {
		// Negative positions count from the end
		start = int64(len(str)) + start
	}

	// Handle length argument if provided
	length := int64(len(str))
	if len(args) == 3 {
		if args[2].IsNull() {
			return types.NewNullValue(), nil
		}

		switch v := args[2].Data.(type) {
		case int64:
			length = v
		case int32:
			length = int64(v)
		default:
			return types.Value{}, fmt.Errorf("SUBSTRING requires integer as third argument")
		}
	}

	// Bounds checking
	if start < 0 {
		start = 0
	}
	if start >= int64(len(str)) {
		return types.NewValue(""), nil
	}

	end := start + length
	if end > int64(len(str)) {
		end = int64(len(str))
	}

	return types.NewValue(str[start:end]), nil
}

func evalTrim(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("TRIM requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	str, ok := args[0].Data.(string)
	if !ok {
		return types.Value{}, fmt.Errorf("TRIM requires string argument")
	}

	return types.NewValue(strings.TrimSpace(str)), nil
}

func evalLTrim(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("LTRIM requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	str, ok := args[0].Data.(string)
	if !ok {
		return types.Value{}, fmt.Errorf("LTRIM requires string argument")
	}

	return types.NewValue(strings.TrimLeft(str, " \t\n\r")), nil
}

func evalRTrim(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("RTRIM requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	str, ok := args[0].Data.(string)
	if !ok {
		return types.Value{}, fmt.Errorf("RTRIM requires string argument")
	}

	return types.NewValue(strings.TrimRight(str, " \t\n\r")), nil
}

func evalConcat(args []types.Value) (types.Value, error) {
	if len(args) < 2 {
		return types.Value{}, fmt.Errorf("CONCAT requires at least 2 arguments, got %d", len(args))
	}

	// In SQL standard, if any argument is NULL, result is NULL
	for _, arg := range args {
		if arg.IsNull() {
			return types.NewNullValue(), nil
		}
	}

	// Concatenate all strings
	var result strings.Builder
	for i, arg := range args {
		str, ok := arg.Data.(string)
		if !ok {
			return types.Value{}, fmt.Errorf("CONCAT argument %d must be string", i+1)
		}
		result.WriteString(str)
	}

	return types.NewValue(result.String()), nil
}

// Numeric functions

func evalAbs(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("ABS requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	switch v := args[0].Data.(type) {
	case int64:
		if v < 0 {
			return types.NewValue(-v), nil
		}
		return types.NewValue(v), nil
	case int32:
		if v < 0 {
			return types.NewValue(int64(-v)), nil
		}
		return types.NewValue(int64(v)), nil
	case float64:
		return types.NewValue(math.Abs(v)), nil
	default:
		return types.Value{}, fmt.Errorf("ABS requires numeric argument")
	}
}

func evalRound(args []types.Value) (types.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return types.Value{}, fmt.Errorf("ROUND requires 1 or 2 arguments, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	var val float64
	switch v := args[0].Data.(type) {
	case int64:
		val = float64(v)
	case int32:
		val = float64(v)
	case float64:
		val = v
	default:
		return types.Value{}, fmt.Errorf("ROUND requires numeric argument")
	}

	// Get precision (default 0)
	precision := 0
	if len(args) == 2 {
		if args[1].IsNull() {
			return types.NewNullValue(), nil
		}

		switch v := args[1].Data.(type) {
		case int64:
			precision = int(v)
		case int32:
			precision = int(v)
		default:
			return types.Value{}, fmt.Errorf("ROUND precision must be integer")
		}
	}

	// Round to specified precision
	multiplier := math.Pow(10, float64(precision))
	result := math.Round(val*multiplier) / multiplier

	return types.NewValue(result), nil
}

func evalFloor(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("FLOOR requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	switch v := args[0].Data.(type) {
	case int64:
		return types.NewValue(v), nil // Already floored
	case int32:
		return types.NewValue(int64(v)), nil // Already floored
	case float64:
		return types.NewValue(math.Floor(v)), nil
	default:
		return types.Value{}, fmt.Errorf("FLOOR requires numeric argument")
	}
}

func evalCeil(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("CEIL requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	switch v := args[0].Data.(type) {
	case int64:
		return types.NewValue(v), nil // Already ceiled
	case int32:
		return types.NewValue(int64(v)), nil // Already ceiled
	case float64:
		return types.NewValue(math.Ceil(v)), nil
	default:
		return types.Value{}, fmt.Errorf("CEIL requires numeric argument")
	}
}

func evalMod(args []types.Value) (types.Value, error) {
	if len(args) != 2 {
		return types.Value{}, fmt.Errorf("MOD requires 2 arguments, got %d", len(args))
	}

	if args[0].IsNull() || args[1].IsNull() {
		return types.NewNullValue(), nil
	}

	var dividend, divisor int64

	switch v := args[0].Data.(type) {
	case int64:
		dividend = v
	case int32:
		dividend = int64(v)
	default:
		return types.Value{}, fmt.Errorf("MOD requires integer arguments")
	}

	switch v := args[1].Data.(type) {
	case int64:
		divisor = v
	case int32:
		divisor = int64(v)
	default:
		return types.Value{}, fmt.Errorf("MOD requires integer arguments")
	}

	if divisor == 0 {
		return types.Value{}, fmt.Errorf("division by zero in MOD")
	}

	return types.NewValue(dividend % divisor), nil
}

func evalPower(args []types.Value) (types.Value, error) {
	if len(args) != 2 {
		return types.Value{}, fmt.Errorf("POWER requires 2 arguments, got %d", len(args))
	}

	if args[0].IsNull() || args[1].IsNull() {
		return types.NewNullValue(), nil
	}

	var base, exponent float64

	switch v := args[0].Data.(type) {
	case int64:
		base = float64(v)
	case int32:
		base = float64(v)
	case float64:
		base = v
	default:
		return types.Value{}, fmt.Errorf("POWER requires numeric arguments")
	}

	switch v := args[1].Data.(type) {
	case int64:
		exponent = float64(v)
	case int32:
		exponent = float64(v)
	case float64:
		exponent = v
	default:
		return types.Value{}, fmt.Errorf("POWER requires numeric arguments")
	}

	result := math.Pow(base, exponent)
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return types.Value{}, fmt.Errorf("POWER result out of range")
	}

	return types.NewValue(result), nil
}

func evalSqrt(args []types.Value) (types.Value, error) {
	if len(args) != 1 {
		return types.Value{}, fmt.Errorf("SQRT requires 1 argument, got %d", len(args))
	}

	if args[0].IsNull() {
		return types.NewNullValue(), nil
	}

	var val float64
	switch v := args[0].Data.(type) {
	case int64:
		val = float64(v)
	case int32:
		val = float64(v)
	case float64:
		val = v
	default:
		return types.Value{}, fmt.Errorf("SQRT requires numeric argument")
	}

	if val < 0 {
		return types.Value{}, fmt.Errorf("SQRT of negative number")
	}

	return types.NewValue(math.Sqrt(val)), nil
}

// Conditional functions

func evalCoalesce(args []types.Value) (types.Value, error) {
	if len(args) < 1 {
		return types.Value{}, fmt.Errorf("COALESCE requires at least 1 argument")
	}

	// Return the first non-NULL value
	for _, arg := range args {
		if !arg.IsNull() {
			return arg, nil
		}
	}

	// All values are NULL
	return types.NewNullValue(), nil
}

func evalNullIf(args []types.Value) (types.Value, error) {
	if len(args) != 2 {
		return types.Value{}, fmt.Errorf("NULLIF requires 2 arguments, got %d", len(args))
	}

	// If either argument is NULL, return the first argument
	if args[0].IsNull() || args[1].IsNull() {
		return args[0], nil
	}

	// If values are equal, return NULL
	if types.CompareValues(args[0], args[1]) == 0 {
		return types.NewNullValue(), nil
	}

	// Otherwise return the first value
	return args[0], nil
}
