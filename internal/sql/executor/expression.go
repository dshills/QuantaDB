package executor

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

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
	return buildExprEvaluatorWithExecutor(expr, schema, nil)
}

// buildExprEvaluatorWithExecutor builds an evaluator with schema and executor for subquery support.
func buildExprEvaluatorWithExecutor(expr planner.Expression, schema *Schema, executor *BasicExecutor) (ExprEvaluator, error) {
	switch e := expr.(type) {
	case *planner.Literal:
		return &literalEvaluator{value: e.Value}, nil

	case *planner.ColumnRef:
		// Resolve column index if schema is provided
		columnIdx := -1
		if schema != nil {
			// If we have a table alias, try to match by table alias and column name
			if e.TableAlias != "" {
				for i, col := range schema.Columns {
					if col.Name == e.ColumnName && col.TableAlias == e.TableAlias {
						columnIdx = i
						break
					}
				}
			}

			// If not found or no table alias, try exact match with column name
			if columnIdx == -1 {
				// Count how many columns match the name (for ambiguity detection)
				matches := 0
				for i, col := range schema.Columns {
					if col.Name == e.ColumnName {
						if matches == 0 {
							columnIdx = i
						}
						matches++
					}
				}

				// If multiple matches and no table alias specified, it's ambiguous
				if matches > 1 && e.TableAlias == "" {
					return nil, fmt.Errorf("ambiguous column reference: %s", e.ColumnName)
				}
			}
		}

		// For correlated subqueries, check if this is a correlation reference
		isCorrelated := false
		if columnIdx < 0 && executor != nil {
			// Check if this could be a correlated column reference
			// We'll mark it as resolved to be handled at execution time
			isCorrelated = true
		}

		return &columnRefEvaluator{
			columnName: e.ColumnName,
			tableAlias: e.TableAlias,
			columnIdx:  columnIdx,
			resolved:   columnIdx >= 0 || isCorrelated,
		}, nil

	case *planner.BinaryOp:
		left, err := buildExprEvaluatorWithExecutor(e.Left, schema, executor)
		if err != nil {
			return nil, err
		}
		right, err := buildExprEvaluatorWithExecutor(e.Right, schema, executor)
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
		operand, err := buildExprEvaluatorWithExecutor(e.Expr, schema, executor)
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
		// Handle specific functions
		switch e.Name {
		case "SUBSTRING":
			// SUBSTRING has 2 or 3 arguments: string, start [, length]
			if len(e.Args) < 2 || len(e.Args) > 3 {
				return nil, fmt.Errorf("SUBSTRING requires 2 or 3 arguments")
			}

			strEval, err := buildExprEvaluatorWithExecutor(e.Args[0], schema, executor)
			if err != nil {
				return nil, fmt.Errorf("failed to build string evaluator for SUBSTRING: %w", err)
			}

			startEval, err := buildExprEvaluatorWithExecutor(e.Args[1], schema, executor)
			if err != nil {
				return nil, fmt.Errorf("failed to build start evaluator for SUBSTRING: %w", err)
			}

			var lengthEval ExprEvaluator
			if len(e.Args) == 3 && e.Args[2] != nil {
				lengthEval, err = buildExprEvaluatorWithExecutor(e.Args[2], schema, executor)
				if err != nil {
					return nil, fmt.Errorf("failed to build length evaluator for SUBSTRING: %w", err)
				}
			}

			return &substringEvaluator{
				strEval:    strEval,
				startEval:  startEval,
				lengthEval: lengthEval,
			}, nil

		default:
			return nil, fmt.Errorf("unsupported function: %s", e.Name)
		}

	case *planner.ExtractExpr:
		// Build evaluator for the FROM expression
		fromEval, err := buildExprEvaluatorWithExecutor(e.From, schema, executor)
		if err != nil {
			return nil, err
		}
		return &extractEvaluator{
			field:    e.Field,
			fromEval: fromEval,
		}, nil

	case *planner.ParameterRef:
		return &parameterRefEvaluator{
			index:    e.Index,
			dataType: e.Type,
		}, nil

	case *planner.SubqueryExpr:
		// Create a subquery evaluator with the executor reference
		return &subqueryEvaluator{
			subplan:      e.Subplan,
			dataType:     e.Type,
			executor:     executor,
			isCorrelated: e.IsCorrelated,
		}, nil

	case *planner.ExistsExpr:
		// Build evaluator for the subquery
		subqueryEval, err := buildExprEvaluatorWithExecutor(e.Subquery, schema, executor)
		if err != nil {
			return nil, err
		}

		return &existsEvaluator{
			subqueryEval: subqueryEval,
			not:          e.Not,
		}, nil

	case *planner.InExpr:
		// Build evaluator for the left expression
		exprEval, err := buildExprEvaluatorWithExecutor(e.Expr, schema, executor)
		if err != nil {
			return nil, err
		}

		if e.Subquery != nil {
			// IN with subquery
			subqueryEval, err := buildExprEvaluatorWithExecutor(e.Subquery, schema, executor)
			if err != nil {
				return nil, err
			}

			return &inSubqueryEvaluator{
				exprEval:     exprEval,
				subqueryEval: subqueryEval,
				not:          e.Not,
			}, nil
		}
		// IN with value list
		var valueEvals []ExprEvaluator
		for _, value := range e.Values {
			valueEval, err := buildExprEvaluatorWithSchema(value, schema)
			if err != nil {
				return nil, err
			}
			valueEvals = append(valueEvals, valueEval)
		}

		return &inValuesEvaluator{
			exprEval:   exprEval,
			valueEvals: valueEvals,
			not:        e.Not,
		}, nil

	case *planner.CaseExpr:
		// Build evaluator for CASE expression
		var caseEval ExprEvaluator
		if e.Expr != nil {
			// Simple CASE - build evaluator for the main expression
			var err error
			caseEval, err = buildExprEvaluatorWithExecutor(e.Expr, schema, executor)
			if err != nil {
				return nil, fmt.Errorf("failed to build CASE expression evaluator: %w", err)
			}
		}

		// Build evaluators for WHEN clauses
		var whenEvals []caseWhenEvaluator
		for i, when := range e.WhenList {
			condEval, err := buildExprEvaluatorWithExecutor(when.Condition, schema, executor)
			if err != nil {
				return nil, fmt.Errorf("failed to build WHEN condition evaluator %d: %w", i, err)
			}

			resultEval, err := buildExprEvaluatorWithExecutor(when.Result, schema, executor)
			if err != nil {
				return nil, fmt.Errorf("failed to build THEN result evaluator %d: %w", i, err)
			}

			whenEvals = append(whenEvals, caseWhenEvaluator{
				conditionEval: condEval,
				resultEval:    resultEval,
			})
		}

		// Build evaluator for ELSE clause
		var elseEval ExprEvaluator
		if e.Else != nil {
			var err error
			elseEval, err = buildExprEvaluatorWithExecutor(e.Else, schema, executor)
			if err != nil {
				return nil, fmt.Errorf("failed to build ELSE evaluator: %w", err)
			}
		}

		return &caseExprEvaluator{
			caseEval:  caseEval,
			whenEvals: whenEvals,
			elseEval:  elseEval,
			dataType:  e.Type,
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
	// First check if this is a correlated column reference
	if ctx != nil && ctx.CorrelatedValues != nil {
		// Try qualified name first
		var key string
		if e.tableAlias != "" {
			key = fmt.Sprintf("%s.%s", e.tableAlias, e.columnName)
		} else {
			key = e.columnName
		}

		if val, ok := ctx.CorrelatedValues[key]; ok {
			return val, nil
		}

		// Try unqualified name as fallback
		if val, ok := ctx.CorrelatedValues[e.columnName]; ok {
			return val, nil
		}
	}

	// Normal column resolution
	if !e.resolved {
		return types.NewNullValue(), fmt.Errorf("column %s not resolved", e.columnName)
	}

	// If this is a correlated column (columnIdx = -1) but no correlation was found
	if e.columnIdx < 0 {
		return types.NewNullValue(), fmt.Errorf("correlated column %s not found in context", e.columnName)
	}

	if e.columnIdx >= len(row.Values) {
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
		// Check for date/interval arithmetic first
		if result, ok := e.evalDateArithmetic(leftVal, rightVal, true); ok {
			return result, nil
		}
		return e.evalArithmetic(leftVal, rightVal, e.createArithmeticOp(func(a, b int32) interface{} { return a + b },
			func(a, b int64) interface{} { return a + b },
			func(a, b float64) interface{} { return a + b }))

	case planner.OpSubtract:
		// Check for date/interval arithmetic first
		if result, ok := e.evalDateArithmetic(leftVal, rightVal, false); ok {
			return result, nil
		}
		return e.evalArithmetic(leftVal, rightVal, e.createArithmeticOp(func(a, b int32) interface{} { return a - b },
			func(a, b int64) interface{} { return a - b },
			func(a, b float64) interface{} { return a - b }))

	case planner.OpMultiply:
		// Check for interval multiplication (interval * scalar or scalar * interval)
		if result, ok := e.evalIntervalMultiply(leftVal, rightVal); ok {
			return result, nil
		}
		return e.evalArithmetic(leftVal, rightVal, e.createArithmeticOp(func(a, b int32) interface{} { return a * b },
			func(a, b int64) interface{} { return a * b },
			func(a, b float64) interface{} { return a * b }))

	case planner.OpDivide:
		return e.evalArithmetic(leftVal, rightVal, e.createDivisionOp())

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
		// LIKE pattern matching
		text, ok1 := leftVal.Data.(string)
		pattern, ok2 := rightVal.Data.(string)
		if !ok1 || !ok2 {
			return types.NewNullValue(), fmt.Errorf("LIKE requires string operands")
		}

		// Convert SQL LIKE pattern to Go regex pattern
		regexPattern := sqlLikeToRegex(pattern)
		matched, err := regexp.MatchString(regexPattern, text)
		if err != nil {
			return types.NewNullValue(), fmt.Errorf("invalid LIKE pattern: %w", err)
		}

		if e.operator == planner.OpNotLike {
			matched = !matched
		}
		return types.NewValue(matched), nil

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

// createArithmeticOp creates a generic arithmetic operation function that handles type conversions.
// It takes type-specific operation functions for int32, int64, and float64.
func (e *binaryOpEvaluator) createArithmeticOp(
	int32Op func(a, b int32) interface{},
	int64Op func(a, b int64) interface{},
	float64Op func(a, b float64) interface{},
) func(a, b interface{}) interface{} {
	return func(a, b interface{}) interface{} {
		switch a := a.(type) {
		case int32:
			switch b := b.(type) {
			case int32:
				return int32Op(a, b)
			case int64:
				return int64Op(int64(a), b)
			case float32:
				return float64Op(float64(a), float64(b))
			case float64:
				return float64Op(float64(a), b)
			}
		case int64:
			switch b := b.(type) {
			case int32:
				return int64Op(a, int64(b))
			case int64:
				return int64Op(a, b)
			case float32:
				return float64Op(float64(a), float64(b))
			case float64:
				return float64Op(float64(a), b)
			}
		case float32:
			switch b := b.(type) {
			case int32:
				return float64Op(float64(a), float64(b))
			case int64:
				return float64Op(float64(a), float64(b))
			case float32:
				return float64Op(float64(a), float64(b))
			case float64:
				return float64Op(float64(a), b)
			}
		case float64:
			switch b := b.(type) {
			case int32:
				return float64Op(a, float64(b))
			case int64:
				return float64Op(a, float64(b))
			case float32:
				return float64Op(a, float64(b))
			case float64:
				return float64Op(a, b)
			}
		}
		return nil
	}
}

// createDivisionOp creates a division operation function that handles type conversions and division by zero.
// Division always promotes to float64 for consistency with SQL standards.
func (e *binaryOpEvaluator) createDivisionOp() func(a, b interface{}) interface{} {
	return func(a, b interface{}) interface{} {
		// Convert both operands to float64
		var dividend, divisor float64
		var ok bool

		switch v := a.(type) {
		case int32:
			dividend = float64(v)
			ok = true
		case int64:
			dividend = float64(v)
			ok = true
		case float32:
			dividend = float64(v)
			ok = true
		case float64:
			dividend = v
			ok = true
		}
		if !ok {
			return nil
		}

		switch v := b.(type) {
		case int32:
			divisor = float64(v)
			ok = true
		case int64:
			divisor = float64(v)
			ok = true
		case float32:
			divisor = float64(v)
			ok = true
		case float64:
			divisor = v
			ok = true
		}
		if !ok {
			return nil
		}

		// Check for division by zero
		if divisor == 0 {
			return nil // Division by zero returns NULL per SQL standard
		}

		return dividend / divisor
	}
}

// evalIntervalMultiply handles interval multiplication operations.
// Returns (result, true) if this is an interval multiplication, (nil, false) otherwise.
func (e *binaryOpEvaluator) evalIntervalMultiply(left, right types.Value) (types.Value, bool) {
	// Helper function to extract numeric factor
	extractFactor := func(val types.Value) (float64, bool) {
		switch v := val.Data.(type) {
		case int32:
			return float64(v), true
		case int64:
			return float64(v), true
		case float32:
			return float64(v), true
		case float64:
			return v, true
		default:
			return 0, false
		}
	}

	// Check for interval * scalar
	if interval, ok := left.Data.(types.Interval); ok {
		if factor, ok := extractFactor(right); ok {
			return types.NewValue(interval.Multiply(factor)), true
		}
		// Return error wrapped in Value to maintain interface consistency
		return types.NewNullValue(), true
	}

	// Check for scalar * interval (commutative)
	if interval, ok := right.Data.(types.Interval); ok {
		if factor, ok := extractFactor(left); ok {
			return types.NewValue(interval.Multiply(factor)), true
		}
		// Return error wrapped in Value to maintain interface consistency
		return types.NewNullValue(), true
	}

	return types.Value{}, false
}

// evalDateArithmetic evaluates date/time arithmetic operations.
// Returns (result, true) if this is a date arithmetic operation, (nil, false) otherwise.
func (e *binaryOpEvaluator) evalDateArithmetic(left, right types.Value, isAdd bool) (types.Value, bool) {
	if left.IsNull() || right.IsNull() {
		return types.NewNullValue(), true
	}

	// Check if left is a date/timestamp (stored as time.Time)
	if leftTime, ok := left.Data.(time.Time); ok {
		// Date/Timestamp + Interval
		if interval, ok := right.Data.(types.Interval); ok {
			if isAdd {
				result := interval.AddToTime(leftTime)
				// Preserve the original type (Date vs Timestamp)
				if left.Type() == types.Date {
					return types.NewDateValue(result), true
				}
				return types.NewTimestampValue(result), true
			}
			// Date/Timestamp - Interval
			result := interval.SubtractFromTime(leftTime)
			// Preserve the original type (Date vs Timestamp)
			if left.Type() == types.Date {
				return types.NewDateValue(result), true
			}
			return types.NewTimestampValue(result), true
		}

		// Date/Timestamp - Date/Timestamp = Interval
		if rightTime, ok := right.Data.(time.Time); ok {
			if !isAdd {
				interval := types.TimeDifference(leftTime, rightTime)
				return types.NewIntervalValue(interval), true
			}
		}
	}

	// Interval + Date/Timestamp (commutative)
	if interval, ok := left.Data.(types.Interval); ok {
		if rightTime, ok := right.Data.(time.Time); ok {
			if isAdd {
				result := interval.AddToTime(rightTime)
				// Preserve the original type (Date vs Timestamp)
				if right.Type() == types.Date {
					return types.NewDateValue(result), true
				}
				return types.NewTimestampValue(result), true
			}
			// Interval - Date/Timestamp doesn't make sense
		}
	}

	// Interval + Interval
	if interval1, ok := left.Data.(types.Interval); ok {
		if interval2, ok := right.Data.(types.Interval); ok {
			if isAdd {
				return types.NewValue(interval1.Add(interval2)), true
			}
			return types.NewValue(interval1.Subtract(interval2)), true
		}
	}

	return types.Value{}, false
}

// evalComparison evaluates comparison operations.
func (e *binaryOpEvaluator) evalComparison(left, right types.Value, op func(int) bool) (types.Value, error) {
	// Handle NULL values
	if left.IsNull() || right.IsNull() {
		// NULL comparisons always return NULL (which is false in WHERE)
		return types.NewNullValue(), nil
	}

	// Simple comparison based on Go's comparable types
	// In a real implementation, we'd use the type system's Compare method
	cmp := 0

	// Handle date/time types first
	if _, ok := left.Data.(time.Time); ok {
		if _, ok := right.Data.(time.Time); ok {
			// Both are time.Time, use appropriate comparator based on type
			if left.Type() == types.Date && right.Type() == types.Date {
				cmp = types.Date.Compare(left, right)
			} else if left.Type() == types.Timestamp && right.Type() == types.Timestamp {
				cmp = types.Timestamp.Compare(left, right)
			} else {
				// Mixed date/timestamp comparison - compare as timestamps
				cmp = types.Timestamp.Compare(left, right)
			}
		} else {
			return types.NewNullValue(), fmt.Errorf("cannot compare time with %T", right.Data)
		}
	} else if _, ok := left.Data.(types.Interval); ok {
		if _, ok := right.Data.(types.Interval); ok {
			cmp = types.IntervalType.Compare(left, right)
		} else {
			return types.NewNullValue(), fmt.Errorf("cannot compare interval with %T", right.Data)
		}
	} else {
		// Handle numeric and other types
		switch l := left.Data.(type) {
		case int32:
			switch r := right.Data.(type) {
			case int32:
				if l < r {
					cmp = -1
				} else if l > r {
					cmp = 1
				}
			case int64:
				if int64(l) < r {
					cmp = -1
				} else if int64(l) > r {
					cmp = 1
				}
			case float64:
				if float64(l) < r {
					cmp = -1
				} else if float64(l) > r {
					cmp = 1
				}
			default:
				return types.NewNullValue(), fmt.Errorf("type mismatch in comparison")
			}

		case int64:
			switch r := right.Data.(type) {
			case int32:
				if l < int64(r) {
					cmp = -1
				} else if l > int64(r) {
					cmp = 1
				}
			case int64:
				if l < r {
					cmp = -1
				} else if l > r {
					cmp = 1
				}
			case float64:
				lf := float64(l)
				if lf < r {
					cmp = -1
				} else if lf > r {
					cmp = 1
				}
			default:
				return types.NewNullValue(), fmt.Errorf("type mismatch in comparison")
			}

		case float32:
			switch r := right.Data.(type) {
			case int32:
				if l < float32(r) {
					cmp = -1
				} else if l > float32(r) {
					cmp = 1
				}
			case int64:
				if l < float32(r) {
					cmp = -1
				} else if l > float32(r) {
					cmp = 1
				}
			case float32:
				if l < r {
					cmp = -1
				} else if l > r {
					cmp = 1
				}
			case float64:
				// Promote float32 to float64 for comparison
				if float64(l) < r {
					cmp = -1
				} else if float64(l) > r {
					cmp = 1
				}
			default:
				return types.NewNullValue(), fmt.Errorf("type mismatch in comparison")
			}

		case float64:
			switch r := right.Data.(type) {
			case int32:
				rf := float64(r)
				if l < rf {
					cmp = -1
				} else if l > rf {
					cmp = 1
				}
			case int64:
				rf := float64(r)
				if l < rf {
					cmp = -1
				} else if l > rf {
					cmp = 1
				}
			case float32:
				// Compare with float32 promoted to float64
				if l < float64(r) {
					cmp = -1
				} else if l > float64(r) {
					cmp = 1
				}
			case float64:
				if l < r {
					cmp = -1
				} else if l > r {
					cmp = 1
				}
			default:
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

		case []byte:
			if r, ok := right.Data.([]byte); ok {
				cmp = bytes.Compare(l, r)
			} else {
				return types.NewNullValue(), fmt.Errorf("type mismatch in comparison")
			}

		default:
			return types.NewNullValue(), fmt.Errorf("unsupported type for comparison: %T", l)
		}
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
		case int32:
			return types.NewValue(-v), nil
		case int64:
			return types.NewValue(-v), nil
		case float32:
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

// subqueryEvaluator evaluates scalar subqueries.
type subqueryEvaluator struct {
	subplan      planner.LogicalPlan
	subOperator  *SubqueryOperator
	dataType     types.DataType
	executor     *BasicExecutor
	isCorrelated bool
	lastRow      *Row // Track last row to detect when re-execution is needed
}

func (e *subqueryEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	if ctx == nil {
		return types.NewNullValue(), fmt.Errorf("execution context is nil")
	}

	// For correlated subqueries, we need to re-execute for each outer row
	needsReExecution := e.isCorrelated && (e.lastRow == nil || !e.rowsEqual(e.lastRow, row))

	if needsReExecution && e.subOperator != nil {
		// Close the previous operator
		e.subOperator.Close()
		e.subOperator = nil
	}

	// Create a new context for the subquery with correlated values
	subCtx := *ctx // Copy context
	if subCtx.CorrelatedValues == nil {
		subCtx.CorrelatedValues = make(map[string]types.Value)
	}

	// If we have a correlation schema and a current row, bind the values
	if ctx.CorrelationSchema != nil && row != nil {
		for i, col := range ctx.CorrelationSchema.Columns {
			if i < len(row.Values) {
				// Create qualified column names for correlation
				if col.TableAlias != "" {
					key := fmt.Sprintf("%s.%s", col.TableAlias, col.Name)
					subCtx.CorrelatedValues[key] = row.Values[i]
				}
				if col.TableName != "" {
					key := fmt.Sprintf("%s.%s", col.TableName, col.Name)
					subCtx.CorrelatedValues[key] = row.Values[i]
				}
				// Also store unqualified name for simpler references
				subCtx.CorrelatedValues[col.Name] = row.Values[i]
			}
		}
	}

	// Build the subquery operator lazily or when re-execution is needed
	if e.subOperator == nil {
		if e.executor == nil {
			return types.NewNullValue(), fmt.Errorf("executor not available for subquery evaluation")
		}

		// Build the physical operator from the logical plan with the new context
		physicalOp, err := e.executor.buildOperator(e.subplan, &subCtx)
		if err != nil {
			return types.NewNullValue(), fmt.Errorf("failed to build subquery operator: %w", err)
		}

		// Wrap in a SubqueryOperator (true for scalar subquery)
		e.subOperator = NewSubqueryOperator(physicalOp, true)

		// Open the subquery operator
		err = e.subOperator.Open(&subCtx)
		if err != nil {
			return types.NewNullValue(), err
		}
	}

	// Get the scalar result
	result, err := e.subOperator.GetScalarResult()
	if err != nil {
		return types.NewNullValue(), err
	}

	// Update lastRow for correlation tracking
	if e.isCorrelated && row != nil {
		e.lastRow = &Row{Values: make([]types.Value, len(row.Values))}
		copy(e.lastRow.Values, row.Values)
	}

	if result == nil {
		return types.NewNullValue(), nil
	}

	return *result, nil
}

// rowsEqual checks if two rows have equal values.
func (e *subqueryEvaluator) rowsEqual(row1, row2 *Row) bool {
	if row1 == nil || row2 == nil {
		return row1 == row2
	}
	if len(row1.Values) != len(row2.Values) {
		return false
	}
	for i := range row1.Values {
		if !row1.Values[i].Equal(row2.Values[i]) {
			return false
		}
	}
	return true
}

// existsEvaluator evaluates EXISTS expressions.
type existsEvaluator struct {
	subqueryEval ExprEvaluator
	not          bool
}

func (e *existsEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// For EXISTS, we need to check if the subquery returns any rows
	// The subqueryEval should be a subqueryEvaluator
	subEval, ok := e.subqueryEval.(*subqueryEvaluator)
	if !ok {
		return types.NewNullValue(), fmt.Errorf("expected subqueryEvaluator for EXISTS, got %T", e.subqueryEval)
	}

	// Build and execute the subquery to check for existence
	// For EXISTS, we need to build the operator differently than scalar subqueries
	if subEval.subOperator == nil {
		// Create a new context for the subquery with correlated values
		subCtx := *ctx // Copy context
		if subCtx.CorrelatedValues == nil {
			subCtx.CorrelatedValues = make(map[string]types.Value)
		}

		// If we have a correlation schema and a current row, bind the values
		// For EXISTS, we should always bind values if available since the subquery
		// might reference outer columns even if not marked as correlated
		if ctx.CorrelationSchema != nil && row != nil {
			for i, col := range ctx.CorrelationSchema.Columns {
				if i < len(row.Values) {
					// Create qualified column names for correlation
					if col.TableAlias != "" {
						key := fmt.Sprintf("%s.%s", col.TableAlias, col.Name)
						subCtx.CorrelatedValues[key] = row.Values[i]
					}
					if col.TableName != "" {
						key := fmt.Sprintf("%s.%s", col.TableName, col.Name)
						subCtx.CorrelatedValues[key] = row.Values[i]
					}
					// Also store unqualified name for simpler references
					subCtx.CorrelatedValues[col.Name] = row.Values[i]
				}
			}
		}

		// Build the physical operator from the logical plan
		if subEval.executor == nil {
			return types.NewNullValue(), fmt.Errorf("executor not available for EXISTS evaluation")
		}

		physicalOp, err := subEval.executor.buildOperator(subEval.subplan, &subCtx)
		if err != nil {
			return types.NewNullValue(), fmt.Errorf("failed to build EXISTS subquery operator: %w", err)
		}

		// Wrap in a SubqueryOperator (false for EXISTS - not scalar)
		subEval.subOperator = NewSubqueryOperator(physicalOp, false)

		// Open the operator
		err = subEval.subOperator.Open(&subCtx)
		if err != nil {
			return types.NewNullValue(), err
		}
	}

	// Check if subquery has any results
	hasResults, err := subEval.subOperator.HasResults()
	if err != nil {
		return types.NewNullValue(), err
	}

	// We need to close and reset for next evaluation
	// For EXISTS, we must rebuild the operator for each outer row
	// to ensure we get fresh results
	subEval.subOperator.Close()
	subEval.subOperator = nil

	result := hasResults
	if e.not {
		result = !result
	}

	return types.NewValue(result), nil
}

// inSubqueryEvaluator evaluates IN expressions with subqueries.
type inSubqueryEvaluator struct {
	exprEval     ExprEvaluator
	subqueryEval ExprEvaluator
	not          bool
}

func (e *inSubqueryEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Evaluate the left expression
	leftVal, err := e.exprEval.Eval(row, ctx)
	if err != nil {
		return types.NewNullValue(), err
	}

	if leftVal.IsNull() {
		return types.NewNullValue(), nil
	}

	// The subqueryEval should be a subqueryEvaluator
	subEval, ok := e.subqueryEval.(*subqueryEvaluator)
	if !ok {
		return types.NewNullValue(), fmt.Errorf("expected subqueryEvaluator for IN")
	}

	// Build the subquery operator if not already built
	if subEval.subOperator == nil {
		// Force evaluation to build the operator
		_, err := subEval.Eval(row, ctx)
		if err != nil {
			return types.NewNullValue(), err
		}
	}

	// Open the subquery operator if not already open
	if subEval.subOperator != nil && !subEval.subOperator.isOpen {
		err := subEval.subOperator.Open(ctx)
		if err != nil {
			return types.NewNullValue(), err
		}
	}

	// Check if the subquery contains the value
	if subEval.subOperator == nil {
		return types.NewNullValue(), fmt.Errorf("subquery operator not initialized")
	}
	contains, err := subEval.subOperator.ContainsValue(leftVal)
	if err != nil {
		return types.NewNullValue(), err
	}

	result := contains
	if e.not {
		result = !result
	}

	return types.NewValue(result), nil
}

// inValuesEvaluator evaluates IN expressions with value lists.
type inValuesEvaluator struct {
	exprEval   ExprEvaluator
	valueEvals []ExprEvaluator
	not        bool
}

func (e *inValuesEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Evaluate the left expression
	leftVal, err := e.exprEval.Eval(row, ctx)
	if err != nil {
		return types.NewNullValue(), err
	}

	if leftVal.IsNull() {
		return types.NewNullValue(), nil
	}

	// Check against each value in the list
	for _, valueEval := range e.valueEvals {
		rightVal, err := valueEval.Eval(row, ctx)
		if err != nil {
			return types.NewNullValue(), err
		}

		if !rightVal.IsNull() && leftVal.Equal(rightVal) {
			result := true
			if e.not {
				result = false
			}
			return types.NewValue(result), nil
		}
	}

	// Not found in the list
	result := !e.not
	return types.NewValue(result), nil
}

// extractEvaluator evaluates EXTRACT expressions.
type extractEvaluator struct {
	field    string        // YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
	fromEval ExprEvaluator // The expression to extract from
}

func (e *extractEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Evaluate the FROM expression
	fromVal, err := e.fromEval.Eval(row, ctx)
	if err != nil {
		return types.NewNullValue(), err
	}

	if fromVal.IsNull() {
		return types.NewNullValue(), nil
	}

	// Extract from time.Time value
	timeVal, ok := fromVal.Data.(time.Time)
	if !ok {
		return types.NewNullValue(), fmt.Errorf("EXTRACT requires date/timestamp value, got %T", fromVal.Data)
	}

	// Extract the requested field
	var result int32
	switch e.field {
	case "YEAR":
		result = int32(timeVal.Year()) // #nosec G115 - Year() returns int within int32 range
	case "MONTH":
		result = int32(timeVal.Month()) // #nosec G115 - Month() returns 1-12
	case "DAY":
		result = int32(timeVal.Day()) // #nosec G115 - Day() returns 1-31
	case "HOUR":
		result = int32(timeVal.Hour()) // #nosec G115 - Hour() returns 0-23
	case "MINUTE":
		result = int32(timeVal.Minute()) // #nosec G115 - Minute() returns 0-59
	case "SECOND":
		result = int32(timeVal.Second()) // #nosec G115 - Second() returns 0-59
	default:
		return types.NewNullValue(), fmt.Errorf("unsupported EXTRACT field: %s", e.field)
	}

	return types.NewValue(result), nil
}

// substringEvaluator evaluates SUBSTRING expressions.
type substringEvaluator struct {
	strEval    ExprEvaluator // The string to extract from
	startEval  ExprEvaluator // The start position (1-based)
	lengthEval ExprEvaluator // Optional length
}

func (e *substringEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Evaluate the string expression
	strVal, err := e.strEval.Eval(row, ctx)
	if err != nil {
		return types.NewNullValue(), err
	}

	if strVal.IsNull() {
		return types.NewNullValue(), nil
	}

	// Get the string value
	str, ok := strVal.Data.(string)
	if !ok {
		return types.NewNullValue(), fmt.Errorf("SUBSTRING requires string value, got %T", strVal.Data)
	}

	// Evaluate the start position
	startVal, err := e.startEval.Eval(row, ctx)
	if err != nil {
		return types.NewNullValue(), err
	}

	if startVal.IsNull() {
		return types.NewNullValue(), nil
	}

	// Get start position (1-based in SQL)
	var start int
	switch v := startVal.Data.(type) {
	case int32:
		start = int(v)
	case int64:
		start = int(v)
	default:
		return types.NewNullValue(), fmt.Errorf("SUBSTRING start position must be an integer, got %T", startVal.Data)
	}

	// Convert to 0-based indexing
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
	length := len(str) - start // Default to rest of string
	if e.lengthEval != nil {
		lengthVal, err := e.lengthEval.Eval(row, ctx)
		if err != nil {
			return types.NewNullValue(), err
		}

		if !lengthVal.IsNull() {
			switch v := lengthVal.Data.(type) {
			case int32:
				length = int(v)
			case int64:
				length = int(v)
			default:
				return types.NewNullValue(), fmt.Errorf("SUBSTRING length must be an integer, got %T", lengthVal.Data)
			}

			if length < 0 {
				return types.NewNullValue(), fmt.Errorf("SUBSTRING length cannot be negative")
			}
		}
	}

	// Extract substring
	end := start + length
	if end > len(str) {
		end = len(str)
	}

	return types.NewValue(str[start:end]), nil
}

// caseWhenEvaluator holds evaluators for a WHEN clause.
type caseWhenEvaluator struct {
	conditionEval ExprEvaluator
	resultEval    ExprEvaluator
}

// caseExprEvaluator evaluates CASE expressions.
type caseExprEvaluator struct {
	caseEval  ExprEvaluator       // nil for searched CASE
	whenEvals []caseWhenEvaluator // WHEN clauses
	elseEval  ExprEvaluator       // ELSE clause (may be nil)
	dataType  types.DataType
}

func (e *caseExprEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// For simple CASE, evaluate the main expression first
	var mainValue types.Value
	if e.caseEval != nil {
		var err error
		mainValue, err = e.caseEval.Eval(row, ctx)
		if err != nil {
			return types.NewNullValue(), fmt.Errorf("failed to evaluate CASE expression: %w", err)
		}

		if mainValue.IsNull() {
			// If the main expression is NULL, return NULL (or ELSE value)
			if e.elseEval != nil {
				return e.elseEval.Eval(row, ctx)
			}
			return types.NewNullValue(), nil
		}
	}

	// Evaluate WHEN clauses in order
	for _, when := range e.whenEvals {
		var matches bool

		if e.caseEval != nil {
			// Simple CASE: compare mainValue with when condition
			whenValue, err := when.conditionEval.Eval(row, ctx)
			if err != nil {
				return types.NewNullValue(), fmt.Errorf("failed to evaluate WHEN value: %w", err)
			}

			if !whenValue.IsNull() && mainValue.Equal(whenValue) {
				matches = true
			}
		} else {
			// Searched CASE: evaluate condition as boolean
			condValue, err := when.conditionEval.Eval(row, ctx)
			if err != nil {
				return types.NewNullValue(), fmt.Errorf("failed to evaluate WHEN condition: %w", err)
			}

			if !condValue.IsNull() {
				if condBool, ok := condValue.Data.(bool); ok {
					matches = condBool
				} else {
					return types.NewNullValue(), fmt.Errorf("WHEN condition must evaluate to boolean, got %T", condValue.Data)
				}
			}
		}

		if matches {
			// Return the result for this WHEN clause
			return when.resultEval.Eval(row, ctx)
		}
	}

	// No WHEN clause matched, return ELSE value or NULL
	if e.elseEval != nil {
		return e.elseEval.Eval(row, ctx)
	}

	return types.NewNullValue(), nil
}

// sqlLikeToRegex converts a SQL LIKE pattern to a regular expression pattern.
// SQL LIKE uses % for any sequence of characters and _ for a single character.
// Special regex characters are escaped.
func sqlLikeToRegex(pattern string) string {
	// First, escape all regex special characters except % and _
	escaped := regexp.QuoteMeta(pattern)

	// Replace escaped % and _ with their unescaped versions
	escaped = strings.ReplaceAll(escaped, `\%`, "%")
	escaped = strings.ReplaceAll(escaped, `\_`, "_")

	// Convert SQL wildcards to regex
	// % matches zero or more characters
	escaped = strings.ReplaceAll(escaped, "%", ".*")
	// _ matches exactly one character
	escaped = strings.ReplaceAll(escaped, "_", ".")

	// Anchor the pattern to match the entire string
	return "^" + escaped + "$"
}
