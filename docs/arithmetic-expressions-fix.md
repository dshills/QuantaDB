# Arithmetic Expression Type Fix

## Issue
Arithmetic operators (+, -, *, /) were incorrectly returning boolean values instead of numeric results. For example, "SELECT 1 + 1" was returning false instead of 2.

## Root Cause
In the planner's `convertExpression` function (`internal/sql/planner/planner.go`), when converting a `parser.BinaryExpr` to a planner expression, the result type was hardcoded to `types.Boolean` regardless of the operator type.

## Fix Applied
Modified the `convertExpression` function to determine the correct result type based on the operator:

1. **Arithmetic operators** (OpAdd, OpSubtract, OpMultiply, OpDivide, OpModulo): Return numeric types
   - Uses the left operand's type, or the right operand's type if left is unknown
   - Defaults to BigInt if both types are unknown
   - Division always returns Float type for SQL compatibility

2. **Logical operators** (OpAnd, OpOr): Return Boolean type

3. **String operators** (OpConcat): Return Text type

4. **Comparison operators**: Default to Boolean type

Also fixed unary expressions:
- OpNot returns Boolean
- OpNegate returns the same type as the operand (or BigInt for unknown types)

## Files Modified
- `internal/sql/planner/planner.go`: Fixed type determination in `convertExpression` for both binary and unary expressions

## Testing
All existing tests pass, confirming the fix doesn't break any functionality. The arithmetic operations now correctly return numeric values that can be used in calculations.