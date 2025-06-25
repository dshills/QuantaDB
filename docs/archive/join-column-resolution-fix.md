# JOIN Column Resolution Fix

## Problem
JOIN queries with qualified column names (e.g., `table.column`) were failing with column resolution errors. The system couldn't properly resolve columns when table aliases were used in multi-table queries.

## Root Cause
The column resolution logic in the executor's expression evaluator was only matching columns by name, ignoring the table qualifier. This caused ambiguity errors or incorrect column resolution in JOIN queries.

## Solution
1. **Extended Column Structure**: Added `TableName` and `TableAlias` fields to both planner and executor Column structs to track table information throughout query processing.

2. **Enhanced Column Resolution**: Modified the column resolution logic in `expression.go` to:
   - First attempt to match by both table alias and column name when a table alias is provided
   - Fall back to column name only matching if no table alias is specified
   - Properly handle ambiguous column references

3. **Schema Preservation**: Updated the planner to preserve table alias information when building projection schemas, ensuring qualified column references maintain their table context.

## Files Modified
- `internal/sql/planner/plan.go`: Added TableName and TableAlias to Column struct
- `internal/sql/executor/executor.go`: Added TableName and TableAlias to Column struct
- `internal/sql/planner/planner.go`: Modified planTableRef to populate table information
- `internal/sql/executor/expression.go`: Enhanced column resolution logic
- `internal/sql/executor/join.go`: Ensure JOIN operators preserve column table information
- `internal/sql/planner/logical.go`: Fixed nil pointer in LogicalJoin.String()

## Testing
Created comprehensive test in `test/test_join_column_resolution.go` that verifies:
- Simple JOIN with qualified columns
- JOIN with WHERE clause using qualified names
- Multiple column SELECT with JOIN
- Complex WHERE with qualified columns

All tests pass successfully, confirming that JOIN column resolution now works correctly.