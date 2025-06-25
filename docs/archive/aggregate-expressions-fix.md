# Aggregate Expressions in Projection Fix

## Problem
Queries with aggregate expressions in projections were failing with "unsupported expression type: *planner.AggregateExpr". This affected queries like:
- `SELECT SUM(a)/SUM(b) FROM table`
- `SELECT COUNT(*) * 100.0 / SUM(amount) FROM table`
- TPC-H Q8 and other queries that compute ratios or perform arithmetic on aggregates

## Root Cause
The planner's `convertExpression` function didn't handle expressions containing aggregates in projection context. When aggregate functions appeared inside arithmetic expressions (e.g., `SUM(a)/SUM(b)`), they needed special handling to be properly referenced after the aggregation phase.

## Solution
Implemented an `AggregateRewriter` that:
1. **Extracts Aggregates**: Recursively traverses expressions to find all AggregateExpr nodes
2. **Replaces with Column References**: Replaces each aggregate with a ColumnRef that will be resolved after aggregation
3. **Maintains Consistency**: Ensures aggregate names match between planner and executor

Key components:
- `aggregate_rewriter.go`: New visitor pattern implementation that rewrites expressions
- Modified `buildSelectPlan` to use the rewriter when processing SELECT columns
- Ensured aggregate aliases are set correctly for proper column resolution

## Files Modified
- `internal/sql/planner/aggregate_rewriter.go`: New file implementing the rewriter
- `internal/sql/planner/planner.go`: Updated to use rewriter in buildSelectPlan
- Added `rewriteSelectColumns` and `buildProjectionSchema` helper functions

## Testing
Created comprehensive test in `test/test_aggregate_expressions.go` that verifies:
- Simple aggregates: `SELECT SUM(amount)`
- Multiple aggregates: `SELECT SUM(amount), SUM(quantity)`
- Aggregate arithmetic: `SELECT SUM(amount) / SUM(quantity)`
- Complex expressions: `SELECT SUM(amount) * 100.0 / SUM(quantity)`
- GROUP BY with aggregates: `SELECT product, SUM(amount) / SUM(quantity) as avg_price FROM sales GROUP BY product`

All tests pass successfully, confirming that aggregate expressions in projections now work correctly.

## Impact
This fix enables:
- TPC-H Q8 and similar queries that compute ratios
- Complex analytical queries with calculated metrics
- Better PostgreSQL compatibility for aggregate expressions