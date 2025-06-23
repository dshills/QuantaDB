# Plan: Support Aggregate Expressions in Projection

## Problem
Queries with aggregate expressions in projections fail with "unsupported expression type: *planner.AggregateExpr". This affects queries like:
- `SELECT SUM(a)/SUM(b) FROM table`
- `SELECT COUNT(*) * 100.0 / SUM(amount) FROM table`
- TPC-H Q8 and other queries that compute ratios or perform arithmetic on aggregates

## Analysis
1. The planner's `convertExpression` function doesn't handle `AggregateExpr` type
2. Aggregate expressions need special handling because they're computed during aggregation phase
3. When aggregate expressions appear in projections (after GROUP BY), they should be treated as column references to the aggregation results

## Solution Design
1. **Handle AggregateExpr in convertExpression**:
   - When an AggregateExpr appears in projection context, convert it to a ColumnRef
   - The column should reference the computed aggregate value from the aggregation operator

2. **Track Aggregate Outputs**:
   - During aggregation planning, assign names/aliases to aggregate expressions
   - Make these available as "virtual columns" for projection

3. **Support Arithmetic on Aggregates**:
   - Allow BinaryOp expressions that reference aggregate results
   - Example: SUM(a)/SUM(b) becomes BinaryOp(ColumnRef("sum_a"), ColumnRef("sum_b"), OpDivide)

## Implementation Steps
1. Check if AggregateExpr type exists in planner
2. Add handling for aggregate expressions in projection context
3. Ensure aggregate results are accessible as columns
4. Test with various aggregate expression patterns
5. Verify TPC-H Q8 works

## Test Cases
- `SELECT SUM(a) FROM table` (simple aggregate)
- `SELECT SUM(a)/SUM(b) FROM table` (arithmetic on aggregates)
- `SELECT SUM(a) * 100.0 / COUNT(*) FROM table` (mixed arithmetic)
- `SELECT SUM(CASE WHEN condition THEN value END) / SUM(value) FROM table` (complex aggregates)