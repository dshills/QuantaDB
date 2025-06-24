# Cross Product Analysis

## Investigation Summary

The issue reported is that cross products with table aliases (e.g., `FROM nation n1, nation n2`) are producing duplicate results like `(5,5), (5,5), (6,6), (6,6)` instead of the correct cartesian product `(5,5), (5,6), (6,5), (6,6)`.

## Key Findings

### 1. Parser Level (✓ Correct)
- The parser correctly handles comma-separated tables in FROM clauses
- `parseTableExpression` in parser.go properly creates a CROSS JOIN for comma syntax
- Table aliases are correctly preserved in the AST

### 2. Planner Level (✓ Correct)
- `planTableExpression` correctly creates LogicalJoin nodes with CrossJoin type
- Table aliases are preserved in LogicalScan nodes
- The planner properly builds the join tree for multiple tables

### 3. Executor Level (✓ Mostly Correct)
- Fixed: Added alias support to StorageScanOperator
- Column resolution in expression evaluator handles table aliases correctly
- Independent iterators are created for each table scan

### 4. Join Execution (Potential Issue)
The NestedLoopJoinOperator appears to handle the reset correctly by closing and reopening the right child operator. However, the symptoms suggest that both scans might be returning the same subset of data.

## Hypothesis

The issue might be related to:
1. **MVCC visibility**: Both scans might be seeing a different snapshot of the data
2. **Iterator state**: The iterator might not be fully reset when reopened
3. **Buffer pool interaction**: Page caching might be causing unexpected behavior

## Fix Applied

Added table alias support to StorageScanOperator:
- Modified `NewStorageScanOperator` to accept and preserve table alias
- Updated schema columns to include TableAlias field
- Modified executor's `buildScanOperator` to pass alias from LogicalScan

## Next Steps

If the issue persists after the alias fix:
1. Add logging to track which rows each scan operator returns
2. Verify MVCC snapshot timestamps are consistent
3. Check if buffer pool page pinning affects iterator behavior
4. Test with a simple in-memory dataset to isolate storage issues

## Test Query

To verify the fix:
```sql
SELECT n1.n_nationkey, n2.n_nationkey 
FROM nation n1, nation n2
WHERE n1.n_nationkey IN (5, 6) 
  AND n2.n_nationkey IN (5, 6)
ORDER BY n1.n_nationkey, n2.n_nationkey;
```

Expected: 4 rows - (5,5), (5,6), (6,5), (6,6)