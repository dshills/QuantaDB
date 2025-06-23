# GROUP BY Server Crash Fix Plan

## Problem Description

The QuantaDB server crashes with a SIGSEGV (segmentation fault) when executing any query containing a GROUP BY clause. This is a critical blocker for TPC-H benchmarks and aggregate query testing.

### Symptoms
- Server crashes immediately upon executing GROUP BY queries
- SIGSEGV indicates memory access violation
- Affects all aggregate queries with grouping
- Prevents TPC-H benchmark execution

### Example Failing Query
```sql
SELECT product, SUM(amount) 
FROM sales 
GROUP BY product;
```

## Root Cause Analysis

Based on the symptoms, the likely causes are:
1. **Nil pointer dereference** in AggregateOperator
2. **Uninitialized data structures** (maps, slices)
3. **Schema mismatch** between operators
4. **Memory corruption** in aggregation logic

## Investigation Plan

### Step 1: Reproduce the Crash
1. Create minimal test case with GROUP BY
2. Run with debugging enabled
3. Capture stack trace and crash location

### Step 2: Debug with GDB/Delve
1. Set breakpoints in AggregateOperator
2. Step through initialization and execution
3. Identify exact line causing crash

### Step 3: Code Review Focus Areas
1. `internal/sql/executor/aggregate.go`
   - AggregateOperator.Open()
   - AggregateOperator.Next()
   - aggregateGroup initialization
2. `internal/sql/planner/planner.go`
   - buildAggregatePlan()
   - Schema construction
3. Memory initialization patterns

## Implementation Plan

### Phase 1: Immediate Fix
1. **Add nil checks** in AggregateOperator
2. **Initialize all maps/slices** properly
3. **Validate schema** before execution
4. **Add defensive programming** checks

### Phase 2: Comprehensive Testing
1. Test simple GROUP BY
2. Test GROUP BY with multiple columns
3. Test GROUP BY with HAVING
4. Test GROUP BY with aggregates
5. Test empty result sets

### Phase 3: Prevent Regression
1. Add unit tests for edge cases
2. Add integration tests
3. Document initialization requirements
4. Add logging for debugging

## Code Analysis Areas

### AggregateOperator Structure
```go
type AggregateOperator struct {
    baseOperator
    child       Operator
    groupBy     []ExprEvaluator
    aggregates  []AggregateExpr
    groups      map[uint64]*aggregateGroup
    groupIter   []*aggregateGroup
    iterIndex   int
    initialized bool
}
```

### Critical Methods
1. **NewAggregateOperator** - Constructor validation
2. **Open()** - Initialization logic
3. **Next()** - Iteration and result building
4. **computeGroupKey()** - Hash computation
5. **buildResult()** - Result row construction

## Success Criteria

1. No crashes on GROUP BY queries
2. Correct aggregation results
3. All TPC-H aggregate queries work
4. No performance regression
5. Comprehensive test coverage

## Risk Mitigation

1. Keep changes minimal and focused
2. Test each change incrementally
3. Maintain backward compatibility
4. Document all assumptions
5. Add extensive error handling

## Timeline

- **Hour 1**: Reproduce and debug crash
- **Hour 2**: Implement fix
- **Hour 3**: Test thoroughly
- **Hour 4**: Document and commit

## Notes

- GROUP BY was working in earlier versions
- Recent changes may have introduced the regression
- Check recent commits for changes to executor
- Consider memory management patterns