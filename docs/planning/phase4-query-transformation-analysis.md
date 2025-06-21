# Phase 4: Query Transformation Analysis

**Date**: December 21, 2024  
**Author**: Analysis Team

## Current State Analysis

### What Exists

#### 1. Basic Optimizer Framework
- **Location**: `internal/sql/planner/optimizer.go`
- **Status**: ✅ Implemented
- **Features**:
  - Rule-based optimization framework
  - Predicate pushdown (partially implemented)
  - Projection pushdown (stub only - NOT implemented)
  - Constant folding (stub only - NOT implemented)
  - Index selection (fully implemented with composite index support)
  - Index condition pushdown

#### 2. Predicate Handling
- **Predicate Pushdown**: Basic implementation exists
  - Can push filters through projections
  - TODO: Join predicate pushdown not implemented
  - TODO: Validation of column availability not implemented

#### 3. Expression Support
- **IN Predicate**: ✅ Parsed and executed
  - `InExpr` AST node exists
  - Basic IN list evaluation works
  - No subquery support in IN clause
- **EXISTS**: ❌ Not implemented
  - No token for EXISTS in lexer
  - No AST node for EXISTS predicate
  - No subquery support

#### 4. Subquery Support
- **Status**: ❌ Not implemented
  - No subquery parsing
  - No subquery AST nodes
  - No correlated/uncorrelated subquery handling
  - No subquery in FROM clause support

#### 5. CTE (Common Table Expression) Support
- **Status**: ❌ Not implemented
  - No WITH token in lexer
  - No CTE parsing
  - No CTE AST nodes
  - No recursive CTE support

#### 6. Join Support
- **Basic Joins**: ✅ Implemented (from Phase 2)
  - Hash join, nested loop join, merge join
  - Semi/anti joins for EXISTS transformation (executor level)
- **Join Optimization**: ✅ Implemented
  - Join reordering
  - Cost-based join selection

### What Needs to Be Implemented for Phase 4

#### 1. Complete Projection Pushdown
**Current**: Empty stub returning `false`
**Required**:
- Track required columns through plan tree
- Push projections below joins, filters, sorts
- Eliminate unnecessary columns early
- Handle column aliasing and renaming
- Integration with index-only scans

#### 2. Subquery Support Infrastructure
**Parser Changes**:
- Add subquery parsing in FROM clause
- Add subquery parsing in WHERE clause (IN, EXISTS)
- Add scalar subquery support in SELECT list
- Create AST nodes for subqueries

**Planner Changes**:
- Subquery plan generation
- Correlation detection
- Subquery type classification

#### 3. Subquery Decorrelation
**Techniques to Implement**:
- Magic sets transformation
- Lateral join conversion
- Aggregation pull-up
- EXISTS to semi-join transformation
- IN to semi-join transformation
- NOT EXISTS to anti-join transformation
- NOT IN to anti-join with NULL handling

#### 4. CTE Implementation
**Parser**:
- Add WITH token to lexer
- Parse WITH clause
- Support multiple CTEs
- Handle recursive CTEs

**Planner**:
- CTE materialization strategy
- CTE inlining for simple cases
- Recursive CTE execution plan
- CTE optimization (push predicates into CTEs)

#### 5. EXISTS/IN Transformation
**Parser**:
- Add EXISTS token and parsing
- Support NOT EXISTS
- Handle correlated EXISTS

**Optimizer Rules**:
- EXISTS → Semi-join transformation
- IN(subquery) → Semi-join transformation
- NOT EXISTS → Anti-join transformation
- NOT IN(subquery) → Anti-join with NULL handling
- Correlation removal where possible

## Implementation Priority

### High Priority (Week 1)
1. **Projection Pushdown** - Foundation for other optimizations
2. **Subquery Parser Support** - Required for everything else
3. **Basic Subquery Planning** - Generate plans for subqueries

### Medium Priority (Week 2)
1. **EXISTS/IN Transformations** - Major performance impact
2. **Simple Decorrelation** - Uncorrelated subqueries first
3. **Subquery Type Detection** - Classify subqueries

### Lower Priority (Week 3-4)
1. **CTE Support** - Less common but important
2. **Advanced Decorrelation** - Complex correlated queries
3. **CTE Optimization** - Predicate pushdown into CTEs

## Technical Design Notes

### Projection Pushdown Algorithm
```
1. Start from root, collect required columns
2. At each node:
   - Determine columns needed by this node
   - Determine columns needed by parent
   - Union = required columns to fetch from children
3. Push projection below node if beneficial
4. Special cases:
   - Joins: track columns from each side
   - Aggregates: project group-by + aggregate columns
   - Sorts: include sort columns
```

### Subquery Classification
- **Scalar**: Returns single value
- **Exists**: Returns boolean
- **In-list**: Returns set for IN predicate
- **Derived table**: Subquery in FROM clause
- **Correlated**: References outer query columns
- **Uncorrelated**: Independent of outer query

### Decorrelation Strategies
1. **Uncorrelated → Join**
2. **Correlated EXISTS → Lateral Semi-join**
3. **Correlated Scalar → Lateral Join + Aggregate**
4. **Magic Sets** for complex correlations

## Success Metrics

1. **Projection Pushdown**:
   - Reduces data flow by 50%+ in typical queries
   - Enables index-only scans

2. **Subquery Performance**:
   - Correlated subqueries 10x+ faster
   - EXISTS queries use semi-joins

3. **CTE Performance**:
   - Simple CTEs inlined
   - Complex CTEs materialized once

4. **Test Coverage**:
   - All TPC-H queries with subqueries work
   - PostgreSQL compatibility for subquery behavior

## Risk Areas

1. **NULL Semantics**: NOT IN with NULLs is tricky
2. **Correlation Complexity**: Some patterns very hard to decorrelate
3. **Performance Regression**: Bad transformations can hurt
4. **PostgreSQL Compatibility**: Must match PG behavior exactly

## Recommended Approach

1. Start with projection pushdown (simpler, high impact)
2. Add basic subquery parsing
3. Implement EXISTS → semi-join (already have semi-join operator)
4. Build up decorrelation rules incrementally
5. Add CTE support last (most complex)
6. Extensive testing at each step

## References

- PostgreSQL subquery processing: `src/backend/optimizer/plan/subselect.c`
- "Unnesting Arbitrary Queries" - Neumann & Kemper
- SQL Server decorrelation: "Orthogonal Optimization of Subqueries and Aggregation"