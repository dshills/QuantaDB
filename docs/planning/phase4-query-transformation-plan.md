# Phase 4: Query Transformation Enhancements

## Overview

Phase 4 focuses on advanced query transformation optimizations that improve query performance by rewriting queries into more efficient forms. This phase builds on the semi/anti join operators implemented in Phase 2 and the advanced index features from Phase 3.

**Duration**: 1 week (5 working days)  
**Start Date**: December 23, 2024  
**Dependencies**: Phase 2 (semi/anti joins) and Phase 3 (index optimizations) complete

## Goals

1. **Complete Projection Pushdown**: Reduce data flow by eliminating unnecessary columns early
2. **Subquery Support**: Add parser and planner support for subqueries in all contexts
3. **Subquery Decorrelation**: Transform correlated subqueries into efficient joins
4. **CTE Implementation**: Support Common Table Expressions with optimization
5. **EXISTS/IN Transformations**: Convert to semi/anti joins for better performance

## Implementation Tasks

### Task 4.1: Complete Projection Pushdown (1 day)

**Goal**: Minimize data movement by pushing projections as close to table scans as possible.

**Implementation Steps**:
1. Column tracking through plan tree
   - Create `ColumnSet` structure to track required columns
   - Add `RequiredColumns()` method to all plan nodes
   - Propagate requirements bottom-up through the tree

2. Projection insertion algorithm
   - Insert projections below joins when columns can be eliminated
   - Push through filters (easy - filters don't add columns)
   - Handle sort columns specially (needed until sort completes)

3. Schema management
   - Update schema at each projection point
   - Maintain column mappings for expression rewriting
   - Handle star projections and aliases

4. Integration with existing optimizations
   - Work with index-only scans (already implemented)
   - Coordinate with predicate pushdown
   - Update cost model for reduced data flow

**Files to modify**:
- `internal/sql/planner/optimizer.go` - Complete ProjectionPushdown rule
- `internal/sql/planner/plan.go` - Add RequiredColumns interface
- `internal/sql/planner/column_tracking.go` (new) - Column set utilities
- `internal/sql/planner/projection_pushdown_test.go` (new)

**Testing**:
- Unit tests for column tracking
- Integration tests with joins and aggregates
- Performance benchmarks showing I/O reduction

### Task 4.2: Subquery Parser Support (1 day)

**Goal**: Add comprehensive subquery support to the SQL parser.

**Implementation Steps**:
1. Lexer enhancements
   - Add EXISTS token
   - Ensure proper handling of nested parentheses

2. Parser grammar updates
   - Subqueries in FROM clause (derived tables)
   - Scalar subqueries in SELECT list
   - Subqueries in WHERE with EXISTS/IN/comparison operators
   - Correlated subquery support with outer references

3. AST node definitions
   ```go
   type SubqueryExpr struct {
       Query      *SelectStmt
       Correlated bool
       Type       SubqueryType // Scalar, Exists, In, Comparison
   }
   ```

4. Parser rule implementation
   - Handle operator precedence correctly
   - Support ANY/ALL modifiers
   - Parse correlation names and aliases

**Files to modify**:
- `internal/sql/parser/lexer.go` - Add EXISTS token
- `internal/sql/parser/parser.go` - Add subquery parsing rules
- `internal/sql/parser/ast.go` - Add SubqueryExpr node
- `internal/sql/parser/parser_test.go` - Comprehensive tests

**Testing**:
- Parse all standard subquery forms
- Nested subquery tests
- Error cases and edge conditions

### Task 4.3: Basic Subquery Planning (1 day)

**Goal**: Generate execution plans for subqueries and integrate with main query.

**Implementation Steps**:
1. Subquery plan generation
   - Create separate planner context for subqueries
   - Handle correlation by tracking outer references
   - Generate LogicalSubquery plan nodes

2. Subquery types and execution strategies
   - Scalar subqueries: execute once, cache result
   - EXISTS: transform to semi-join (later step)
   - IN: transform to semi-join or hash lookup
   - Correlated: identify for decorrelation

3. Basic execution support
   ```go
   type SubqueryOperator struct {
       Subplan  Plan
       Cache    *SubqueryCache
       IsScalar bool
   }
   ```

4. Integration with main plan
   - Wire subquery results into parent expressions
   - Handle NULL propagation correctly
   - Error on multi-row results for scalar context

**Files to create/modify**:
- `internal/sql/planner/subquery.go` (new) - Subquery planning
- `internal/sql/planner/logical_plan.go` - Add LogicalSubquery
- `internal/sql/executor/subquery.go` (new) - Subquery operator
- `internal/sql/planner/subquery_test.go` (new)

**Testing**:
- Simple scalar subqueries
- Uncorrelated EXISTS/IN
- Multi-level nesting

### Task 4.4: Subquery Decorrelation and Transformations (1 day)

**Goal**: Transform subqueries into efficient join operations.

**Implementation Steps**:
1. EXISTS/NOT EXISTS transformation
   ```sql
   -- Transform:
   SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.x)
   -- Into:
   SELECT * FROM t1 SEMI JOIN t2 ON t1.x = t2.x
   ```

2. IN/NOT IN transformation
   ```sql
   -- Transform:
   SELECT * FROM t1 WHERE t1.x IN (SELECT t2.x FROM t2)
   -- Into:
   SELECT * FROM t1 SEMI JOIN t2 ON t1.x = t2.x
   ```

3. Correlated subquery decorrelation
   - Apply magic sets transformation
   - Or use lateral join approach
   - Handle aggregates in correlated subqueries

4. Transformation rules
   ```go
   type SubqueryElimination struct{}
   
   func (s *SubqueryElimination) Apply(plan LogicalPlan) (LogicalPlan, bool) {
       // Detect subquery patterns
       // Transform to equivalent joins
       // Preserve NULL semantics
   }
   ```

**Files to create/modify**:
- `internal/sql/planner/subquery_elimination.go` (new)
- `internal/sql/planner/decorrelation.go` (new)
- `internal/sql/planner/optimizer.go` - Add new rules
- Tests for each transformation

**Testing**:
- EXISTS/IN with various correlations
- NOT EXISTS/NOT IN with NULL handling
- Complex nested correlations
- Performance comparisons

### Task 4.5: CTE Implementation (1 day)

**Goal**: Support Common Table Expressions with materialization and inlining.

**Implementation Steps**:
1. Parser support
   ```sql
   WITH cte1 AS (SELECT ...), 
        cte2 AS (SELECT ... FROM cte1)
   SELECT * FROM cte2
   ```

2. CTE planning
   - Build CTE dependency graph
   - Detect recursive CTEs (initial support)
   - Choose materialization vs inlining

3. CTE execution strategies
   ```go
   type CTEOperator struct {
       Name         string
       Materialized bool
       Definition   Plan
       Cache        *CTECache
   }
   ```

4. Optimization opportunities
   - Predicate pushdown into CTEs
   - Column pruning for CTEs
   - CTE inlining for single references

**Files to create/modify**:
- `internal/sql/parser/parser.go` - WITH clause parsing
- `internal/sql/planner/cte.go` (new) - CTE planning
- `internal/sql/executor/cte.go` (new) - CTE operator
- `internal/sql/planner/cte_optimizer.go` (new)

**Testing**:
- Simple CTEs
- Multiple CTEs with dependencies
- Recursive CTEs (basic)
- Performance tests

## Testing & Validation Plan

### Unit Tests
- Each transformation rule in isolation
- Parser tests for all new syntax
- Planner tests for each query pattern

### Integration Tests
- End-to-end query execution
- Complex queries mixing features
- TPC-H queries 3, 5, 8, 10 (mentioned in TODO)

### Performance Validation
```sql
-- Before: Correlated subquery
SELECT c_name FROM customer c
WHERE EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.c_id = c.c_id AND o.total > 1000
)

-- After: Semi-join (should be much faster)
SELECT c_name FROM customer c
SEMI JOIN orders o ON c.c_id = o.c_id
WHERE o.total > 1000
```

### Test Queries
1. **Projection Pushdown**
   ```sql
   SELECT t1.a FROM t1 JOIN t2 ON t1.id = t2.id WHERE t2.x > 100
   -- Should push projection of only needed columns
   ```

2. **Subquery Decorrelation**
   ```sql
   SELECT * FROM orders o 
   WHERE total > (SELECT AVG(total) FROM orders o2 WHERE o2.region = o.region)
   -- Transform to window function or lateral join
   ```

3. **CTE Optimization**
   ```sql
   WITH regional_sales AS (
       SELECT region, SUM(total) as total FROM orders GROUP BY region
   )
   SELECT * FROM regional_sales WHERE total > 10000
   -- Should push WHERE into CTE
   ```

## Success Metrics

1. **Projection Pushdown**
   - 30-50% reduction in data movement for wide tables
   - Measurable reduction in memory usage

2. **Subquery Transformation**
   - 10-100x speedup for EXISTS/IN queries
   - Elimination of N+1 query patterns

3. **Overall Impact**
   - TPC-H Q3, Q5, Q8, Q10 within 20% of PostgreSQL
   - No performance regressions on simple queries

## Implementation Order

1. **Day 1**: Projection Pushdown - High impact, relatively simple
2. **Day 2**: Subquery Parser - Foundation for remaining work  
3. **Day 3**: Basic Subquery Planning - Get subqueries working
4. **Day 4**: Decorrelation - Transform to efficient plans
5. **Day 5**: CTEs - Advanced feature building on subqueries

## Risks and Mitigations

1. **Risk**: NULL semantics in NOT IN
   - **Mitigation**: Careful testing, follow PostgreSQL behavior

2. **Risk**: Infinite recursion in recursive CTEs
   - **Mitigation**: Add iteration limits, detect cycles

3. **Risk**: Wrong results from incorrect decorrelation
   - **Mitigation**: Extensive testing, conservative transformations

4. **Risk**: Performance regression from bad transformations
   - **Mitigation**: Cost-based decisions, transformation flags

## Dependencies

- Phase 2 semi/anti join operators (✅ Complete)
- Phase 3 index optimizations (✅ Complete)
- Existing optimizer framework (✅ Available)

## Notes

- Start with projection pushdown for immediate impact
- Subquery work enables future window function support
- CTE implementation prepares for recursive queries
- Focus on correctness over performance initially
- PostgreSQL compatibility is the guide for semantics

---
*Created: December 21, 2024*  
*Estimated Duration: 5 days*  
*Prerequisites: Phases 1-3 complete*