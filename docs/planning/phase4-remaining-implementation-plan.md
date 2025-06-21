# Phase 4 Remaining Implementation Plan

## Status: Tasks 4.1 & 4.2 Complete ✅

**Completed:**
- ✅ Task 4.1: Projection Pushdown Implementation
- ✅ Task 4.2: Subquery Parser Support

**Remaining:**
- 🔲 Task 4.3: Implement Basic Subquery Planning
- 🔲 Task 4.4: Implement Subquery Decorrelation  
- 🔲 Task 4.5: Implement CTE Support
- 🔲 Performance benchmarks for Phase 4

## Implementation Order & Timeline

### Task 4.3: Basic Subquery Planning (2-3 hours)
**Goal**: Enable basic subquery execution without decorrelation

**Implementation Steps:**
1. **Planner Expression Conversion** (30 min)
   - Add SubqueryExpr handling to `convertExpression()` in planner
   - Add ExistsExpr handling for EXISTS predicates
   - Update InExpr conversion to handle subqueries
   
2. **Subquery Plan Node** (45 min)
   - Create LogicalSubquery plan node
   - Add to plan node interfaces and visitors
   - Implement RequiredColumns for subqueries
   
3. **Basic Executor Support** (60 min)
   - Create SubqueryOperator for simple execution
   - Handle scalar subquery results (single value)
   - Add proper error handling for multiple rows
   
4. **Integration & Testing** (30 min)
   - Wire into expression evaluation
   - Add basic test cases
   - Verify simple subqueries work

**Files to Create/Modify:**
- `internal/sql/planner/expression.go` - Add subquery conversion
- `internal/sql/planner/plan.go` - Add LogicalSubquery node
- `internal/sql/executor/subquery.go` - Basic subquery execution
- `internal/sql/planner/subquery_planning_test.go` - Tests

### Task 4.4: Subquery Decorrelation (2-3 hours)
**Goal**: Transform EXISTS/IN subqueries to semi/anti joins

**Implementation Steps:**
1. **Decorrelation Analysis** (45 min)
   - Identify correlated vs uncorrelated subqueries
   - Extract correlation predicates
   - Check decorrelation feasibility
   
2. **EXISTS Transformation** (60 min)
   - EXISTS → SEMI JOIN transformation
   - NOT EXISTS → ANTI JOIN transformation
   - Handle correlation conditions properly
   
3. **IN Transformation** (45 min)
   - IN (subquery) → SEMI JOIN with equality
   - NOT IN (subquery) → ANTI JOIN with NULL handling
   - Optimize for index lookups when possible
   
4. **Integration with Optimizer** (30 min)
   - Add SubqueryDecorrelation optimization rule
   - Integrate with existing join optimizations
   - Update cost estimates for decorrelated plans

**Files to Create/Modify:**
- `internal/sql/planner/decorrelation.go` - Decorrelation logic
- `internal/sql/planner/optimizer.go` - Add decorrelation rule  
- `internal/sql/planner/decorrelation_test.go` - Tests

### Task 4.5: CTE Support (1-2 hours)
**Goal**: Support Common Table Expressions with basic optimization

**Implementation Steps:**
1. **Parser Enhancement** (30 min)
   - Add WITH token and parsing
   - Create CTE AST nodes
   - Handle recursive CTE syntax (basic)
   
2. **CTE Planning** (45 min)
   - Create LogicalCTE plan node
   - Handle CTE materialization decisions
   - Support CTE references in queries
   
3. **Basic Optimization** (30 min)
   - Inline simple CTEs when beneficial
   - Avoid multiple materialization when possible
   - Basic cost-based CTE decisions

**Files to Create/Modify:**
- `internal/sql/parser/parser.go` - Add WITH parsing
- `internal/sql/parser/ast.go` - Add CTE nodes
- `internal/sql/planner/cte.go` - CTE planning
- `internal/sql/planner/cte_test.go` - Tests

### Performance Benchmarks (1 hour)
**Goal**: Validate Phase 4 performance improvements

**Implementation Steps:**
1. **TPC-H Query Subset** (30 min)
   - Implement queries 3, 5, 8 that use subqueries
   - Add benchmark framework
   - Measure before/after optimization
   
2. **Subquery Performance Tests** (30 min)
   - EXISTS vs SEMI JOIN performance
   - IN vs SEMI JOIN performance  
   - Projection pushdown data flow reduction

**Files to Create:**
- `internal/sql/planner/phase4_benchmarks_test.go`
- `docs/planning/phase4-performance-results.md`

## Success Criteria

**Task 4.3 Success:**
- ✅ Simple scalar subqueries execute correctly
- ✅ EXISTS predicates work without decorrelation
- ✅ Basic IN subqueries function
- ✅ Error handling for invalid subquery results

**Task 4.4 Success:**
- ✅ EXISTS transforms to SEMI JOIN
- ✅ NOT EXISTS transforms to ANTI JOIN  
- ✅ IN subqueries transform to SEMI JOIN
- ✅ Performance improvement over naive subquery execution

**Task 4.5 Success:**
- ✅ Basic WITH clauses parse and execute
- ✅ Simple CTE inlining works
- ✅ Multiple CTE references handled

**Overall Phase 4 Success:**
- ✅ All subquery forms supported end-to-end
- ✅ Performance benchmarks show improvement
- ✅ Clean integration with existing optimizations
- ✅ Comprehensive test coverage

## Implementation Approach

1. **Start with basic functionality** - get simple cases working first
2. **Incremental complexity** - add features progressively  
3. **Test-driven development** - write tests as we implement
4. **Performance validation** - measure improvements continuously
5. **Clean integration** - maintain compatibility with existing features

This plan provides a systematic approach to completing Phase 4 with measurable progress and quality validation at each step.