# Query Planner Design

## Overview

The query planner is responsible for transforming parsed SQL AST into efficient execution plans. It performs both logical and physical planning, optimizing queries for performance.

## Architecture

### Core Components

```
Parser AST → Logical Planner → Logical Plan → Optimizer → Physical Planner → Physical Plan → Executor
```

### Package Structure

```go
package planner

// Planner transforms AST into executable plans
type Planner interface {
    Plan(stmt parser.Statement) (Plan, error)
}

// Plan represents an executable query plan
type Plan interface {
    Schema() *Schema
    Children() []Plan
    Accept(visitor PlanVisitor) error
}
```

## Logical Plans

### Node Types

```go
// Logical plan nodes represent high-level operations
type LogicalPlan interface {
    Plan
    logicalNode()
}

// Core logical nodes
type LogicalScan struct {
    TableName string
    Alias     string
}

type LogicalFilter struct {
    Child     LogicalPlan
    Predicate Expression
}

type LogicalProject struct {
    Child       LogicalPlan
    Projections []Expression
}

type LogicalSort struct {
    Child   LogicalPlan
    OrderBy []SortKey
}

type LogicalLimit struct {
    Child  LogicalPlan
    Limit  int64
    Offset int64
}

type LogicalJoin struct {
    Left      LogicalPlan
    Right     LogicalPlan
    JoinType  JoinType
    Condition Expression
}
```

## Physical Plans

### Node Types

```go
// Physical plan nodes include execution details
type PhysicalPlan interface {
    Plan
    EstimatedCost() Cost
    EstimatedRows() int64
    physicalNode()
}

// Physical operators
type TableScan struct {
    Table     *catalog.Table
    Columns   []int // Column indices to scan
}

type IndexScan struct {
    Table     *catalog.Table
    Index     *catalog.Index
    Bounds    []Bound // Index bounds for range scan
}

type Filter struct {
    Child     PhysicalPlan
    Predicate Expression
}

type HashJoin struct {
    Left      PhysicalPlan
    Right     PhysicalPlan
    LeftKeys  []Expression
    RightKeys []Expression
}

type NestedLoopJoin struct {
    Left      PhysicalPlan
    Right     PhysicalPlan
    Condition Expression
}

type Sort struct {
    Child   PhysicalPlan
    OrderBy []SortKey
    Limit   *int64 // For top-k optimization
}
```

## Query Optimization

### Rule-Based Optimization

```go
// OptimizationRule transforms logical plans
type OptimizationRule interface {
    Apply(plan LogicalPlan) (LogicalPlan, bool)
}

// Common optimization rules
var DefaultRules = []OptimizationRule{
    &PredicatePushdown{},
    &ProjectionPushdown{},
    &ConstantFolding{},
    &DeadCodeElimination{},
    &JoinReordering{},
}
```

#### Predicate Pushdown

Push WHERE conditions closer to table scans:

```sql
-- Before optimization
SELECT * FROM (SELECT * FROM users) WHERE age > 25

-- After optimization  
SELECT * FROM users WHERE age > 25
```

#### Projection Pushdown

Only scan required columns:

```sql
-- Before optimization
SELECT name FROM (SELECT * FROM users)

-- After optimization
SELECT name FROM users
```

### Cost-Based Optimization

```go
// Cost model for plan selection
type Cost struct {
    StartupCost float64 // Cost before first row
    TotalCost   float64 // Total execution cost
    Rows        float64 // Estimated row count
    Width       int     // Average row width
}

// Statistics for cost estimation
type TableStats struct {
    RowCount     int64
    PageCount    int64
    ColumnStats  map[string]*ColumnStats
}

type ColumnStats struct {
    NullFraction   float64
    DistinctCount  int64
    MinValue       interface{}
    MaxValue       interface{}
    Histogram      []HistogramBucket
}
```

## Implementation Plan

### Phase 1: Basic Planner

```go
// Simple rule-based planner
type BasicPlanner struct {
    catalog *catalog.Catalog
}

func (p *BasicPlanner) Plan(stmt parser.Statement) (Plan, error) {
    // 1. Convert AST to logical plan
    logical := p.buildLogicalPlan(stmt)
    
    // 2. Apply optimization rules
    optimized := p.optimize(logical)
    
    // 3. Convert to physical plan
    physical := p.buildPhysicalPlan(optimized)
    
    return physical, nil
}
```

### Phase 2: Cost-Based Planning

```go
// Advanced planner with cost estimation
type CostBasedPlanner struct {
    catalog *catalog.Catalog
    stats   *stats.Manager
}

func (p *CostBasedPlanner) selectJoinOrder(tables []string) JoinTree {
    // Dynamic programming for join ordering
    // Consider all possible join orders
    // Select order with lowest cost
}

func (p *CostBasedPlanner) chooseScanMethod(table *catalog.Table, 
    predicate Expression) PhysicalPlan {
    // Compare sequential scan vs index scan costs
    // Consider selectivity of predicate
    // Choose method with lower cost
}
```

## Expression Framework

```go
// Expression evaluation
type Expression interface {
    Eval(row Row) (Value, error)
    DataType() DataType
    Accept(visitor ExprVisitor) error
}

// Common expressions
type ColumnRef struct {
    TableAlias string
    ColumnName string
    ColumnIdx  int
}

type Literal struct {
    Value Value
}

type BinaryOp struct {
    Left     Expression
    Right    Expression
    Operator OpType
}

type FunctionCall struct {
    Name string
    Args []Expression
}
```

## Example: SELECT Query Planning

```sql
SELECT u.name, COUNT(o.id) 
FROM users u 
JOIN orders o ON u.id = o.user_id 
WHERE u.age > 25 
GROUP BY u.name 
ORDER BY COUNT(o.id) DESC 
LIMIT 10
```

### Logical Plan

```
Limit(10)
  └── Sort(COUNT(o.id) DESC)
      └── Aggregate(GROUP BY u.name, COUNT(o.id))
          └── Join(u.id = o.user_id)
              ├── Filter(u.age > 25)
              │   └── Scan(users u)
              └── Scan(orders o)
```

### Optimized Logical Plan

```
Limit(10)
  └── Sort(COUNT(o.id) DESC)
      └── Aggregate(GROUP BY u.name, COUNT(o.id))
          └── Join(u.id = o.user_id)
              ├── Scan(users u, filter: age > 25)
              └── Scan(orders o)
```

### Physical Plan

```
Limit(10)
  └── TopKSort(COUNT(o.id) DESC, k=10)
      └── HashAggregate(GROUP BY u.name, COUNT(o.id))
          └── HashJoin(u.id = o.user_id)
              ├── IndexScan(users.age_idx, age > 25)
              └── TableScan(orders)
```

## Testing Strategy

### Unit Tests

```go
func TestPredicatePushdown(t *testing.T) {
    // Test that WHERE conditions are pushed down
    ast := parseSQL("SELECT * FROM (SELECT * FROM t) WHERE x = 1")
    logical := buildLogicalPlan(ast)
    optimized := applyRule(&PredicatePushdown{}, logical)
    
    // Verify filter is directly on scan
    assert.IsType(t, &LogicalFilter{}, optimized)
    filter := optimized.(*LogicalFilter)
    assert.IsType(t, &LogicalScan{}, filter.Child)
}
```

### Integration Tests

```go
func TestEndToEndPlanning(t *testing.T) {
    planner := NewPlanner(catalog)
    
    testCases := []struct {
        sql      string
        expected string // Expected plan structure
    }{
        {
            sql: "SELECT * FROM users WHERE id = 1",
            expected: "IndexScan(users.pk, id = 1)",
        },
        {
            sql: "SELECT * FROM users ORDER BY name",
            expected: "Sort(name) -> TableScan(users)",
        },
    }
    
    for _, tc := range testCases {
        plan := planner.Plan(parseSQL(tc.sql))
        assert.Equal(t, tc.expected, plan.String())
    }
}
```

## Performance Considerations

1. **Plan Caching**: Cache plans for prepared statements
2. **Statistics Updates**: Periodic table statistics updates
3. **Adaptive Planning**: Re-plan based on actual vs estimated rows
4. **Parallel Planning**: Plan subqueries in parallel

## Future Enhancements

1. **Distributed Planning**: Plan queries across multiple nodes
2. **Materialized Views**: Consider materialized views in planning
3. **Query Hints**: Support optimizer hints
4. **Plan Visualization**: Generate EXPLAIN output
5. **Machine Learning**: ML-based cost estimation