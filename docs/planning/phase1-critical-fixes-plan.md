# Phase 1: Critical Bug Fixes - Technical Implementation Plan

## Overview
This document provides detailed technical guidance for fixing the critical issues preventing TPC-H benchmark execution.

## Issue 1: GROUP BY Server Crash

### Symptoms
- Server crashes with SIGSEGV when executing GROUP BY queries
- Crash occurs in lib/pq driver at QueryContext
- Simple COUNT(*) works, but COUNT(*) with GROUP BY crashes

### Diagnosis Steps
1. **Enable detailed logging** in GroupByOperator
2. **Add nil checks** throughout the execution path
3. **Check schema propagation** from child operators
4. **Verify result object creation**

### Implementation Plan

#### 1.1 Debug GroupByOperator
```go
// internal/sql/executor/group_by.go

// Add comprehensive nil checks
func (g *GroupByOperator) Next() (Result, error) {
    // Add logging
    log.Debug("GroupByOperator.Next() called")
    
    // Check if operator is properly initialized
    if g.groups == nil {
        log.Error("GroupByOperator.groups is nil")
        return nil, errors.New("group by operator not properly initialized")
    }
    
    // Check child operator
    if g.child == nil {
        return nil, errors.New("group by operator has no child")
    }
    
    // Ensure schema is set
    if g.schema == nil {
        return nil, errors.New("group by operator has no schema")
    }
}
```

#### 1.2 Fix Result Handling
```go
// Ensure result object is properly created
func (g *GroupByOperator) buildResult() (Result, error) {
    // Create result with proper schema
    result := NewMaterializedResult(g.schema)
    
    // Add nil check before adding rows
    if result == nil {
        return nil, errors.New("failed to create result object")
    }
    
    // Process groups safely
    for _, group := range g.groups {
        if group == nil {
            continue
        }
        row := g.buildGroupRow(group)
        if row != nil {
            result.AddRow(row)
        }
    }
    
    return result, nil
}
```

#### 1.3 Add Connection Safety
```go
// internal/network/connection.go

// Add nil checks in sendResults
func (c *Connection) sendResults(result Result) error {
    // Already has nil checks, but ensure they're comprehensive
    if result == nil {
        c.logger.Debug("Result is nil, sending EmptyQueryResponse")
        return c.sendEmptyQueryResponse()
    }
    
    schema := result.Schema()
    if schema == nil {
        c.logger.Debug("Schema is nil, sending EmptyQueryResponse")
        return c.sendEmptyQueryResponse()
    }
    
    // Continue with normal processing
}
```

## Issue 2: JOIN Column Resolution

### Symptoms
- "column c_custkey not resolved" errors
- Occurs when JOINs are combined with GROUP BY or aggregation
- Column resolver not finding columns from joined tables

### Root Cause Analysis
The column resolver likely isn't handling qualified column names (table.column) properly in JOIN contexts, especially when combined with aggregation.

### Implementation Plan

#### 2.1 Fix Column Resolution in Planner
```go
// internal/sql/planner/resolve.go

type ColumnResolver struct {
    schemas     map[string]*executor.Schema  // table alias -> schema
    columnIndex map[string]int               // full column path -> index
}

func (r *ColumnResolver) ResolveColumn(table, column string) (*ColumnRef, error) {
    // Handle both qualified and unqualified names
    if table != "" {
        // Qualified name: table.column
        key := fmt.Sprintf("%s.%s", table, column)
        if idx, ok := r.columnIndex[key]; ok {
            return &ColumnRef{
                Table:  table,
                Column: column,
                Index:  idx,
            }, nil
        }
    }
    
    // Try unqualified name in all tables
    for tbl, schema := range r.schemas {
        for i, col := range schema.Columns {
            if col.Name == column {
                return &ColumnRef{
                    Table:  tbl,
                    Column: column,
                    Index:  i,
                }, nil
            }
        }
    }
    
    return nil, fmt.Errorf("column %s not found", column)
}
```

#### 2.2 Update JOIN Planning
```go
// internal/sql/planner/join.go

func (p *Planner) buildJoin(node *parser.JoinClause) (*LogicalJoin, error) {
    // Build left and right children
    left, err := p.buildFrom(node.Left)
    if err != nil {
        return nil, err
    }
    
    right, err := p.buildFrom(node.Right)
    if err != nil {
        return nil, err
    }
    
    // Create merged schema for column resolution
    mergedSchema := mergeSchemas(left.Schema(), right.Schema())
    
    // Update column resolver with both schemas
    p.resolver.AddSchema(leftAlias, left.Schema())
    p.resolver.AddSchema(rightAlias, right.Schema())
    
    // Now resolve join condition with full context
    condition, err := p.buildExpression(node.On)
    if err != nil {
        return nil, fmt.Errorf("failed to build join condition: %w", err)
    }
    
    return &LogicalJoin{
        Left:      left,
        Right:     right,
        Type:      node.Type,
        Condition: condition,
        Schema:    mergedSchema,
    }, nil
}
```

## Issue 3: Aggregate Expressions in Projection

### Symptoms
- "unsupported expression type: *planner.AggregateExpr" 
- Cannot use expressions like SUM(a)/SUM(b) in SELECT
- TPC-H Q8 fails because of this

### Implementation Plan

#### 3.1 Extend Projection Builder
```go
// internal/sql/executor/builder.go

func (b *Builder) buildProjection(expr planner.Expression) (projectionFunc, error) {
    switch e := expr.(type) {
    case *planner.AggregateExpr:
        // Handle aggregate expressions in projection
        return b.buildAggregateProjection(e)
    case *planner.BinaryExpr:
        // Handle binary expressions that may contain aggregates
        return b.buildBinaryProjection(e)
    // ... existing cases
    }
}

func (b *Builder) buildAggregateProjection(expr *planner.AggregateExpr) (projectionFunc, error) {
    // Aggregates should already be computed by GroupByOperator
    // This just references the computed value
    aggIndex := b.findAggregateIndex(expr)
    if aggIndex < 0 {
        return nil, fmt.Errorf("aggregate not found in schema")
    }
    
    return func(row executor.Row) (executor.Value, error) {
        return row.Values[aggIndex], nil
    }, nil
}
```

#### 3.2 Support Complex Aggregate Expressions
```go
// Handle expressions like SUM(a)/SUM(b)
func (b *Builder) buildBinaryProjection(expr *planner.BinaryExpr) (projectionFunc, error) {
    leftProj, err := b.buildProjection(expr.Left)
    if err != nil {
        return nil, err
    }
    
    rightProj, err := b.buildProjection(expr.Right)
    if err != nil {
        return nil, err
    }
    
    return func(row executor.Row) (executor.Value, error) {
        left, err := leftProj(row)
        if err != nil {
            return executor.Value{}, err
        }
        
        right, err := rightProj(row)
        if err != nil {
            return executor.Value{}, err
        }
        
        // Handle division of aggregates
        if expr.Op == parser.TokenSlash {
            return divideValues(left, right)
        }
        
        // Handle other operators...
        return applyBinaryOp(expr.Op, left, right)
    }, nil
}
```

## Testing Strategy

### 1. Unit Tests for Each Fix
```go
// internal/sql/executor/group_by_test.go
func TestGroupByWithNilHandling(t *testing.T) {
    // Test GROUP BY doesn't crash with various inputs
}

func TestGroupByEmptyResult(t *testing.T) {
    // Test GROUP BY with no matching rows
}

// internal/sql/planner/resolve_test.go  
func TestJoinColumnResolution(t *testing.T) {
    // Test qualified and unqualified column names
}

func TestMultiTableColumnResolution(t *testing.T) {
    // Test resolution with multiple tables
}
```

### 2. Integration Tests
```sql
-- Test GROUP BY fix
SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment;
SELECT o_orderpriority, COUNT(*) FROM orders GROUP BY o_orderpriority;

-- Test JOIN column resolution
SELECT COUNT(*) FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey;
SELECT c.c_name, COUNT(*) FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey GROUP BY c.c_name;

-- Test aggregate expressions
SELECT SUM(l_quantity) / COUNT(*) as avg_quantity FROM lineitem;
SELECT l_returnflag, SUM(l_extendedprice) / SUM(l_quantity) FROM lineitem GROUP BY l_returnflag;
```

### 3. Regression Tests
- Add all failing queries to regression test suite
- Ensure fixes don't break existing functionality
- Run full test suite after each fix

## Implementation Order

1. **Fix GROUP BY crash first** (highest priority)
   - This blocks most other testing
   - Add extensive logging
   - Test with simple GROUP BY queries

2. **Fix JOIN column resolution** (second priority)  
   - Needed for most TPC-H queries
   - Test with various JOIN patterns

3. **Add aggregate expression support** (third priority)
   - Needed for complex TPC-H queries
   - Test with Q8 specifically

## Debugging Tips

### 1. Enable Verbose Logging
```bash
# Set log level to debug
export QUANTA_LOG_LEVEL=debug

# Or modify server startup
./quantadb --log-level=debug
```

### 2. Use GDB for Crash Analysis
```bash
# Run under GDB
gdb ./quantadb
(gdb) run
# When it crashes:
(gdb) bt
(gdb) frame <n>
(gdb) print <variable>
```

### 3. Add Trace Points
```go
// Add throughout the code
log.Debugf("GroupByOperator state: groups=%d, schema=%v", len(g.groups), g.schema)
log.Debugf("Processing row: %v", row)
```

## Success Criteria

### GROUP BY Fix Success
- Query `SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment` executes without crash
- Returns correct results
- No memory leaks or goroutine leaks

### JOIN Resolution Success  
- All TPC-H queries with JOINs parse correctly
- Column references resolve in JOIN conditions
- Qualified names (table.column) work properly

### Aggregate Expression Success
- Complex expressions like `SUM(a)/SUM(b)` work
- TPC-H Q8 executes successfully
- Results match expected values

## Next Steps After Phase 1

Once these critical fixes are complete:
1. Run full TPC-H benchmark suite again
2. Identify any new errors or crashes
3. Move to Phase 2 (DISTINCT, LIMIT, BYTEA)
4. Begin index integration work