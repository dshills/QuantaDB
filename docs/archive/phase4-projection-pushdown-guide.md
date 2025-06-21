# Projection Pushdown Implementation Guide

## Overview

This guide provides detailed implementation steps for completing the projection pushdown optimization in QuantaDB. This is the first task of Phase 4 and has high impact on query performance.

## Current State

The `ProjectionPushdown` rule in `optimizer.go` currently just returns `false`. We need to implement the actual optimization logic.

## Implementation Design

### Core Concepts

1. **Required Columns**: Set of columns needed by parent operators
2. **Available Columns**: Set of columns provided by child operators  
3. **Projection Points**: Where to insert projections to eliminate columns

### Algorithm Overview

```
1. Traverse plan tree bottom-up collecting required columns
2. At each node, determine what columns children must provide
3. Insert projections where columns can be eliminated
4. Update schemas throughout the tree
```

## Step-by-Step Implementation

### Step 1: Create Column Tracking Infrastructure

Create `internal/sql/planner/column_tracking.go`:

```go
package planner

import (
    "github.com/dshills/QuantaDB/internal/catalog"
)

// ColumnSet represents a set of columns referenced in a query
type ColumnSet struct {
    columns map[ColumnRef]bool
}

// NewColumnSet creates an empty column set
func NewColumnSet() *ColumnSet {
    return &ColumnSet{
        columns: make(map[ColumnRef]bool),
    }
}

// Add adds a column to the set
func (cs *ColumnSet) Add(col ColumnRef) {
    cs.columns[col] = true
}

// AddAll adds all columns from another set
func (cs *ColumnSet) AddAll(other *ColumnSet) {
    for col := range other.columns {
        cs.columns[col] = true
    }
}

// Contains checks if a column is in the set
func (cs *ColumnSet) Contains(col ColumnRef) bool {
    return cs.columns[col]
}

// ToSlice returns columns as a slice
func (cs *ColumnSet) ToSlice() []ColumnRef {
    result := make([]ColumnRef, 0, len(cs.columns))
    for col := range cs.columns {
        result = append(result, col)
    }
    return result
}

// ColumnRef uniquely identifies a column
type ColumnRef struct {
    TableAlias string
    ColumnName string
}
```

### Step 2: Add Required Columns Analysis

Add to `internal/sql/planner/plan.go`:

```go
// RequiredColumnsAnalyzer computes required columns for plan nodes
type RequiredColumnsAnalyzer interface {
    // RequiredColumns returns columns this node needs from its children
    RequiredColumns() *ColumnSet
    
    // RequiredInputColumns returns columns needed from specific child
    RequiredInputColumns(childIndex int) *ColumnSet
}
```

Implement for each logical plan node:

```go
// LogicalProject
func (p *LogicalProject) RequiredColumns() *ColumnSet {
    cols := NewColumnSet()
    for _, expr := range p.Projections {
        extractColumns(expr, cols)
    }
    return cols
}

// LogicalFilter  
func (f *LogicalFilter) RequiredColumns() *ColumnSet {
    cols := NewColumnSet()
    extractColumns(f.Predicate, cols)
    // Also need all columns required by parent
    if parent := f.parent; parent != nil {
        cols.AddAll(parent.RequiredColumns())
    }
    return cols
}

// LogicalJoin
func (j *LogicalJoin) RequiredInputColumns(childIndex int) *ColumnSet {
    cols := NewColumnSet()
    
    // Extract columns from join condition
    extractColumns(j.Condition, cols)
    
    // Add columns required by parent
    parentCols := j.RequiredColumns()
    
    // Filter to only columns from the specified child
    filtered := NewColumnSet()
    schema := j.Children()[childIndex].Schema()
    for _, col := range parentCols.ToSlice() {
        if columnInSchema(col, schema) {
            filtered.Add(col)
        }
    }
    
    return filtered
}
```

### Step 3: Implement Column Extraction

```go
// extractColumns finds all column references in an expression
func extractColumns(expr Expression, cols *ColumnSet) {
    switch e := expr.(type) {
    case *ColumnRef:
        cols.Add(ColumnRef{
            TableAlias: e.TableAlias,
            ColumnName: e.ColumnName,
        })
    case *BinaryOp:
        extractColumns(e.Left, cols)
        extractColumns(e.Right, cols)
    case *UnaryOp:
        extractColumns(e.Expr, cols)
    case *FunctionCall:
        for _, arg := range e.Args {
            extractColumns(arg, cols)
        }
    case *Star:
        // Star requires all columns - mark specially
        cols.Add(ColumnRef{ColumnName: "*"})
    // ... other expression types
    }
}
```

### Step 4: Implement Projection Pushdown Rule

Update `ProjectionPushdown` in `optimizer.go`:

```go
type ProjectionPushdown struct{}

func (p *ProjectionPushdown) Apply(plan LogicalPlan) (LogicalPlan, bool) {
    // Phase 1: Analyze required columns bottom-up
    requiredCols := p.analyzeRequiredColumns(plan)
    
    // Phase 2: Insert projections where beneficial
    newPlan, modified := p.insertProjections(plan, requiredCols)
    
    return newPlan, modified
}

func (p *ProjectionPushdown) analyzeRequiredColumns(plan LogicalPlan) map[LogicalPlan]*ColumnSet {
    required := make(map[LogicalPlan]*ColumnSet)
    
    // Post-order traversal
    var analyze func(node LogicalPlan, parentReqs *ColumnSet)
    analyze = func(node LogicalPlan, parentReqs *ColumnSet) {
        // First process children
        if req, ok := node.(RequiredColumnsAnalyzer); ok {
            for i, child := range node.Children() {
                childReqs := req.RequiredInputColumns(i)
                analyze(child.(LogicalPlan), childReqs)
            }
        }
        
        // Then process this node
        required[node] = parentReqs
    }
    
    // Start with all columns required at root
    rootReqs := NewColumnSet()
    if proj, ok := plan.(*LogicalProject); ok {
        rootReqs = proj.RequiredColumns()
    } else {
        // If no projection, need all columns
        rootReqs.Add(ColumnRef{ColumnName: "*"})
    }
    
    analyze(plan, rootReqs)
    return required
}

func (p *ProjectionPushdown) insertProjections(plan LogicalPlan, required map[LogicalPlan]*ColumnSet) (LogicalPlan, bool) {
    modified := false
    
    // Check each node to see if we can eliminate columns
    var visit func(node LogicalPlan) LogicalPlan
    visit = func(node LogicalPlan) LogicalPlan {
        // First recurse to children
        newChildren := make([]LogicalPlan, len(node.Children()))
        for i, child := range node.Children() {
            newChildren[i] = visit(child.(LogicalPlan))
        }
        
        // Rebuild node with new children
        node = rebuildWithChildren(node, newChildren)
        
        // Check if we should insert projection
        if p.shouldInsertProjection(node, required[node]) {
            reqCols := required[node]
            projExprs := make([]Expression, 0, len(reqCols.columns))
            
            // Create projection expressions
            for col := range reqCols.columns {
                projExprs = append(projExprs, &ColumnRef{
                    TableAlias: col.TableAlias,
                    ColumnName: col.ColumnName,
                })
            }
            
            // Insert projection
            projection := NewLogicalProject(node, projExprs, nil, nil)
            modified = true
            return projection
        }
        
        return node
    }
    
    result := visit(plan)
    return result, modified
}

func (p *ProjectionPushdown) shouldInsertProjection(node LogicalPlan, required *ColumnSet) bool {
    // Don't project if we need all columns
    if required.Contains(ColumnRef{ColumnName: "*"}) {
        return false
    }
    
    // Don't add projection immediately after another projection
    if _, ok := node.(*LogicalProject); ok {
        return false
    }
    
    // Check if we can eliminate columns
    availableCols := getAvailableColumns(node)
    if len(required.columns) < len(availableCols)*0.5 {
        // Eliminate if we need less than 50% of columns
        return true
    }
    
    // Special cases
    switch n := node.(type) {
    case *LogicalJoin:
        // Always project after joins to eliminate join columns
        return true
    case *LogicalSort:
        // Project after sort completes
        if n.parent != nil {
            if _, parentIsSort := n.parent.(*LogicalSort); !parentIsSort {
                return true
            }
        }
    }
    
    return false
}
```

### Step 5: Schema Management

Update schema after projections:

```go
func updateSchemaAfterProjection(projection *LogicalProject) {
    newSchema := &Schema{
        Columns: make([]SchemaColumn, len(projection.Projections)),
    }
    
    for i, expr := range projection.Projections {
        col := SchemaColumn{
            Name: getExpressionName(expr),
            Type: getExpressionType(expr, projection.Children()[0].Schema()),
        }
        
        if i < len(projection.Aliases) && projection.Aliases[i] != "" {
            col.Name = projection.Aliases[i]
        }
        
        newSchema.Columns[i] = col
    }
    
    projection.schema = newSchema
}
```

### Step 6: Integration Points

1. **With Index Selection**: 
   - Projections enable more index-only scans
   - Check covering indexes after projection

2. **With Join Reordering**:
   - Required columns affect join cost estimates
   - Less data = cheaper joins

3. **With Predicate Pushdown**:
   - Apply predicate pushdown first
   - Then projection pushdown

### Step 7: Testing Strategy

Create comprehensive tests:

```go
func TestProjectionPushdown(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string // Expected plan structure
    }{
        {
            name: "simple projection through join",
            input: `SELECT t1.a, t2.b 
                   FROM t1 JOIN t2 ON t1.id = t2.id`,
            expected: `Project[t1.a, t2.b]
                      └─Join[t1.id = t2.id]
                        ├─Project[t1.a, t1.id]
                        │ └─Scan[t1]
                        └─Project[t2.b, t2.id]
                          └─Scan[t2]`,
        },
        // More test cases...
    }
}
```

## Performance Validation

### Metrics to Track

1. **Columns Read**: Count columns fetched from storage
2. **Memory Usage**: Track row width * row count
3. **Network Transfer**: For distributed queries

### Benchmark Example

```go
func BenchmarkProjectionPushdown(b *testing.B) {
    // Create wide table (100 columns)
    setupWideTable()
    
    // Query selecting 2 columns
    query := "SELECT col1, col99 FROM wide_table WHERE col1 > 100"
    
    b.Run("WithoutProjection", func(b *testing.B) {
        disableProjectionPushdown()
        for i := 0; i < b.N; i++ {
            execute(query)
        }
    })
    
    b.Run("WithProjection", func(b *testing.B) {
        enableProjectionPushdown()
        for i := 0; i < b.N; i++ {
            execute(query)
        }
    })
}
```

## Common Pitfalls

1. **ORDER BY Columns**: Must keep columns needed for sorting until after sort
2. **Hidden Columns**: Some operations need columns not in final output
3. **Star Projections**: Handle SELECT * specially
4. **Expression Columns**: f(a,b) needs both a and b

## Next Steps

After implementing basic projection pushdown:

1. Add cost-based decisions (only push if beneficial)
2. Handle complex expressions and subqueries  
3. Integrate with statistics for better decisions
4. Add runtime projection elimination

---
*Part of Phase 4 Query Transformation Enhancements*