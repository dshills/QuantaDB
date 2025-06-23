# JOIN Column Resolution Fix Plan

## Problem Summary
JOIN queries fail with "column not resolved" errors because:
1. Column names in JOIN schemas aren't qualified with table names/aliases
2. The column resolver can't find columns from specific tables in JOIN results
3. This blocks most TPC-H queries which use multi-table JOINs

## Root Cause Analysis

### Current Behavior
1. When creating a JOIN, schemas are concatenated without qualification
2. Column names lose their table context
3. When resolving "table.column" references, the resolver can't match them

### Example
```sql
FROM customer c, orders o
WHERE c.c_custkey = o.o_custkey
```
- The JOIN schema has columns: [c_custkey, c_name, ..., o_custkey, o_orderkey, ...]
- But when resolving "c.c_custkey", it looks for a column named "c_custkey" with table "c"
- The schema doesn't track which columns came from which table

## Implementation Plan

### Step 1: Track Table Source in Schema
Modify the Column struct to include table information:
- Add `TableName` and `TableAlias` fields to track source
- Update schema building to preserve this information

### Step 2: Update JOIN Schema Building
In both planner and executor:
- When concatenating schemas, preserve table information
- Set TableAlias from the JOIN's table aliases

### Step 3: Fix Column Resolution
Update `buildExprEvaluatorWithSchema`:
- Match columns by name AND table alias if provided
- Support both qualified (table.column) and unqualified references
- Handle ambiguous columns appropriately

### Step 4: Update Expression Building
Ensure ColumnRef expressions preserve table information:
- Parser already captures TableAlias
- Ensure it's propagated through planning

## Code Changes

### 1. Schema Column Enhancement
```go
// internal/sql/executor/schema.go
type Column struct {
    Name       string
    Type       types.DataType  
    Nullable   bool
    TableName  string // Add source table name
    TableAlias string // Add table alias
}
```

### 2. JOIN Schema Building
```go
// internal/sql/planner/planner.go
func (p *Planner) buildJoinSchema(left, right LogicalPlan, leftAlias, rightAlias string) *Schema {
    // Qualify columns from left side
    // Qualify columns from right side
    // Combine into new schema
}
```

### 3. Column Resolution Fix
```go
// internal/sql/executor/expression.go
func buildExprEvaluatorWithSchema(expr planner.Expression, schema *Schema) (ExprEvaluator, error) {
    // Enhanced column matching logic
    // Support qualified and unqualified names
}
```

## Testing Strategy
1. Unit tests for column resolution with qualified names
2. Integration tests for multi-table JOINs
3. TPC-H query tests that were previously failing

## Success Criteria
- JOIN queries with qualified column names work
- TPC-H queries with JOINs execute successfully
- No regression in existing queries