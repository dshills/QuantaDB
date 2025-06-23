# DISTINCT Implementation Plan

## Overview

Implement DISTINCT support in QuantaDB to enable queries like:
- `SELECT DISTINCT column FROM table`
- `SELECT DISTINCT col1, col2 FROM table`
- `SELECT COUNT(DISTINCT column) FROM table`

Currently, DISTINCT queries fail with: "unexpected token in expression: DISTINCT"

## Background

DISTINCT eliminates duplicate rows from the result set. In SQL:
- `DISTINCT` applies to the entire row, not individual columns
- `DISTINCT ON (columns)` is a PostgreSQL extension (not in scope)
- `COUNT(DISTINCT expr)` is a special aggregate function

## Implementation Approach

### 1. Parser Support
- Check if parser already recognizes DISTINCT keyword
- Add DISTINCT flag to SelectStmt AST node
- Handle DISTINCT in aggregate functions (COUNT(DISTINCT))

### 2. Logical Plan
- Add DistinctPlan/LogicalDistinct operator
- Place after projection, before sorting/limit
- Track which columns to use for distinctness

### 3. Physical Operator
- Create DistinctOperator in executor
- Use hash-based approach for efficiency
- Alternative: sort-based approach for ordered data

### 4. Query Planning
- Detect DISTINCT in SelectStmt
- Insert LogicalDistinct into plan tree
- Optimize placement relative to other operators

## Technical Design

### Parser Changes
```go
type SelectStmt struct {
    Distinct bool  // Add this flag
    Columns  []SelectColumn
    // ... other fields
}
```

### Executor Operator
```go
type DistinctOperator struct {
    baseOperator
    child    Operator
    seen     map[uint64]bool  // Hash of seen rows
    rowBuf   []*Row          // Buffer for iteration
}
```

### Algorithm
1. Read all rows from child operator
2. Hash each row to detect duplicates
3. Keep only first occurrence of each unique row
4. Return deduplicated rows

### Hash Function
- Use FNV-64 hash of all column values
- Handle NULL values consistently
- Consider type-specific hashing

## Query Examples

### Basic DISTINCT
```sql
SELECT DISTINCT product FROM sales;
-- Returns unique product values
```

### Multi-Column DISTINCT
```sql
SELECT DISTINCT product, region FROM sales;
-- Returns unique (product, region) combinations
```

### DISTINCT with ORDER BY
```sql
SELECT DISTINCT product FROM sales ORDER BY product;
-- Distinct first, then sort
```

### COUNT(DISTINCT)
```sql
SELECT COUNT(DISTINCT product) FROM sales;
-- Count unique products (aggregate function)
```

## Implementation Steps

1. **Parser Analysis**
   - Locate DISTINCT parsing in lexer/parser
   - Understand current error location
   - Add necessary AST support

2. **Executor Implementation**
   - Create DistinctOperator
   - Implement Open/Next/Close methods
   - Handle memory management

3. **Planner Integration**
   - Detect DISTINCT flag
   - Create LogicalDistinct plan
   - Convert to DistinctOperator

4. **Testing**
   - Unit tests for operator
   - Integration tests with queries
   - Performance testing

## Optimization Opportunities

1. **Sort-Based Distinct**
   - If data is already sorted, use sequential scan
   - More memory efficient for large datasets

2. **Push Down Distinct**
   - In some cases, push below joins
   - Reduce intermediate result size

3. **Index Usage**
   - Use indexes for DISTINCT on indexed columns
   - Skip duplicate values during index scan

## Success Criteria

1. All example queries execute correctly
2. Correct handling of NULL values
3. Memory usage scales reasonably
4. Performance comparable to PostgreSQL
5. TPC-H queries using DISTINCT work

## Risks and Mitigation

1. **Memory Usage**
   - Risk: Large datasets exhaust memory
   - Mitigation: Implement spill-to-disk

2. **Performance**
   - Risk: Hash computation overhead
   - Mitigation: Optimize hash function

3. **NULL Handling**
   - Risk: Incorrect NULL semantics
   - Mitigation: Test edge cases thoroughly

## Timeline

- Hour 1: Parser analysis and AST changes
- Hour 2: Implement DistinctOperator
- Hour 3: Planner integration
- Hour 4: Testing and fixes
- Hour 5: Documentation and optimization