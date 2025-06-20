# Query Executor Design

## Overview

The query executor is responsible for executing query plans produced by the planner. It implements the Volcano model (iterator-based execution) where operators pull data from their children.

## Architecture

### Core Concepts

1. **Volcano Model**: Each operator implements an iterator interface with Open(), Next(), and Close() methods
2. **Pull-Based Execution**: Parent operators pull rows from child operators
3. **Pipelined Execution**: Rows flow through operators one at a time (with buffering where needed)
4. **Type Safety**: Strong typing throughout with runtime type checking

### Execution Flow

```
Client Request → Planner → Logical Plan → Physical Plan → Executor → Results
                                                              ↓
                                                         Storage Engine
```

## Operator Interface

```go
// Operator is the base interface for all execution operators
type Operator interface {
    // Open initializes the operator
    Open(ctx *ExecContext) error
    
    // Next returns the next row or nil when done
    Next() (*Row, error)
    
    // Close cleans up resources
    Close() error
    
    // Schema returns the output schema
    Schema() *Schema
}
```

## Core Operators

### 1. Scan Operator
- Reads data from storage engine
- Supports full table scans and range scans
- Handles column projection at storage level

### 2. Filter Operator
- Evaluates WHERE predicates
- Passes through rows that match
- Short-circuit evaluation for efficiency

### 3. Project Operator
- Evaluates expressions for SELECT columns
- Handles column renaming (aliases)
- Can compute derived columns

### 4. Sort Operator
- Implements ORDER BY
- Uses external sorting for large datasets
- Supports multiple sort keys and directions

### 5. Limit Operator
- Implements LIMIT and OFFSET
- Early termination optimization
- Minimal memory overhead

### 6. HashJoin Operator (future)
- Build hash table from smaller relation
- Probe with larger relation
- Supports all join types

### 7. Aggregate Operator (future)
- Implements GROUP BY and aggregations
- Hash-based or sort-based grouping
- Supports HAVING clauses

## Expression Evaluation

### Expression Evaluator
```go
type ExprEvaluator interface {
    Eval(row *Row, ctx *ExecContext) (Value, error)
}
```

### Supported Expressions
- Column references
- Literals
- Arithmetic operations (+, -, *, /, %)
- Comparison operations (=, !=, <, <=, >, >=)
- Logical operations (AND, OR, NOT)
- NULL handling (IS NULL, IS NOT NULL)
- Type casting
- Function calls (future)

## Execution Context

```go
type ExecContext struct {
    // Transaction context
    Txn Transaction
    
    // Memory allocator
    Memory *MemoryPool
    
    // Statistics collector
    Stats *ExecStats
    
    // Parameters for prepared statements
    Params []Value
    
    // Catalog for metadata lookups
    Catalog Catalog
}
```

## Memory Management

### Memory Pool
- Per-query memory allocation
- Spill to disk when exceeding limits
- Cleanup on query completion

### Buffering Strategy
- Streaming operators: Minimal buffering (Filter, Project)
- Blocking operators: Full materialization (Sort, HashJoin)
- Adaptive buffering based on available memory

## Error Handling

### Error Types
1. **Data Errors**: Type mismatches, constraint violations
2. **Resource Errors**: Out of memory, disk full
3. **System Errors**: I/O failures, network issues
4. **Cancellation**: Query timeout or client cancel

### Error Propagation
- Errors bubble up through operator tree
- Cleanup happens in reverse order
- Transaction rollback on error

## Performance Optimizations

### 1. Vectorized Execution (future)
- Process batches of rows
- Better CPU cache utilization
- SIMD instructions where possible

### 2. Predicate Pushdown
- Push filters to storage layer
- Reduce data movement
- Index usage optimization

### 3. Projection Pushdown
- Only read required columns
- Reduce memory footprint
- Better I/O efficiency

### 4. Pipeline Breakers
- Identify operators that require materialization
- Optimize memory usage for these operators
- Consider disk-based algorithms for large data

## Testing Strategy

### Unit Tests
- Test each operator in isolation
- Mock child operators
- Verify correctness and error handling

### Integration Tests
- Test operator combinations
- End-to-end query execution
- Performance benchmarks

### Property-Based Tests
- Generate random queries and data
- Verify result correctness
- Stress test edge cases

## Implementation Phases

### Phase 1: Basic Operators (Current)
1. Base operator framework
2. Scan operator with storage integration
3. Filter operator with expression evaluation
4. Project operator
5. Basic expression evaluator

### Phase 2: Sorting and Limiting
1. Sort operator with external sorting
2. Limit operator with offset support
3. Top-K optimization

### Phase 3: Joins and Aggregation
1. Nested loop join
2. Hash join
3. Basic aggregation
4. GROUP BY support

### Phase 4: Advanced Features
1. Window functions
2. Common table expressions (CTEs)
3. Recursive queries
4. Parallel execution

## Example: SELECT Query Execution

```sql
SELECT name, age * 2 as double_age 
FROM users 
WHERE age > 25 
ORDER BY name 
LIMIT 10
```

### Operator Tree
```
Limit(10)
  └── Sort(name ASC)
      └── Project(name, age * 2 AS double_age)
          └── Filter(age > 25)
              └── Scan(users)
```

### Execution Steps
1. **Open Phase** (top-down):
   - Limit.Open() → Sort.Open() → Project.Open() → Filter.Open() → Scan.Open()
   - Scan opens table handle
   - Sort allocates sort buffer

2. **Execution Phase** (pull-based):
   - Limit.Next() pulls from Sort
   - Sort.Next() buffers all rows from Project (blocking)
   - Project.Next() pulls from Filter, evaluates expressions
   - Filter.Next() pulls from Scan, evaluates predicate
   - Scan.Next() reads from storage

3. **Close Phase** (bottom-up):
   - Each operator cleans up resources
   - Sort releases memory
   - Scan closes table handle