# Index-Query Planner Integration Plan

## Overview
Integrate the existing B+Tree index implementation with the query planner to enable index-backed query execution. This will significantly improve query performance for indexed columns.

## Current State Analysis

### Existing Components
1. **B+Tree Index** (`internal/index/`)
   - Complete B+Tree implementation
   - Insert, search, delete, range scan operations
   - Not connected to query execution

2. **Query Planner** (`internal/sql/planner/`)
   - Logical plan generation
   - Physical plan creation
   - Basic cost estimation
   - No index awareness

3. **Storage Backend** (`internal/storage/backend/`)
   - Table creation with column definitions
   - Sequential scan implementation
   - No index metadata storage

## Implementation Plan

### Phase 1: Index Metadata Management (Days 1-2)

#### 1.1 Extend Catalog for Index Metadata
- Add index definitions to catalog schema
- Store index metadata alongside table metadata
- Track indexed columns and index types

#### 1.2 CREATE INDEX Parser Support
- Add CREATE INDEX statement to SQL parser
- Parse index name, table, columns
- Support UNIQUE constraint

#### 1.3 Index Creation in Storage
- Connect CREATE INDEX to B+Tree creation
- Store index files alongside table data
- Update catalog with index information

### Phase 2: Query Planning with Indexes (Days 3-5)

#### 2.1 Index-Aware Plan Generation
- Modify planner to check for available indexes
- Generate IndexScan alternatives to SeqScan
- Consider indexes for WHERE clause predicates

#### 2.2 Cost Model Enhancement
```go
// Simplified cost model
type PlanCost struct {
    StartupCost float64  // Cost before first row
    TotalCost   float64  // Total execution cost
    Rows        float64  // Estimated row count
}

// Index scan typically has:
// - Higher startup cost (index lookup)
// - Lower total cost for selective queries
// - Better performance for ORDER BY on indexed columns
```

#### 2.3 Statistics Framework
- Row count estimates
- Column cardinality
- Index selectivity
- Update statistics during INSERT/UPDATE/DELETE

### Phase 3: Index Scan Operator (Days 6-8)

#### 3.1 IndexScan Physical Operator
```go
type IndexScan struct {
    Table     *Table
    Index     *BTree
    Predicate Expression
    Direction ScanDirection
}
```

#### 3.2 Index-to-Heap Fetch
- Use index to get row IDs
- Fetch full rows from heap storage
- Handle visibility checks (MVCC)

#### 3.3 Index-Only Scans
- When query only needs indexed columns
- Avoid heap access entirely
- Significant performance boost

### Phase 4: Advanced Index Usage (Days 9-10)

#### 4.1 Multi-Column Indexes
- Support compound indexes
- Prefix matching for partial key lookups
- Column order optimization

#### 4.2 Index for Sorting
- Use indexes to avoid explicit sort operations
- Support ORDER BY via index traversal
- Handle DESC index scans

#### 4.3 Index for Joins
- Index nested loop joins
- Use indexes on join columns
- Optimize join order based on index availability

### Phase 5: Testing & Optimization (Days 11-14)

#### 5.1 Integration Tests
- Test all index operations
- Verify correct query results
- Compare performance with/without indexes

#### 5.2 Benchmarks
```sql
-- Create test schema
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE,
    created_at TIMESTAMP
);

-- Test queries
SELECT * FROM users WHERE id = 1000;          -- Primary key lookup
SELECT * FROM users WHERE email = 'test@example.com';  -- Unique index
SELECT * FROM users ORDER BY created_at;      -- Index scan for sorting
```

#### 5.3 Performance Monitoring
- Query execution time comparison
- Index usage statistics
- Cost model accuracy validation

## Technical Considerations

### 1. Index Maintenance
- Keep indexes updated during INSERT/UPDATE/DELETE
- Handle unique constraint violations
- Consider deferred index updates for bulk operations

### 2. Transaction Safety
- Ensure index operations are transactional
- Rollback index changes on transaction abort
- Coordinate with WAL for crash recovery

### 3. Memory Management
- Buffer pool integration for index pages
- LRU eviction for index nodes
- Separate cache for frequently accessed index roots

### 4. Query Optimization Rules
```
Rules for index selection:
1. Equality predicates on indexed columns
2. Range predicates on indexed columns
3. ORDER BY matches index order
4. JOIN conditions on indexed columns
5. Covering indexes for index-only scans
```

## Success Criteria

1. **Functional Requirements**
   - CREATE INDEX statement works
   - Query planner considers indexes
   - Correct query results with indexes
   - Performance improvement for indexed queries

2. **Performance Targets**
   - 100x speedup for point lookups
   - 10x speedup for range scans on large tables
   - Minimal overhead for index maintenance

3. **Code Quality**
   - Comprehensive test coverage
   - Clear documentation
   - Clean integration with existing code

## Risk Mitigation

1. **Complexity Risk**
   - Start with single-column B+Tree indexes
   - Add advanced features incrementally
   - Extensive testing at each phase

2. **Performance Risk**
   - Profile index maintenance overhead
   - Optimize hot paths
   - Consider async index updates

3. **Correctness Risk**
   - Extensive integration tests
   - Property-based testing for indexes
   - Comparison with PostgreSQL behavior

## Next Steps

1. Review current planner code structure
2. Design index metadata schema
3. Implement CREATE INDEX parser support
4. Begin Phase 1 implementation

---
*Estimated Total Time: 2 weeks*
*Priority: High*
*Dependencies: None (B+Tree already implemented)*