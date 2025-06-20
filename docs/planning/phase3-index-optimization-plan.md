# Phase 3: Advanced Index Optimization Implementation Plan

**Created**: December 21, 2024  
**Status**: Ready for Implementation  
**Target Completion**: 1 week  
**Prerequisites**: Phase 1 & 2 Complete ✅

## Overview

Phase 3 focuses on advanced index optimizations to dramatically improve query performance through multi-column index support, covering indexes (index-only scans), and index intersection capabilities.

## Current State

### ✅ Completed
- Basic B+Tree index implementation
- Single-column index scans
- Index creation and management
- Cost-based index selection

### ❌ Missing
- Multi-column (composite) index support
- Index-only scans (covering indexes)
- Index intersection for combining indexes
- Partial index support
- Index condition pushdown

## Implementation Tasks

## Task 3.1: Multi-Column Index Support (3 days)

### 3.1.1: Extend B+Tree for Composite Keys
**Files**: `internal/index/btree.go`, `internal/index/composite_key.go`
**Time**: 1 day

```go
// CompositeKey represents a multi-column index key
type CompositeKey struct {
    Values []types.Value
}

// Compare implements lexicographic comparison
func (c CompositeKey) Compare(other CompositeKey) int {
    for i := 0; i < len(c.Values) && i < len(other.Values); i++ {
        cmp := compareValues(c.Values[i], other.Values[i])
        if cmp != 0 {
            return cmp
        }
    }
    return len(c.Values) - len(other.Values)
}
```

**Implementation Steps**:
1. Create CompositeKey type with proper comparison logic
2. Update B+Tree to handle composite keys
3. Modify index creation to support multiple columns
4. Add serialization for composite keys

### 3.1.2: Index Matching for Composite Predicates
**Files**: `internal/sql/planner/index_matcher.go`
**Time**: 1 day

```go
type IndexMatch struct {
    Index            *catalog.Index
    EqualityColumns  []string     // Columns with = predicates
    RangeColumn      string       // First column with range predicate
    RangeStart       types.Value  // Range start value
    RangeEnd         types.Value  // Range end value
    IncludeStart     bool
    IncludeEnd       bool
    RemainingFilters []Expression // Filters not covered by index
}

func MatchCompositeIndex(index *catalog.Index, predicates []Expression) *IndexMatch {
    // Match predicates to index columns in order
    // Support patterns like: col1 = ? AND col2 = ? AND col3 > ?
}
```

**Implementation Steps**:
1. Create IndexMatch structure for composite matches
2. Implement prefix matching algorithm
3. Handle equality + single range pattern
4. Calculate selectivity for composite predicates

### 3.1.3: Query Planner Integration
**Files**: `internal/sql/planner/index_scan.go`
**Time**: 1 day

**Implementation Steps**:
1. Update IndexScan plan node for composite keys
2. Modify cost estimation for multi-column indexes
3. Implement index selection among multiple candidates
4. Add index hint support for testing

## Task 3.2: Index-Only Scans (2 days)

### 3.2.1: Covering Index Detection
**Files**: `internal/sql/planner/covering_index.go`
**Time**: 0.5 day

```go
func IsCoveringIndex(index *catalog.Index, requiredColumns []string) bool {
    // Check if all required columns are in the index
    // Consider both key columns and included columns
}

func PlanIndexOnlyScan(table *catalog.Table, index *catalog.Index, 
                       predicates []Expression, projections []string) *IndexOnlyScan {
    if !IsCoveringIndex(index, projections) {
        return nil
    }
    // Create index-only scan plan
}
```

### 3.2.2: Index-Only Scan Operator
**Files**: `internal/sql/executor/index_only_scan.go`
**Time**: 1 day

```go
type IndexOnlyScanOperator struct {
    table       *catalog.Table
    index       *catalog.Index
    indexMgr    *index.Manager
    startKey    types.Value
    endKey      types.Value
    projections []int // Column indices to extract from index
    iterator    index.Iterator
}

func (op *IndexOnlyScanOperator) Next() (*Row, error) {
    // Read directly from index without table lookup
    // Extract projected columns from index entry
}
```

**Implementation Steps**:
1. Create IndexOnlyScanOperator
2. Implement column extraction from index entries
3. Add MVCC visibility checks (if needed)
4. Optimize for sequential index scans

### 3.2.3: Visibility Map for MVCC
**Files**: `internal/storage/visibility_map.go`
**Time**: 0.5 day

```go
type VisibilityMap struct {
    // Track which pages have all-visible tuples
    // Enables index-only scans to skip visibility checks
}
```

**Implementation Steps**:
1. Design visibility map structure
2. Update during VACUUM operations
3. Use in index-only scans to avoid table access
4. Handle concurrent updates

## Task 3.3: Index Intersection (1 day)

### 3.3.1: Bitmap Index Scan
**Files**: `internal/sql/executor/bitmap_scan.go`
**Time**: 0.5 day

```go
type BitmapIndexScan struct {
    indexes     []*catalog.Index
    predicates  []Expression
    bitmap      *Bitmap
}

type Bitmap struct {
    // Efficient representation of matching row IDs
    // Support AND/OR operations between bitmaps
}
```

### 3.3.2: Index Intersection Planner
**Files**: `internal/sql/planner/index_intersection.go`
**Time**: 0.5 day

```go
func PlanIndexIntersection(table *catalog.Table, predicates []Expression) Plan {
    // Find indexes that can satisfy different predicates
    // Estimate cost of intersection vs single index
    // Choose optimal combination
}
```

## Task 3.4: Additional Optimizations (1 day)

### 3.4.1: Index Condition Pushdown
**Files**: `internal/sql/executor/index_scan.go`
**Time**: 0.5 day

- Push non-index predicates to index scan
- Early filtering during index traversal
- Reduce unnecessary table lookups

### 3.4.2: Partial Index Support
**Files**: `internal/catalog/index.go`
**Time**: 0.5 day

```go
type Index struct {
    // ... existing fields
    WhereClause Expression // Partial index predicate
}
```

## Testing Plan

### Unit Tests
1. **Composite Key Tests** (`index/composite_key_test.go`)
   - Comparison logic
   - Serialization/deserialization
   - Edge cases (NULL handling)

2. **Index Matching Tests** (`planner/index_matcher_test.go`)
   - Various predicate patterns
   - Selectivity estimation
   - Cost calculation

3. **Index-Only Scan Tests** (`executor/index_only_scan_test.go`)
   - Covering index detection
   - Column extraction
   - Performance comparison

### Integration Tests
1. **Multi-Column Index Queries**
   ```sql
   CREATE INDEX idx_orders ON orders(customer_id, order_date, status);
   -- Test various query patterns
   SELECT * FROM orders WHERE customer_id = 100 AND order_date > '2024-01-01';
   SELECT * FROM orders WHERE customer_id = 100 AND order_date = '2024-01-01' AND status = 'pending';
   ```

2. **Covering Index Queries**
   ```sql
   CREATE INDEX idx_covering ON products(category_id) INCLUDE (name, price);
   -- Should use index-only scan
   SELECT name, price FROM products WHERE category_id = 5;
   ```

3. **Index Intersection Queries**
   ```sql
   CREATE INDEX idx_status ON orders(status);
   CREATE INDEX idx_date ON orders(order_date);
   -- Should use bitmap intersection
   SELECT * FROM orders WHERE status = 'pending' AND order_date > '2024-01-01';
   ```

### Performance Benchmarks
1. **Composite Index Performance**
   - Compare with single-column indexes
   - Measure selectivity improvements
   - Test with various data distributions

2. **Index-Only Scan Performance**
   - Measure I/O reduction
   - Compare with regular index scans
   - Test with different covering percentages

3. **Index Intersection Performance**
   - Compare with single index + filter
   - Measure bitmap operation overhead
   - Test with various selectivities

## Success Criteria

### Functional
- ✅ Multi-column indexes reduce query execution time by 50-80%
- ✅ Index-only scans eliminate table access for covered queries
- ✅ Index intersection effectively combines multiple indexes
- ✅ All existing index functionality remains working

### Performance
- ✅ TPC-H Q6 (range query) improves by 5x with composite index
- ✅ Covering indexes reduce I/O by 90% for projection queries
- ✅ Index intersection outperforms single index for multi-predicate queries

### Quality
- ✅ 100% test coverage for new index code
- ✅ No performance regression for existing queries
- ✅ Memory usage remains reasonable for bitmap operations

## Implementation Order

1. **Day 1**: Composite key implementation and B+Tree updates
2. **Day 2**: Index matching and planner integration
3. **Day 3**: Complete multi-column index support with tests
4. **Day 4**: Index-only scan implementation
5. **Day 5**: Index intersection and final optimizations
6. **Day 6**: Integration testing and benchmarking
7. **Day 7**: Bug fixes and documentation

## Risk Mitigation

### Technical Risks
1. **Composite Key Complexity**: Handle NULL values and collations properly
2. **MVCC Compatibility**: Ensure index-only scans respect visibility
3. **Memory Usage**: Bitmap operations for large tables

### Mitigation Strategies
1. Extensive testing with edge cases
2. Fallback to regular scans if visibility map unavailable
3. Bitmap size limits and spilling to disk if needed

## Next Steps After Phase 3

With advanced index optimization complete, we'll have:
- Dramatic performance improvements for complex queries
- Reduced I/O through covering indexes
- Better multi-predicate query handling

This sets us up for Phase 4: Query Transformation Enhancements, focusing on:
- Complete projection pushdown
- Subquery decorrelation
- CTE optimization
- Set operation optimization