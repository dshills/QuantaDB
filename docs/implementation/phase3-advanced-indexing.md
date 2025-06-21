# Phase 3: Advanced Indexing Implementation Plan

**Status**: 🔄 In Progress  
**Started**: December 21, 2024  
**Goal**: Implement multi-column indexes, covering indexes, and advanced index operations

## Overview

Phase 3 focuses on advanced indexing capabilities that will dramatically improve query performance and bring QuantaDB closer to production-ready status. This phase builds upon the solid foundation of Phase 2's join optimizations.

## Architecture Goals

### Current Index System
- ✅ Single-column B+Tree indexes
- ✅ Basic index scans
- ✅ Cost-based index selection
- ✅ Integration with query planner

### Phase 3 Enhancements
- 🎯 Multi-column (composite) indexes
- 🎯 Covering index optimization  
- 🎯 Index-only scans
- 🎯 Bitmap index scans for complex predicates
- 🎯 Index intersection algorithms

## Implementation Plan

### Task 1: Composite Key Support for B+Tree
**Priority**: High  
**Estimated Effort**: 2-3 hours  
**Files**: `internal/index/`, `internal/sql/types/`

#### Goals:
- Extend B+Tree to support multi-column keys
- Implement composite key comparison logic
- Add serialization for composite keys
- Update insert/search/delete operations

#### Implementation Steps:
1. **Composite Key Structure**:
   ```go
   type CompositeKey struct {
       Values []types.Value
       Types  []types.Type
   }
   ```

2. **Comparison Function**:
   - Lexicographic ordering across columns
   - NULL handling for each column
   - Type-safe comparisons

3. **Serialization**:
   - Efficient binary format
   - Variable-length encoding
   - Backwards compatibility

4. **B+Tree Updates**:
   - Generic key interface
   - Update all tree operations
   - Maintain existing single-column support

#### Success Criteria:
- [ ] Composite keys work in B+Tree operations
- [ ] Performance comparable to single-column
- [ ] All existing tests pass
- [ ] New comprehensive test suite

### Task 2: Index Matching for Composite Predicates  
**Priority**: High  
**Estimated Effort**: 2-3 hours  
**Files**: `internal/sql/planner/`, `internal/catalog/`

#### Goals:
- Match queries to appropriate composite indexes
- Implement partial index usage (leftmost prefix rule)
- Cost-based selection between multiple indexes

#### Implementation Steps:
1. **Predicate Analysis**:
   - Extract equality/range conditions
   - Identify column order requirements
   - Handle mixed condition types

2. **Index Matching Algorithm**:
   ```go
   func (p *Planner) findBestIndex(table *Table, predicates []Predicate) *IndexMatch {
       // Score each index based on:
       // - Number of columns matched
       // - Selectivity estimates  
       // - Index scan vs table scan cost
   }
   ```

3. **Leftmost Prefix Support**:
   - Use partial composite indexes
   - Calculate remaining filter cost
   - Optimize for range scans

#### Success Criteria:
- [ ] Correct index selection for composite predicates
- [ ] Partial index usage works correctly
- [ ] Cost estimates are accurate
- [ ] Performance improvements measurable

### Task 3: Query Planner Integration
**Priority**: High  
**Estimated Effort**: 2-3 hours  
**Files**: `internal/sql/planner/`, `internal/sql/executor/`

#### Goals:
- Update planner to generate composite index scans
- Implement index scan operators for multi-column
- Integration with existing optimization passes

#### Implementation Steps:
1. **Index Scan Generation**:
   - Create composite key scan ranges
   - Handle partial key scans
   - Optimize for common patterns

2. **Statistics Integration**:
   - Multi-column selectivity estimates
   - Histogram support for composite keys
   - Correlation analysis between columns

3. **Plan Optimization**:
   - Choose between multiple viable indexes
   - Consider index intersection
   - Balance index scans vs table scans

#### Success Criteria:
- [ ] Generated plans use composite indexes
- [ ] Cost estimates drive correct decisions
- [ ] Query performance improvements
- [ ] No regression in existing functionality

### Task 4: Covering Index Detection
**Priority**: High  
**Estimated Effort**: 2-3 hours  
**Files**: `internal/sql/planner/`, `internal/catalog/`

#### Goals:
- Detect when index covers all needed columns
- Eliminate table lookups for covered queries
- Optimize SELECT, WHERE, and ORDER BY clauses

#### Implementation Steps:
1. **Coverage Analysis**:
   ```go
   func (idx *Index) CoversQuery(requiredCols []string) bool {
       // Check if index includes all required columns
       // Consider both key and included columns
   }
   ```

2. **Plan Generation**:
   - Generate index-only scan plans
   - Compare costs with index+table lookup
   - Handle visibility and MVCC considerations

3. **Column Tracking**:
   - Track required columns through plan tree
   - Prune unnecessary columns early
   - Optimize projection pushdown

#### Success Criteria:
- [ ] Covering indexes detected correctly
- [ ] Significant performance improvement
- [ ] Correct results with MVCC
- [ ] Comprehensive test coverage

### Task 5: IndexOnlyScanOperator
**Priority**: High  
**Estimated Effort**: 2-3 hours  
**Files**: `internal/sql/executor/`

#### Goals:
- Implement index-only scan execution
- Handle MVCC visibility without table access
- Optimize for high performance

#### Implementation Steps:
1. **Operator Implementation**:
   ```go
   type IndexOnlyScanOperator struct {
       indexScan    *IndexScanOperator
       visibilityMap *VisibilityMap  // For MVCC
       outputSchema *Schema
   }
   ```

2. **Visibility Handling**:
   - Check tuple visibility from index
   - Fall back to table when needed
   - Optimize for common cases

3. **Performance Optimization**:
   - Batch visibility checks
   - Prefetch strategies
   - Memory-efficient implementation

#### Success Criteria:
- [ ] Index-only scans execute correctly
- [ ] MVCC semantics preserved
- [ ] Performance significantly better than table scans
- [ ] Integration with query planner

### Task 6: Visibility Map for MVCC
**Priority**: Medium  
**Estimated Effort**: 2-3 hours  
**Files**: `internal/storage/`, `internal/txn/`

#### Goals:
- Track tuple visibility at page level
- Enable efficient index-only scans
- Integrate with MVCC system

#### Implementation Steps:
1. **Visibility Map Structure**:
   - Bitmap per page indicating all-visible tuples
   - Efficient storage and updates
   - Integration with vacuum process

2. **MVCC Integration**:
   - Update visibility on commits/aborts
   - Handle concurrent transactions
   - Maintain consistency

3. **Index-Only Scan Integration**:
   - Fast visibility checks
   - Fallback mechanisms
   - Performance monitoring

#### Success Criteria:
- [ ] Visibility map accurately tracks tuple visibility
- [ ] Index-only scans use visibility information
- [ ] No false positives/negatives
- [ ] Minimal overhead

### Task 7: Bitmap Index Scan
**Priority**: Medium  
**Estimated Effort**: 3-4 hours  
**Files**: `internal/sql/executor/`

#### Goals:
- Implement bitmap scans for complex predicates
- Enable index intersection and union
- Optimize for large result sets

#### Implementation Steps:
1. **Bitmap Implementation**:
   ```go
   type TupleBitmap struct {
       pages map[PageID]*PageBitmap
       exact bool  // vs lossy compression
   }
   ```

2. **Scan Operators**:
   - BitmapIndexScanOperator
   - BitmapAndOperator (intersection)
   - BitmapOrOperator (union)
   - BitmapHeapScanOperator

3. **Optimization**:
   - Lossy bitmap compression
   - Efficient set operations
   - Memory management

#### Success Criteria:
- [ ] Bitmap scans work for complex queries
- [ ] Index intersection produces correct results
- [ ] Memory usage within reasonable bounds
- [ ] Performance improvement for target queries

### Task 8: Index Intersection Planner Logic
**Priority**: Medium  
**Estimated Effort**: 2-3 hours  
**Files**: `internal/sql/planner/`

#### Goals:
- Generate bitmap index intersection plans
- Cost-based selection of intersection strategy
- Integration with existing planner

#### Implementation Steps:
1. **Plan Generation**:
   - Identify intersection opportunities
   - Generate bitmap scan plans
   - Cost individual vs intersection scans

2. **Cost Modeling**:
   - Estimate selectivity of intersections
   - Model bitmap memory usage
   - Compare with other access methods

3. **Optimization Rules**:
   - When to use intersection vs single index
   - Order of intersection operations
   - Memory vs CPU trade-offs

#### Success Criteria:
- [ ] Planner generates bitmap intersection plans
- [ ] Cost estimates drive correct decisions
- [ ] Performance improvement on complex queries
- [ ] No regression on simple queries

## Testing Strategy

### Unit Tests
- [ ] Composite key comparison and serialization
- [ ] B+Tree operations with multi-column keys
- [ ] Index matching algorithms
- [ ] Covering index detection
- [ ] Bitmap operations

### Integration Tests  
- [ ] End-to-end query execution with composite indexes
- [ ] Index-only scan correctness
- [ ] MVCC compliance for index-only scans
- [ ] Performance regression tests

### Performance Tests
- [ ] Composite index vs single-column benchmarks
- [ ] Covering index performance gains
- [ ] Bitmap scan vs other methods
- [ ] Memory usage under load

## Documentation Updates

### Implementation Documentation
- [ ] Composite index design decisions
- [ ] Index selection algorithm details
- [ ] Performance characteristics
- [ ] Usage examples and best practices

### User Documentation
- [ ] CREATE INDEX syntax extensions
- [ ] Query optimization guidelines
- [ ] Performance tuning recommendations
- [ ] EXPLAIN output interpretation

## Success Metrics

### Performance Targets
- [ ] 5-10x improvement for queries using composite indexes
- [ ] 2-5x improvement for covering index queries
- [ ] <10% memory overhead for index structures
- [ ] No regression on existing functionality

### Feature Completeness
- [ ] Support for up to 32 columns per index
- [ ] All SQL standard comparison operators
- [ ] Integration with ANALYZE for statistics
- [ ] Production-ready error handling

## Risk Mitigation

### Technical Risks
1. **Complexity**: Phase implementation to manage complexity
2. **Performance**: Continuous benchmarking and optimization
3. **Memory**: Careful memory management and testing
4. **Compatibility**: Extensive regression testing

### Schedule Risks
1. **Estimation**: Buffer time for complex tasks
2. **Dependencies**: Clear task ordering and dependencies
3. **Quality**: Don't compromise on testing and documentation

## Post-Phase 3 Opportunities

### Phase 4 Candidates
- Parallel index scans
- Partial indexes (WHERE clauses)
- Expression indexes
- Full-text search indexes
- Spatial indexing support

### Performance Optimizations
- Index compression techniques
- Adaptive index selection
- Machine learning for cost estimation
- Automatic index recommendations

---

**Total Estimated Effort**: 16-22 hours  
**Expected Timeline**: 2-3 days of focused implementation  
**Key Dependencies**: Phase 2 completion, stable B+Tree implementation