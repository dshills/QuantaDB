# Index-Query Integration Implementation Plan

## Overview
This document outlines the plan to complete the index-query integration in QuantaDB. While the index infrastructure is well-designed, critical integration points for maintaining indexes during DML operations are missing.

## Current State Analysis

### ✅ Completed Components
1. **B+Tree Index Implementation** - Full implementation with thread safety
2. **Index Manager** - Lifecycle management for indexes
3. **Query Planner Integration** - Cost-based index selection
4. **Index Scan Operators** - Various index scan strategies in executor

### ❌ Critical Missing Components
1. **Index Updates on DML** - INSERT/UPDATE/DELETE don't update indexes
2. **Index Population** - CREATE INDEX doesn't populate with existing data
3. **Storage Integration** - No hooks between storage and index manager

## Implementation Plan

### Phase 1: Fix Index Updates on DML Operations (Priority: CRITICAL)

#### 1.1 Update InsertOperator
- Modify `internal/sql/executor/insert.go`
- Add index manager reference to operator
- Call `indexMgr.InsertIntoIndexes()` after successful row insertion
- Handle errors and rollback

#### 1.2 Update UpdateOperator  
- Modify `internal/sql/executor/update.go`
- Add index manager reference to operator
- Delete old index entries before update
- Insert new index entries after update
- Handle partial index updates

#### 1.3 Update DeleteOperator
- Modify `internal/sql/executor/delete.go`
- Add index manager reference to operator
- Call `indexMgr.DeleteFromIndexes()` before row deletion
- Handle cascading deletes

### Phase 2: Implement Index Population on Creation

#### 2.1 Populate Existing Data
- Modify `internal/sql/executor/create_index.go`
- Add table scan to read all existing rows
- Insert each row into the new index
- Show progress for large tables

#### 2.2 Handle Concurrent Modifications
- Lock table during index creation
- Or implement online index building

### Phase 3: Integrate Index Manager with Executor

#### 3.1 Pass Index Manager to Operators
- Update `internal/sql/executor/executor.go`
- Modify operator factory methods
- Ensure all DML operators receive index manager

#### 3.2 Update Operator Interfaces
- Add index manager parameter to relevant operators
- Update constructor signatures
- Maintain backward compatibility

### Phase 4: Add Transaction Support

#### 4.1 Transactional Index Updates
- Ensure index updates use same transaction
- Implement rollback for index operations
- Handle concurrent index access

#### 4.2 WAL Integration
- Log index operations to WAL
- Implement index recovery from WAL
- Handle crash consistency

### Phase 5: Testing and Validation

#### 5.1 Unit Tests
- Test index updates on INSERT/UPDATE/DELETE
- Test index population on creation
- Test transaction rollback scenarios

#### 5.2 Integration Tests
- End-to-end index usage tests
- Performance benchmarks
- Concurrent access tests

#### 5.3 Regression Tests
- Ensure existing functionality not broken
- Test all index scan strategies
- Validate query planner decisions

## Implementation Order

1. **Day 1-2**: Fix INSERT to update indexes (highest priority)
2. **Day 3-4**: Fix UPDATE and DELETE to maintain indexes
3. **Day 5**: Implement index population on CREATE INDEX
4. **Day 6**: Add comprehensive tests
5. **Day 7**: Performance testing and optimization

## Success Criteria

1. All DML operations correctly maintain indexes
2. CREATE INDEX populates with existing data
3. No performance regression for non-indexed operations
4. All tests pass including new index tests
5. Documentation updated

## Risks and Mitigations

1. **Risk**: Performance impact on DML operations
   - **Mitigation**: Benchmark and optimize index update paths

2. **Risk**: Transaction complexity
   - **Mitigation**: Keep index updates in same transaction context

3. **Risk**: Concurrent access issues
   - **Mitigation**: Use existing locking mechanisms in index manager

## Next Steps

Begin with Phase 1.1 - updating the InsertOperator to maintain indexes on row insertion.