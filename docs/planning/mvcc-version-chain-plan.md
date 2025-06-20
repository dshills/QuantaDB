# Phase 4: MVCC Version Chain Management Plan

## Overview

Version chain management is a critical component of MVCC that tracks multiple versions of the same row. Currently, our MVCC implementation creates new row versions but doesn't properly maintain chains linking old and new versions together. This phase will implement proper version chain management to support efficient version traversal and enable future vacuum operations.

## Current State

### What's Working
- MVCC row format with version metadata (CreatedByTxn, CreatedAt, DeletedByTxn, DeletedAt, NextVersion)
- Snapshot-based visibility filtering for reads
- Proper isolation level handling (Read Committed, Repeatable Read)
- Logical timestamp generation and consistency

### What's Missing
1. **Version Chain Linking**: UpdateRow creates new versions but doesn't properly link them
2. **Version Chain Traversal**: No mechanism to follow version chains during reads
3. **Version Chain Cleanup**: No way to identify which versions can be safely removed
4. **Index Consistency**: Indexes don't handle multiple row versions properly

## Design Goals

1. **Maintain Version Chains**: Every update creates a new version linked to the previous one
2. **Efficient Traversal**: Quickly find the correct version for a given snapshot
3. **Space Efficiency**: Enable identification of obsolete versions for cleanup
4. **Index Compatibility**: Ensure indexes work correctly with versioned rows

## Implementation Plan

### Step 1: Fix Version Chain Creation (2 days)

#### 1.1 Update Row Version Linking
- **File**: `internal/sql/executor/mvcc_storage_backend.go`
- **Method**: `UpdateRow()`
- **Changes**:
  ```go
  // Current: Creates new version but doesn't link properly
  // New: Properly link old version to new version
  
  1. Insert new row version with updated data
  2. Get the RowID of the new version
  3. Update old row's NextVersion to point to new RowID
  4. Mark old row as deleted by current transaction
  ```

#### 1.2 Add Version Pointer Encoding/Decoding
- **File**: `internal/sql/executor/mvcc_row.go`
- **Functions**: Already have `EncodeVersionPointer()` and `DecodeVersionPointer()`
- **Verify**: These functions correctly handle PageID and SlotID encoding

### Step 2: Implement Version Chain Traversal (2 days)

#### 2.1 Create Version Chain Iterator
- **New File**: `internal/sql/executor/version_chain_iterator.go`
- **Purpose**: Traverse version chains to find appropriate version for a snapshot
- **Interface**:
  ```go
  type VersionChainIterator interface {
      // Find the visible version for a given snapshot
      FindVersion(rowID RowID, snapshotTS int64) (*MVCCRow, RowID, error)
      // Get all versions of a row (for debugging/admin)
      GetAllVersions(rowID RowID) ([]*MVCCRow, []RowID, error)
  }
  ```

#### 2.2 Update Scan Operations
- **File**: `internal/sql/executor/mvcc_storage_backend.go`
- **Method**: `GetRow()`
- **Changes**:
  - Use version chain iterator to find correct version
  - Handle case where newer versions exist but aren't visible

### Step 3: Implement Version Chain Index Management (2 days)

#### 3.1 Index Entry Versioning
- **Problem**: Indexes currently point to a single RowID, but with MVCC, multiple versions exist
- **Solution**: 
  - Index entries always point to the head of the version chain (newest version)
  - During index lookups, traverse the chain to find the visible version
  
#### 3.2 Update Index Operations
- **File**: `internal/sql/executor/index_scan.go`
- **Changes**:
  - After getting RowID from index, use version chain iterator
  - Find the appropriate version based on snapshot timestamp

### Step 4: Add Version Chain Utilities (1 day)

#### 4.1 Version Chain Validation
- **New Function**: `ValidateVersionChain(tableID int64, rowID RowID) error`
- **Purpose**: Verify chain integrity (no cycles, proper linking)

#### 4.2 Version Chain Statistics
- **New Function**: `GetVersionChainStats(tableID int64) *VersionChainStats`
- **Returns**:
  - Average chain length
  - Longest chain
  - Number of chains
  - Dead version count

### Step 5: Testing and Validation (2 days)

#### 5.1 Unit Tests
- **File**: `internal/sql/executor/version_chain_test.go`
- **Tests**:
  - Version chain creation on update
  - Version chain traversal with different snapshots
  - Concurrent updates creating proper chains
  - Chain validation detecting corruption

#### 5.2 Integration Tests
- **File**: `internal/sql/executor/mvcc_integration_test.go`
- **Tests**:
  - Multi-version reads with different isolation levels
  - Long version chains (many updates to same row)
  - Version chain consistency under concurrent load
  - Index scans with versioned rows

## Implementation Details

### Version Pointer Format
```go
// 48 bits total: 32 bits for PageID, 16 bits for SlotID
// Stored as int64 in MVCCRowHeader.NextVersion
// 0 means no next version
```

### Version Chain Structure
```
[Current Version] -> [Previous Version] -> [Original Version] -> 0
     (Newest)            (Older)             (Oldest)
```

### Visibility Rules for Chained Versions
1. Start from the head of the chain (newest version)
2. Traverse backwards until finding a version where:
   - CreatedAt <= snapshotTS
   - DeletedAt == 0 OR DeletedAt > snapshotTS
3. If no visible version found, row doesn't exist for this snapshot

## Success Criteria

1. **Functional Requirements**:
   - ✓ Updates create properly linked version chains
   - ✓ Reads traverse chains to find correct version
   - ✓ All existing MVCC tests continue to pass
   - ✓ New version chain tests pass

2. **Performance Requirements**:
   - Version chain traversal < 1ms for chains up to 10 versions
   - No significant degradation in scan performance
   - Index lookups remain efficient despite versioning

3. **Correctness Requirements**:
   - No lost updates
   - Consistent reads at all isolation levels
   - Proper version visibility based on snapshots

## Risks and Mitigations

1. **Risk**: Performance degradation with long version chains
   - **Mitigation**: Implement chain length limits and monitoring
   - **Future**: Phase 5 vacuum will clean up old versions

2. **Risk**: Storage overhead from multiple versions
   - **Mitigation**: Track metrics to identify hot rows
   - **Future**: Vacuum process will reclaim space

3. **Risk**: Index inconsistency with versioned rows
   - **Mitigation**: Careful testing of index operations
   - **Consider**: HOT (Heap Only Tuple) optimization later

## Dependencies

- Completed: Phase 3 (Snapshot-based visibility)
- Required: Understanding of current page/slot structure
- Required: MVCCRow format and serialization

## Next Steps After Phase 4

**Phase 5: Vacuum Process**
- Identify and remove obsolete versions
- Compact version chains
- Reclaim storage space
- Update indexes after cleanup

## Estimated Timeline

- Total: 7 days
  - Step 1: 2 days (Version chain creation)
  - Step 2: 2 days (Chain traversal)
  - Step 3: 2 days (Index integration)
  - Step 4: 1 day (Utilities)
  - Step 5: 2 days (Testing) - overlaps with development

## Notes

- The existing `EncodeVersionPointer` and `DecodeVersionPointer` functions suggest this was planned
- Current `UpdateRow` in `MVCCStorageBackend` has TODOs about version chain management
- Consider read-heavy vs write-heavy workload optimizations
- Future optimization: In-place updates for non-indexed columns (PostgreSQL HOT)