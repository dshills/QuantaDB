# Phase 5: MVCC Vacuum Process Plan

## Overview

The vacuum process is essential for maintaining database health in an MVCC system. As transactions create new row versions, old versions accumulate and consume storage space. The vacuum process identifies and removes obsolete versions that are no longer visible to any active or future transactions, reclaiming storage space and keeping version chains manageable.

## Current State

### What's Working
- Version chain management with proper backward-pointing chains
- Version chain traversal with visibility checks
- Transaction tracking with logical timestamps
- Version chain validation and statistics utilities

### What's Missing
1. **Dead Version Identification**: No mechanism to identify which versions can be safely removed
2. **Space Reclamation**: No way to physically remove dead versions and compact pages
3. **Transaction Horizon Tracking**: No system to determine the oldest transaction that needs to see data
4. **Automated Cleanup**: No background process or manual command to trigger cleanup

## Design Goals

1. **Safety First**: Never remove versions that might be needed by active transactions
2. **Incremental Operation**: Support both full and incremental vacuum operations
3. **Minimal Locking**: Minimize impact on concurrent operations
4. **Space Efficiency**: Effectively reclaim space from dead versions
5. **Performance**: Keep vacuum overhead low and predictable

## Key Concepts

### Transaction Horizon
The oldest snapshot timestamp that any active transaction might use. Versions deleted before this horizon are safe to remove.

### Dead Version
A row version that:
1. Has been deleted (DeletedAt > 0)
2. Was deleted before the transaction horizon
3. Is not the head of a version chain

### Vacuum Strategies
1. **Lazy Vacuum**: Marks space as reusable but doesn't compact pages
2. **Full Vacuum**: Compacts pages and rebuilds tables (future enhancement)

## Implementation Plan

### Step 1: Transaction Horizon Management (2 days)

#### 1.1 Create Transaction Horizon Tracker
- **New File**: `internal/txn/horizon.go`
- **Purpose**: Track the oldest active transaction snapshot
- **Interface**:
  ```go
  type HorizonTracker interface {
      // Get the oldest snapshot that might be needed
      GetHorizon() int64
      // Update when transactions start/end
      OnTransactionStart(txn *Transaction)
      OnTransactionEnd(txn *Transaction)
      // Get minimum of all active snapshots
      GetMinActiveSnapshot() int64
  }
  ```

#### 1.2 Integrate with Transaction Manager
- **File**: `internal/txn/manager.go`
- **Changes**:
  - Add HorizonTracker to Manager
  - Update BeginTransaction to register with tracker
  - Update Commit/Rollback to unregister
  - Handle long-running read transactions

### Step 2: Dead Version Detection (2 days)

#### 2.1 Create Vacuum Scanner
- **New File**: `internal/sql/executor/vacuum_scanner.go`
- **Purpose**: Scan tables and identify dead versions
- **Functions**:
  ```go
  type VacuumScanner struct {
      storage    *MVCCStorageBackend
      horizon    int64
      tableID    int64
  }
  
  // Find all dead versions in a table
  func (vs *VacuumScanner) FindDeadVersions() ([]DeadVersion, error)
  
  // Check if a specific version is dead
  func (vs *VacuumScanner) IsDeadVersion(row *MVCCRow, rowID RowID) bool
  ```

#### 2.2 Dead Version Criteria
- Version is dead if:
  1. `DeletedAt > 0 && DeletedAt < horizon`
  2. It's not the head of a version chain
  3. No active transaction needs it

### Step 3: Space Reclamation (3 days)

#### 3.1 Create Vacuum Executor
- **New File**: `internal/sql/executor/vacuum_executor.go`
- **Purpose**: Remove dead versions and update version chains
- **Core Operations**:
  ```go
  type VacuumExecutor struct {
      storage     *MVCCStorageBackend
      bufferPool  *storage.BufferPool
      stats       *VacuumStats
  }
  
  // Remove a dead version and update chain links
  func (ve *VacuumExecutor) RemoveDeadVersion(tableID int64, deadVersion DeadVersion) error
  
  // Update version chain to skip removed version
  func (ve *VacuumExecutor) UpdateVersionChain(tableID int64, beforeRowID, afterRowID RowID) error
  
  // Compact page if significant space freed
  func (ve *VacuumExecutor) CompactPageIfNeeded(pageID storage.PageID) error
  ```

#### 3.2 Version Chain Relinking
When removing version B from chain A→B→C:
1. Update A's NextVersion to point to C
2. Physically delete B
3. Update page free space accounting

#### 3.3 Page Compaction (Optional for Phase 5)
- Track freed space per page
- If > 50% free, compact by moving rows
- Update slot arrays and free space pointers

### Step 4: Vacuum Operations (2 days)

#### 4.1 Manual VACUUM Command
- **New File**: `internal/sql/executor/vacuum_operator.go`
- **SQL Support**: `VACUUM [table_name]`
- **Operations**:
  1. Get transaction horizon
  2. Scan for dead versions
  3. Remove dead versions
  4. Report statistics

#### 4.2 Vacuum Statistics
```go
type VacuumStats struct {
    TablesProcessed   int
    VersionsScanned   int
    VersionsRemoved   int
    SpaceReclaimed    int64
    PagesCompacted    int
    Duration          time.Duration
    Errors            []error
}
```

### Step 5: Safety and Testing (2 days)

#### 5.1 Safety Mechanisms
- **Transaction Barrier**: Prevent vacuum during DDL operations
- **Version Validation**: Verify chain integrity before/after removal
- **Rollback Support**: Log all operations for potential recovery
- **Lock Management**: Row-level locks during chain updates

#### 5.2 Comprehensive Testing
- **Unit Tests**:
  - Horizon calculation with concurrent transactions
  - Dead version detection accuracy
  - Version chain relinking correctness
  - Page compaction logic
  
- **Integration Tests**:
  - Vacuum with concurrent reads/writes
  - Long-running transaction handling
  - Space reclamation verification
  - Performance benchmarks

## Implementation Details

### Dead Version Storage
```go
type DeadVersion struct {
    RowID        RowID
    TableID      int64
    CreatedAt    int64
    DeletedAt    int64
    NextVersion  int64
    Size         int
}
```

### Vacuum Process Flow
```
1. START VACUUM
2. Get current transaction horizon
3. For each table:
   a. Scan all row versions
   b. Identify dead versions
   c. Build removal plan
   d. Execute removals:
      - Update version chains
      - Delete dead versions
      - Update space tracking
   e. Optional: Compact pages
4. Report statistics
5. END VACUUM
```

### Concurrent Operation Handling
- Use row-level locks when updating chains
- Never vacuum rows visible to active transactions
- Handle new versions created during vacuum
- Coordinate with buffer pool for page access

## Success Criteria

1. **Functional Requirements**:
   - ✓ Correctly identifies all dead versions
   - ✓ Safely removes versions without data loss
   - ✓ Maintains version chain integrity
   - ✓ Reclaims space effectively

2. **Performance Requirements**:
   - Vacuum overhead < 10% of normal operations
   - Can process 1M dead versions in < 1 minute
   - Minimal impact on concurrent queries

3. **Safety Requirements**:
   - Zero data loss or corruption
   - No impact on transaction isolation
   - Graceful handling of errors

## Risks and Mitigations

1. **Risk**: Removing versions still needed by transactions
   - **Mitigation**: Conservative horizon calculation
   - **Validation**: Check twice before removal

2. **Risk**: Version chain corruption during updates
   - **Mitigation**: Atomic chain pointer updates
   - **Recovery**: Validation and repair utilities

3. **Risk**: Performance impact on production
   - **Mitigation**: Incremental processing
   - **Throttling**: Configurable rate limits

## Configuration Options

```go
type VacuumConfig struct {
    // Minimum age before version can be removed (safety margin)
    MinDeadAge         time.Duration
    // Maximum versions to process per batch
    BatchSize          int
    // Sleep between batches (throttling)
    BatchDelay         time.Duration
    // Enable page compaction
    EnableCompaction   bool
    // Minimum free space % to trigger compaction
    CompactionThreshold float64
}
```

## Future Enhancements (Phase 6+)

1. **Autovacuum Daemon**:
   - Background process monitoring dead versions
   - Automatic triggering based on thresholds
   - Table-specific vacuum strategies

2. **HOT Updates** (Heap-Only Tuples):
   - Updates that don't modify indexed columns
   - Keep versions on same page
   - Reduce index maintenance overhead

3. **Vacuum Progress Tracking**:
   - Real-time progress reporting
   - Cancelable operations
   - Resume from interruption

4. **Advanced Compaction**:
   - Cross-page row movement
   - Table reorganization
   - Online table rebuild

## Estimated Timeline

- Total: 9 days
  - Step 1: 2 days (Transaction horizon)
  - Step 2: 2 days (Dead version detection)
  - Step 3: 3 days (Space reclamation)
  - Step 4: 2 days (Vacuum operations)
  - Step 5: 2 days (Safety and testing) - overlaps with development

## Notes

- Start with conservative approach - better to keep too much than remove needed data
- Consider vacuum impact on WAL and recovery
- Future: Integrate with statistics collection for better planning
- Monitor production workloads to tune vacuum parameters