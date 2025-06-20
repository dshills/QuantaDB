# MVCC Isolation Level Fix Plan

## Problem Summary

The current MVCC implementation has critical isolation level bugs:
1. **Repeatable Read not maintaining snapshot isolation** - Each scan uses `time.Now()` instead of transaction snapshot timestamp
2. **Phantom reads occurring** - New rows from concurrent transactions become visible within a transaction
3. **Non-repeatable reads in Repeatable Read** - Values change between reads within the same transaction

## Root Cause

The `MVCCStorageBackend.ScanTable()` method uses `time.Now().Unix()` for visibility checks instead of the transaction's snapshot timestamp. This breaks MVCC semantics.

## Fix Plan

### Phase 1: Thread Snapshot Timestamp Through Storage Operations

1. **Update Storage Interface** 
   - Add snapshot timestamp parameter to `ScanTable()` method
   - Update all storage backend implementations

2. **Modify MVCCStorageBackend**
   - Accept snapshot timestamp in `ScanTable()`
   - Pass timestamp to `MVCCRawRowIterator` for visibility checks
   - Remove `time.Now()` usage for visibility

3. **Update Executor Integration**
   - Modify `StorageScanOperator` to pass `ctx.SnapshotTS` to storage
   - Ensure all scan operations use transaction snapshot

### Phase 2: Fix Visibility Logic

1. **Enhance MVCCRawRowIterator**
   - Store snapshot timestamp in iterator struct
   - Use stored timestamp for all visibility checks
   - Ensure consistent visibility throughout iteration

2. **Update Visibility Rules**
   - Read Committed: Use current timestamp for each statement
   - Repeatable Read: Use transaction start timestamp for all reads
   - Serializable: Same as Repeatable Read (for now)

### Phase 3: Handle Edge Cases

1. **Transaction Timestamp Management**
   - Ensure snapshot timestamp is set when transaction begins
   - For Read Committed, update snapshot at statement boundaries
   - For Repeatable Read, maintain same snapshot throughout transaction

2. **Index Scans**
   - Apply same visibility filtering to index scans (when implemented)
   - Ensure consistency between table and index scans

### Phase 4: Testing and Validation

1. **Fix Existing Tests**
   - Verify `TestMVCCIsolationLevels` passes
   - Ensure no phantom reads in Repeatable Read
   - Confirm proper snapshot isolation

2. **Add New Tests**
   - Test visibility with multiple concurrent transactions
   - Verify timestamp consistency within transactions
   - Test edge cases (long-running transactions, high concurrency)

## Implementation Steps

### Step 1: Update Storage Interface
```go
// In internal/storage/storage.go
type Storage interface {
    // ... existing methods ...
    ScanTable(ctx context.Context, tableName string, snapshotTS int64) (RowIterator, error)
}
```

### Step 2: Update MVCCStorageBackend
```go
// In internal/storage/mvcc_storage.go
func (m *MVCCStorageBackend) ScanTable(ctx context.Context, tableName string, snapshotTS int64) (RowIterator, error) {
    // Pass snapshot timestamp to iterator
    rawIter, err := m.DiskStorageBackend.RawScanTable(ctx, tableName)
    if err != nil {
        return nil, err
    }
    return &MVCCRawRowIterator{
        RawRowIterator: rawIter,
        snapshotTS:     snapshotTS,  // Use provided snapshot
    }, nil
}
```

### Step 3: Update StorageScanOperator
```go
// In internal/sql/executor/scan_operators.go
func (s *StorageScanOperator) Next(ctx *ExecContext) (*Row, error) {
    // Use snapshot timestamp from context
    iter, err := ctx.Storage.ScanTable(ctx.Ctx, s.tableName, ctx.SnapshotTS)
    // ...
}
```

### Step 4: Fix ExecContext Usage
```go
// In internal/network/handler.go
// Ensure SnapshotTS is properly set based on isolation level
if ctx.IsolationLevel == sql.RepeatableRead || ctx.IsolationLevel == sql.Serializable {
    ctx.SnapshotTS = txn.StartTimestamp()
} else {
    // Read Committed gets new snapshot per statement
    ctx.SnapshotTS = conn.server.txnManager.GetCurrentTimestamp()
}
```

## Success Criteria

1. All MVCC isolation tests pass
2. No phantom reads in Repeatable Read isolation
3. Proper snapshot isolation maintained
4. Performance remains acceptable
5. No race conditions or concurrency issues

## Estimated Timeline

- Day 1-2: Update storage interfaces and MVCCStorageBackend
- Day 3: Update executor integration
- Day 4: Fix timestamp management and edge cases
- Day 5: Testing and validation

## Risks and Mitigations

1. **Risk**: Breaking existing functionality
   - **Mitigation**: Comprehensive test coverage before changes

2. **Risk**: Performance degradation
   - **Mitigation**: Benchmark before/after, optimize if needed

3. **Risk**: Compatibility issues
   - **Mitigation**: Careful interface updates, maintain backwards compatibility

## Notes

- This fix aligns with "Phase 3: Add visibility filtering to all scan operators" in the roadmap
- Consider extracting timestamp helpers to shared utility package (low priority)
- Future work: Apply same patterns to index scans when implemented