# Transaction-Storage Full Integration Plan

## Overview
This document outlines the plan to fully integrate QuantaDB's MVCC transaction system with the storage engine, enabling true multi-version concurrency control with proper isolation levels.

## Current State
- **MVCC Manager**: Fully implemented with timestamp-based versioning
- **Storage Engine**: Disk-based with basic CRUD operations
- **Missing Link**: Transaction metadata not stored with rows, no visibility filtering

## Implementation Plan

### Phase 1: Enhanced Row Format (2 days)
#### Goal: Add MVCC metadata to stored rows

1. **Define MVCC Row Header**
   ```go
   type MVCCRowHeader struct {
       RowID        int64
       CreatedByTxn int64
       CreatedAt    int64
       DeletedByTxn int64  // 0 if not deleted
       DeletedAt    int64  // 0 if not deleted
       NextVersion  int64  // PageID:Offset of next version
   }
   ```

2. **Update Row Serialization**
   - Modify `SerializeRow()` to include MVCC header
   - Update `DeserializeRow()` to read MVCC metadata
   - Maintain backward compatibility flag

3. **Tasks**:
   - [ ] Define MVCCRowHeader structure
   - [ ] Update row serialization format
   - [ ] Modify storage backend to write MVCC metadata
   - [ ] Add tests for new row format

### Phase 2: Transaction Context Threading (2 days)
#### Goal: Properly propagate transaction IDs through all operations

1. **Update ExecContext**
   - Remove `LegacyTxn` field usage
   - Use MVCC `Txn` for all operations
   - Add helper methods for transaction access

2. **Update Storage Operations**
   - Pass transaction ID to all storage methods
   - Set `currentTxnID` from ExecContext
   - Use transaction timestamp for version ordering

3. **Tasks**:
   - [ ] Refactor ExecContext to use single transaction
   - [ ] Update storage backend method signatures
   - [ ] Thread transaction through executor operations
   - [ ] Remove legacy transaction usage

### Phase 3: Visibility Filtering (3 days)
#### Goal: Implement visibility checks in all read operations

1. **Update Scan Iterator**
   ```go
   type MVCCRowIterator struct {
       baseIterator *diskRowIterator
       txn          *Transaction
       snapshot     *Snapshot
   }
   ```

2. **Implement Visibility Checks**
   - Wrap `diskRowIterator` with MVCC filtering
   - Check each row against transaction snapshot
   - Skip invisible versions
   - Handle version chains

3. **Update Operations**
   - Modify `ScanTable()` to use MVCC iterator
   - Update index scans for visibility
   - Ensure consistent reads across operations

4. **Tasks**:
   - [ ] Create MVCCRowIterator wrapper
   - [ ] Implement IsVisible checks for rows
   - [ ] Update StorageScanOperator
   - [ ] Add visibility tests
   - [ ] Update index scan operators

### Phase 4: Version Chain Management (2 days)
#### Goal: Properly handle multiple versions of rows

1. **Update Operations**
   - **INSERT**: Create new version with transaction ID
   - **UPDATE**: Create new version, link to old
   - **DELETE**: Mark current version as deleted
   - **SELECT**: Follow version chain to find visible version

2. **Version Storage**
   - Store all versions in pages
   - Use NextVersion pointer for chains
   - Consider page overflow for long chains

3. **Tasks**:
   - [ ] Implement version chain creation
   - [ ] Update UPDATE to create new versions
   - [ ] Modify DELETE to mark versions
   - [ ] Add version chain traversal logic
   - [ ] Handle page overflow scenarios

### Phase 5: Vacuum Process (3 days)
#### Goal: Clean up old invisible versions

1. **Implement Vacuum Worker**
   ```go
   type VacuumWorker struct {
       storage      *StorageBackend
       txnManager   *TransactionManager
       minActiveTxn int64
   }
   ```

2. **Cleanup Logic**
   - Track minimum active transaction
   - Identify dead versions (invisible to all)
   - Reclaim space in pages
   - Update version chains

3. **Scheduling**
   - Background vacuum process
   - Configurable thresholds
   - Manual VACUUM command

4. **Tasks**:
   - [ ] Create VacuumWorker structure
   - [ ] Implement dead tuple detection
   - [ ] Add space reclamation logic
   - [ ] Create VACUUM SQL command
   - [ ] Add vacuum scheduling
   - [ ] Implement statistics tracking

### Phase 6: Testing & Validation (2 days)
#### Goal: Ensure correctness of MVCC implementation

1. **Isolation Tests**
   - Test all isolation levels
   - Verify phantom reads, dirty reads
   - Check repeatable read behavior

2. **Concurrency Tests**
   - Multiple concurrent transactions
   - Stress test with conflicts
   - Performance benchmarks

3. **Recovery Tests**
   - Crash recovery with versions
   - WAL replay correctness
   - Version chain integrity

4. **Tasks**:
   - [ ] Create isolation level test suite
   - [ ] Add concurrent transaction tests
   - [ ] Implement crash recovery tests
   - [ ] Add performance benchmarks
   - [ ] Document test scenarios

## Technical Considerations

### Storage Format Changes
- Need migration path for existing data
- Consider version format versioning
- Plan for backward compatibility

### Performance Impact
- Additional metadata per row (~40 bytes)
- Visibility checks add CPU overhead
- Version chains may cause random I/O
- Vacuum process adds background load

### Memory Management
- Transaction snapshot caching
- Version chain traversal optimization
- Buffer pool considerations

## Success Metrics
- All isolation levels working correctly
- No data corruption under concurrent load
- Vacuum keeps database size stable
- Performance within 10% of non-MVCC

## Risk Mitigation
- Extensive testing at each phase
- Feature flags for gradual rollout
- Benchmarking after each phase
- Rollback plan if issues found

## Timeline
- **Total Duration**: ~14 days
- **Phase 1-2**: 4 days (Foundation)
- **Phase 3-4**: 5 days (Core MVCC)
- **Phase 5**: 3 days (Cleanup)
- **Phase 6**: 2 days (Validation)

## Dependencies
- Existing MVCC transaction manager
- Storage engine with WAL support
- Row serialization framework

## Next Steps
1. Review and approve plan
2. Create detailed tasks in issue tracker
3. Begin Phase 1 implementation
4. Daily progress updates