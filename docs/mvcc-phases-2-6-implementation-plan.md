# MVCC Phases 2-6 Implementation Plan

## Overview
This document provides a detailed implementation plan for completing the MVCC transaction-storage integration in QuantaDB. Phase 1 (MVCC row format) is complete. This plan covers the remaining phases.

## Current State (After Phase 1)
✅ **Completed**:
- MVCCRowHeader with transaction metadata
- MVCCRow serialization/deserialization
- MVCCStorageBackend with basic operations
- Timestamp-based visibility checks
- Thread-safe transaction ID handling

❌ **Missing**:
- Transaction context not properly threaded through query execution
- Visibility checks not applied in all read paths
- No version chain management for updates
- No garbage collection of old versions
- Limited testing of concurrent scenarios

## Phase 2: Thread Transaction Context (2 days)

### Goal
Properly propagate transaction context through all database operations, from network layer to storage.

### Implementation Tasks

#### Day 1: Update ExecContext and Operations
1. **Modify ExecContext** (`internal/sql/executor/exec_context.go`)
   ```go
   type ExecContext struct {
       // Remove LegacyTxn, use only MVCC transaction
       Txn *txn.MvccTransaction
       // Add snapshot timestamp for consistent reads
       SnapshotTS int64
       // Add isolation level
       IsolationLevel txn.IsolationLevel
   }
   ```

2. **Update Network Handler** (`internal/network/handler.go`)
   - Begin transaction when receiving BEGIN command
   - Store transaction in connection state
   - Pass transaction to executor for all queries

3. **Update Query Executor** (`internal/sql/executor/executor.go`)
   - Extract transaction from context
   - Set transaction ID in storage backend
   - Use snapshot timestamp for reads

4. **Update All DML Operations**
   - `InsertOperator`: Use transaction from context
   - `UpdateOperator`: Create version chains
   - `DeleteOperator`: Mark as deleted (not physical delete)
   - `ScanOperator`: Apply visibility checks

#### Day 2: Integration and Testing
1. **Remove Legacy Transaction Usage**
   - Search for all uses of `engine.Transaction`
   - Replace with MVCC transaction
   - Update tests

2. **Add Transaction Flow Tests**
   - Test transaction propagation from network to storage
   - Test isolation between concurrent transactions
   - Test rollback behavior

### Deliverables
- [ ] Updated ExecContext with single transaction type
- [ ] Network handler with transaction management
- [ ] All operators using MVCC transactions
- [ ] Comprehensive transaction flow tests

## Phase 3: Visibility Filtering (3 days)

### Goal
Implement visibility checks in all read operations to ensure proper isolation.

### Implementation Tasks

#### Day 1: Update Scan Operations
1. **Enhance StorageScanOperator** (`internal/sql/executor/scan.go`)
   ```go
   func (s *StorageScanOperator) Next() (*Row, error) {
       for s.iterator.Next() {
           row, rowID, err := s.iterator.Value()
           if err != nil {
               return nil, err
           }
           
           // Check MVCC visibility
           if s.isVisible(row, s.ctx.SnapshotTS) {
               return row, nil
           }
       }
       return nil, nil // EOF
   }
   ```

2. **Update Index Scan** (`internal/sql/executor/index_scan.go`)
   - Add visibility checks after index lookup
   - Handle version chains in index entries

3. **Create Visibility Helper**
   ```go
   type VisibilityChecker struct {
       txnID      int64
       snapshotTS int64
       isolation  IsolationLevel
   }
   
   func (v *VisibilityChecker) IsVisible(row *MVCCRow) bool {
       // Implement isolation-specific visibility rules
   }
   ```

#### Day 2: Update Join and Aggregate Operations
1. **Update Join Operators**
   - Hash Join: Filter during build and probe phases
   - Nested Loop Join: Check visibility for both sides
   - Sort-Merge Join: Filter during merge

2. **Update Aggregate Operators**
   - Filter invisible rows before aggregation
   - Handle GROUP BY with visibility

3. **Update Sort Operator**
   - Only sort visible rows
   - Maintain row metadata through sort

#### Day 3: Isolation Level Support
1. **Implement Read Uncommitted**
   - See all versions (even uncommitted)
   
2. **Implement Read Committed**
   - See only committed versions
   - New snapshot for each statement

3. **Implement Repeatable Read**
   - Fixed snapshot for entire transaction
   - Prevent phantom reads

4. **Implement Serializable**
   - Track read/write sets
   - Detect conflicts

### Deliverables
- [ ] All scan operators with visibility filtering
- [ ] Join operators respecting visibility
- [ ] Aggregate operators with proper filtering
- [ ] All isolation levels working correctly
- [ ] Isolation level tests

## Phase 4: Version Chain Management (2 days)

### Goal
Properly handle multiple versions of rows with update chains.

### Implementation Tasks

#### Day 1: Version Chain Creation
1. **Enhance Update Operation**
   ```go
   func (m *MVCCStorageBackend) UpdateRow(tableID int64, rowID RowID, newData *Row) error {
       // 1. Get current version
       currentVersion, err := m.GetMVCCRow(tableID, rowID)
       
       // 2. Create new version
       newVersion := &MVCCRow{
           Header: MVCCRowHeader{
               CreatedByTxn: m.currentTxnID,
               CreatedAt:    time.Now().Unix(),
               PrevVersion:  rowID, // Link to current
           },
           Data: newData,
       }
       
       // 3. Insert new version
       newRowID, err := m.insertVersion(tableID, newVersion)
       
       // 4. Update current version to point to new
       currentVersion.Header.NextVersion = newRowID
       m.updateInPlace(tableID, rowID, currentVersion)
       
       // 5. Mark current as deleted by this transaction
       currentVersion.Header.DeletedByTxn = m.currentTxnID
       currentVersion.Header.DeletedAt = time.Now().Unix()
   }
   ```

2. **Version Chain Traversal**
   ```go
   func (m *MVCCStorageBackend) findVisibleVersion(tableID int64, rowID RowID, snapshot int64) (*MVCCRow, RowID, error) {
       current := rowID
       
       for current != InvalidRowID {
           row, err := m.GetMVCCRow(tableID, current)
           if err != nil {
               return nil, InvalidRowID, err
           }
           
           if m.isVisible(row, snapshot) {
               return row, current, nil
           }
           
           // Move to next version
           current = row.Header.NextVersion
       }
       
       return nil, InvalidRowID, ErrNoVisibleVersion
   }
   ```

3. **Handle Long Version Chains**
   - Implement chain following with max depth
   - Consider caching frequently accessed versions
   - Add metrics for chain length

#### Day 2: Index Integration
1. **Update Index Entries**
   - Store all version pointers in index
   - Or store only latest version and follow chain

2. **Handle Unique Constraints**
   - Check visibility when enforcing uniqueness
   - Allow multiple "deleted" versions

3. **Update Statistics**
   - Count only visible rows
   - Track version chain statistics

### Deliverables
- [ ] Version chain creation on updates
- [ ] Version chain traversal with visibility
- [ ] Index integration with versions
- [ ] Unique constraint handling
- [ ] Version chain tests

## Phase 5: Vacuum Process (3 days)

### Goal
Clean up old versions that are no longer visible to any transaction.

### Implementation Tasks

#### Day 1: Vacuum Worker Design
1. **Create Vacuum Worker**
   ```go
   type VacuumWorker struct {
       storage    *MVCCStorageBackend
       txnManager *txn.Manager
       config     VacuumConfig
   }
   
   type VacuumConfig struct {
       MinDeadTuples   int
       MaxPagesPerRun  int
       SleepDuration   time.Duration
       FreezeAge       int64
   }
   ```

2. **Dead Tuple Detection**
   ```go
   func (v *VacuumWorker) isDeadTuple(row *MVCCRow) bool {
       // Get minimum active transaction
       minActiveTxn := v.txnManager.GetMinActiveTransaction()
       
       // Row is dead if:
       // 1. Deleted before min active transaction
       // 2. No active transaction can see it
       
       if row.Header.DeletedByTxn > 0 && 
          row.Header.DeletedByTxn < minActiveTxn {
           return true
       }
       
       return false
   }
   ```

3. **Free Space Management**
   - Track free space in pages
   - Compact pages when possible
   - Update free space map

#### Day 2: Vacuum Implementation
1. **Table Vacuum**
   ```go
   func (v *VacuumWorker) VacuumTable(tableID int64) error {
       // 1. Scan all pages
       // 2. Identify dead tuples
       // 3. Remove dead tuples
       // 4. Update indexes
       // 5. Update statistics
   }
   ```

2. **Page Compaction**
   ```go
   func (v *VacuumWorker) CompactPage(page *Page) {
       // 1. Identify live tuples
       // 2. Move tuples to compact space
       // 3. Update slot directory
       // 4. Update free space
   }
   ```

3. **Index Cleanup**
   - Remove index entries for dead tuples
   - Update index statistics

#### Day 3: Vacuum Scheduling and Commands
1. **Background Vacuum**
   ```go
   func (v *VacuumWorker) Start() {
       go func() {
           for {
               tables := v.getTablesNeedingVacuum()
               for _, table := range tables {
                   v.VacuumTable(table.ID)
               }
               time.Sleep(v.config.SleepDuration)
           }
       }()
   }
   ```

2. **VACUUM SQL Command**
   - `VACUUM`: Vacuum all tables
   - `VACUUM table_name`: Vacuum specific table
   - `VACUUM FULL`: Compact storage

3. **Autovacuum Tuning**
   - Track table statistics
   - Trigger based on dead tuple percentage
   - Avoid vacuuming during high load

### Deliverables
- [ ] VacuumWorker implementation
- [ ] Dead tuple detection algorithm
- [ ] Page compaction logic
- [ ] Background vacuum process
- [ ] VACUUM SQL command
- [ ] Vacuum tests and benchmarks

## Phase 6: Testing & Validation (2 days)

### Goal
Ensure the MVCC implementation is correct and performant.

### Implementation Tasks

#### Day 1: Correctness Testing
1. **Isolation Level Tests**
   ```go
   func TestReadCommitted(t *testing.T) {
       // Test dirty reads prevention
       // Test non-repeatable reads allowed
       // Test phantom reads allowed
   }
   
   func TestRepeatableRead(t *testing.T) {
       // Test dirty reads prevention
       // Test non-repeatable reads prevention
       // Test phantom reads allowed
   }
   
   func TestSerializable(t *testing.T) {
       // Test all anomalies prevented
       // Test write skew prevention
   }
   ```

2. **Concurrent Transaction Tests**
   - Multiple readers and writers
   - Deadlock detection
   - Long-running transactions
   - Transaction abort/rollback

3. **Version Chain Tests**
   - Long update chains
   - Branching chains
   - Chain traversal performance

#### Day 2: Performance Testing
1. **Benchmark Suite**
   ```go
   func BenchmarkMVCCInsert(b *testing.B)
   func BenchmarkMVCCUpdate(b *testing.B)
   func BenchmarkMVCCSelect(b *testing.B)
   func BenchmarkConcurrentTransactions(b *testing.B)
   func BenchmarkVacuum(b *testing.B)
   ```

2. **Stress Tests**
   - High concurrency scenarios
   - Large transactions
   - Many versions per row
   - Vacuum under load

3. **Performance Metrics**
   - Transaction throughput
   - Query latency
   - Version chain length
   - Vacuum effectiveness

### Deliverables
- [ ] Comprehensive isolation level tests
- [ ] Concurrent transaction test suite
- [ ] Performance benchmark suite
- [ ] Stress test scenarios
- [ ] Performance analysis report

## Implementation Schedule

| Phase | Duration | Start Date | End Date | Dependencies |
|-------|----------|------------|----------|--------------|
| Phase 2 | 2 days | Day 1-2 | Day 2 | Phase 1 ✅ |
| Phase 3 | 3 days | Day 3-5 | Day 5 | Phase 2 |
| Phase 4 | 2 days | Day 6-7 | Day 7 | Phase 3 |
| Phase 5 | 3 days | Day 8-10 | Day 10 | Phase 4 |
| Phase 6 | 2 days | Day 11-12 | Day 12 | All phases |

**Total Duration**: 12 days

## Risk Mitigation

### Technical Risks
1. **Performance Degradation**
   - Mitigation: Benchmark after each phase
   - Fallback: Optimize hot paths

2. **Compatibility Issues**
   - Mitigation: Test with PostgreSQL drivers
   - Fallback: Add compatibility layer

3. **Concurrency Bugs**
   - Mitigation: Race detector in all tests
   - Fallback: Additional synchronization

### Implementation Risks
1. **Scope Creep**
   - Mitigation: Strict phase boundaries
   - Fallback: Defer nice-to-haves

2. **Integration Complexity**
   - Mitigation: Incremental integration
   - Fallback: Simplify design

## Success Criteria

### Functional
- All isolation levels working correctly
- No data corruption under concurrent load
- Vacuum keeps database size stable
- All existing tests still pass

### Performance
- Less than 20% overhead vs non-MVCC
- Sub-millisecond visibility checks
- Vacuum completes in reasonable time
- No memory leaks

### Quality
- 90%+ test coverage for MVCC code
- All race conditions eliminated
- Clear documentation
- Clean code structure

## Next Steps
1. Review and approve this plan
2. Set up development branch
3. Begin Phase 2 implementation
4. Daily progress tracking
5. Phase completion reviews