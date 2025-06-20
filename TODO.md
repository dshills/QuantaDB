# QuantaDB TODO List

## Summary
**Last Updated**: December 20, 2024
**Project Status**: MVCC transaction-storage integration in progress
**Recent Updates**: 
- Implemented MVCC row format with transaction metadata (Phase 1 complete)
- Added MVCCStorageBackend with visibility checks
- Fixed all critical concurrency and safety issues from code review
- Completed Phase 4: Version chain management with in-place updates
- See `docs/transaction-storage-integration-plan.md` for full integration plan

## Current Sprint (Q1 2025 - Phase 1: Performance Optimization)

### Medium Priority 🟡

#### 1. Transaction-Storage Full Integration
**Status**: Phase 1 complete - MVCC row format implemented
**Location**: `internal/txn/`, `internal/storage/`, `internal/sql/executor/`
**Completed** (Phase 1):
- [x] Enhanced row format with MVCC metadata
- [x] MVCCStorageBackend with atomic transaction ID handling
- [x] Timestamp-based visibility checks
- [x] Raw iterator for MVCC-aware scans
**Completed** (Phase 2):
- [x] Phase 2: Thread transaction context through operations
  - Updated ExecContext to use only MVCC transactions
  - Added SnapshotTS and IsolationLevel fields
  - Updated network handler to pass transaction context
  - Modified all DML operators to set transaction ID on storage
  - Removed legacy transaction usage
  - Updated server initialization to use MVCCStorageBackend
**Completed** (Phase 3):
- [x] Phase 3: Add visibility filtering to all scan operators
  - Updated StorageBackend interface to accept snapshot timestamps
  - Modified MVCCStorageBackend to use snapshot timestamps for visibility
  - Updated all scan operators to pass snapshot timestamp from context
  - Fixed timestamp generation to use logical timestamps consistently
  - Fixed isolation level handling for Read Committed vs Repeatable Read
  - All MVCC isolation tests now pass
**Completed** (Phase 4):
- [x] Phase 4: Implement version chain management
  - Refactored UpdateRow to use in-place updates with version backup
  - Created VersionChainIterator for traversing version chains
  - Updated GetRow to use version chain traversal
  - Added validation and statistics utilities
  - All version chain tests passing
**Completed** (Phase 5):
- [x] Phase 5: Create vacuum process for old versions
  - Created HorizonTracker for safe vacuum threshold management
  - Integrated horizon tracking with transaction manager
  - Implemented VacuumScanner for dead version detection
  - Built VacuumExecutor with batch processing and relinking
  - Added VACUUM SQL command with parser and operator support
  - Fixed vacuum to handle standalone dead versions
  - All vacuum tests passing successfully
**Remaining Tasks** (Phase 6):
- [ ] Phase 6: Comprehensive testing and validation
**New Issues Found**:
- [x] Fix transaction test API mismatches - BeginTransaction signature changed (DONE)
- [x] Fix TestVacuumWithConcurrentReads timeout issue (DONE)
- [x] Fix concurrent insert performance test - slice bounds error in raw iterator
- [x] Remove or deprecate non-MVCC ScanOperator that bypasses visibility checks
- [x] Extract timestamp helpers to shared utility package (low priority)
**Estimated Time**: 1-2 days remaining (Phase 6)
**Impact**: True ACID compliance with proper isolation

#### 2. Query Optimization Improvements
**Status**: Statistics collection complete, advanced optimizations pending
**Tasks**:
- [ ] Implement join reordering based on cost
- [ ] Enhance predicate pushdown for more cases
- [ ] Add histogram-based selectivity for range queries
**Estimated Time**: 5 days
**Impact**: Advanced query optimizations using real statistics

## Future Roadmap (from ROADMAP.md)

### Phase 2: Enterprise Features (Q2 2025)
- Authentication & user management
- Role-based access control
- Backup & recovery tools
- Monitoring & metrics
- Admin dashboard

### Phase 3: Advanced SQL (Q3 2025)
- Common Table Expressions (CTEs)
- Window functions
- Stored procedures & triggers
- JSON/JSONB support
- Array types

### Phase 4: Distributed Features (Q4 2025)
- Streaming replication
- Horizontal sharding
- Raft consensus
- Distributed transactions

## Quick Reference

### Build & Test
```bash
make build          # Build server and CLI
make test          # Run all tests
make test-coverage # Generate coverage report
golangci-lint run  # Run linter
```

### Test Storage Features
```sql
-- All these operations now work with persistence!
CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
UPDATE users SET email = 'newemail@example.com' WHERE id = 1;
DELETE FROM users WHERE id = 1;
SELECT * FROM users;
```

## Contributing

See CONTRIBUTING.md for guidelines. Priority areas:
- Performance optimizations
- PostgreSQL compatibility improvements  
- Test coverage expansion
- Documentation updates

## Recent Improvements ✨

### Vacuum Test Fixes (December 20, 2024)
- ✅ Fixed TestVacuumWithConcurrentReads timeout caused by infinite iterator loop
- ✅ Implemented proper transaction management in vacuum tests
- ✅ Added active read transaction protection to prevent premature vacuum
- ✅ Used actual RowIDs from InsertRow instead of hardcoded slot positions
- ✅ Set aggressive safety config for deterministic test behavior
- ✅ Fixed TestVacuumOperator by creating table in storage
- ✅ All vacuum tests now pass reliably with race detector

### Testing Infrastructure (December 20, 2024)
- ✅ Added unit tests for transaction context propagation
- ✅ Created concurrent transaction isolation tests
- ✅ Added thread-safe timestamp service to avoid direct NextTimestamp() calls
- ✅ Tests revealed critical isolation level bugs that need fixing

### Transaction Context Threading (December 20, 2024)
- ✅ Updated ExecContext to remove LegacyTxn and use only MVCC transactions
- ✅ Added SnapshotTS and IsolationLevel fields to ExecContext
- ✅ Modified connection handler to pass transaction context to all queries
- ✅ Updated all DML operators (INSERT, UPDATE, DELETE, SCAN) to use transaction ID
- ✅ Modified server initialization to use MVCCStorageBackend with transaction manager
- ✅ All tests pass with the new transaction-aware architecture

### MVCC Row Format Implementation (December 20, 2024)
- ✅ Created MVCCRowHeader with transaction metadata (IDs, timestamps, version pointers)
- ✅ Implemented MVCCRow with efficient serialization/deserialization
- ✅ Built MVCCStorageBackend extending DiskStorageBackend
- ✅ Added RawRowIterator for accessing raw row data
- ✅ Implemented timestamp-based visibility checks for proper MVCC semantics
- ✅ Fixed all concurrency issues with atomic operations
- ✅ Added comprehensive error handling with typed errors
- ✅ Optimized performance with pre-allocated buffers and cached formatters
- ✅ All tests pass with race detection enabled

---
*Last Updated: December 20, 2024*
*For completed features, see `docs/COMPLETED.md`*
*For detailed planning, see `docs/ROADMAP.md` and `docs/CURRENT_STATUS.md`*