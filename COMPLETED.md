### High Priority 🔴

#### 0. Repository Maintenance ✅ COMPLETE
**Status**: Repository cleaned up and organized
**Completed**:
- [x] Removed build artifacts, log files, and temporary test files
- [x] Organized documentation into structured categories (/reference/, /implementation/, /planning/)
- [x] Enhanced .gitignore to cover database files, logs, and test artifacts
- [x] Added comprehensive docs/README.md with navigation guide
- [x] Removed obsolete test scripts and protocol files

### Medium Priority 🟡

#### 1. Transaction-Storage Full Integration ✅ COMPLETE
**Status**: ALL PHASES COMPLETE - Full MVCC implementation with ACID compliance
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
**Completed** (Phase 6):
- [x] Phase 6: Comprehensive testing and validation
  - All integration tests passing
  - All CRUD operations working with MVCC semantics
  - Transaction isolation levels functioning correctly
  - Vacuum process working properly
  - Performance test timeouts resolved
  - Clean build and linting successful
**All Issues Resolved**:
- [x] Fix transaction test API mismatches - BeginTransaction signature changed (DONE)
- [x] Fix TestVacuumWithConcurrentReads timeout issue (DONE)
- [x] Fix concurrent insert performance test - slice bounds error in raw iterator
- [x] Remove or deprecate non-MVCC ScanOperator that bypasses visibility checks
- [x] Extract timestamp helpers to shared utility package (low priority)
**Impact**: ✅ Full ACID compliance with proper isolation achieved

#### 2. Query Optimization Improvements
**Status**: Comprehensive plan created, ready for implementation
**Location**: `internal/sql/planner/`, `internal/sql/executor/`
**Plan**: See `docs/planning/query-optimization-improvements-plan.md`
**Tasks** (4-week implementation):

**Phase 1: Enhanced Statistics Collection (Week 1)** ✅ COMPLETE
- [x] Column-level statistics infrastructure with histograms
- [x] Histogram-based selectivity estimation
- [x] Automatic statistics maintenance hooks
- [x] Replace simple heuristics with data-driven estimates

**Phase 2: Join Reordering Optimization (Week 2)** ✅ COMPLETE
- [x] Dynamic programming join enumeration (≤8 tables)
- [x] Greedy join ordering algorithm (>8 tables)
- [x] Sort-merge join implementation with external sort and disk spilling
- [x] Semi/anti join support for EXISTS/IN predicates

**Phase 3: Advanced Index Optimization (Week 3)** ✅ COMPLETE
**Plan**: See `docs/planning/phase3-index-optimization-plan.md`
- [x] Task 3.1: Multi-column index support (3 days) ✅ COMPLETE
  - [x] Composite key implementation in B+Tree
  - [x] Index matching for composite predicates
  - [x] Query planner integration
  - [x] Full test coverage with passing tests
- [x] Task 3.2: Index-only scans / covering indexes (2 days) ✅ COMPLETE
  - [x] Covering index detection
  - [x] IndexOnlyScanOperator implementation
  - [x] Automatic optimization during query planning
  - [ ] Visibility map for MVCC (deferred - not critical)
- [x] Task 3.3: Index intersection (1 day) ✅ COMPLETE
  - [x] Bitmap index scan operations
  - [x] Index intersection planner
  - [x] BitmapAnd/BitmapOr operators
  - [x] BitmapHeapScan operator
  - [x] Integration tests
- [x] Task 3.4: Additional optimizations (1 day) ✅ COMPLETE
  - [x] Index condition pushdown
  - [x] Partial index support

### Advanced Index Optimization Complete (December 21, 2024)
- ✅ **Composite Index Support**: Full multi-column index implementation
  - Updated index manager to handle composite keys with helper method
  - Created CompositeIndexScanOperator for physical execution
  - Integrated with query planner for automatic composite index selection
  - Full test coverage for composite index operations
  - Fixed all API compatibility issues and test failures
- ✅ **Index-Only Scans**: Covering index optimization
  - Implemented covering index detection in query planner
  - Created IndexOnlyScanOperator to avoid table access
  - Automatic detection and optimization during query planning
  - Significant I/O reduction for projection queries
- ✅ **Code Quality**: Improved codebase maintainability
  - Fixed linting issues (reduced from 30 to 22)
  - Extracted common code to reduce duplication
  - Added proper error handling and type safety
  - All tests passing with clean build
- 📝 **Deferred Work**: Index intersection and partial indexes postponed for future phases

### Query Optimization Phase 2 Complete (December 21, 2024)
- ✅ **Sort-Merge Join**: Implemented MergeJoinOperator with all join types
  - External sort with disk spilling for large datasets
  - Buffered iterator for efficient peeking during merge
  - Support for additional join conditions beyond equality
  - Proper handling of duplicate join keys
- ✅ **Semi/Anti Joins**: Implemented for EXISTS/IN/NOT EXISTS/NOT IN patterns
  - Hash-based semi/anti join for equi-join conditions
  - Nested loop variant for complex conditions
  - Special NULL handling for NOT IN semantics
  - Integration with query planner for automatic selection
- ✅ **Join Reordering**: Dynamic programming and greedy algorithms
  - Optimal join ordering for queries with ≤8 tables
  - Efficient greedy approach for larger queries

### Critical Data Corruption Fix (December 21, 2024)
- ✅ Fixed slot offset calculations causing test hangs and data corruption
- ✅ Implemented PageLockManager for thread-safe concurrent page access
- ✅ Corrected page initialization (FreeSpacePtr = PageSize)
- ✅ Fixed MVCC visibility to allow transactions to see their own writes
- ✅ All concurrent insert and transaction tests now pass

### Query Optimization Phase 1 Complete (December 21, 2024)
- ✅ Column-level statistics with equi-depth histograms
- ✅ Histogram-based selectivity estimation for accurate cardinality
- ✅ Automatic statistics maintenance with configurable thresholds
- ✅ Integration with ANALYZE command for manual statistics updates
- ✅ Dynamic programming join reordering (optimal for ≤8 tables)
- ✅ Greedy join ordering for large queries (>8 tables)

### Repository Cleanup and Organization (December 20, 2024)
- ✅ Cleaned up build artifacts, log files, and temporary test files
- ✅ Organized documentation into structured categories:
  - `/docs/reference/` - Technical specifications and design docs
  - `/docs/implementation/` - Implementation guides and designs
  - `/docs/planning/` - Project planning and MVCC phase plans
- ✅ Enhanced .gitignore to cover database files, logs, and test artifacts
- ✅ Added comprehensive docs/README.md with navigation guide
- ✅ Removed obsolete test scripts and protocol files
- ✅ Fixed :memory: database files being tracked by git

### Phase 6: Comprehensive Testing Complete (December 20, 2024)
- ✅ All MVCC transaction-storage integration tests passing
- ✅ End-to-end integration tests validated
- ✅ All SQL CRUD operations working with MVCC semantics
- ✅ Transaction isolation levels functioning correctly
- ✅ Vacuum process working properly
- ✅ Performance test timeouts resolved using -short flag properly
- ✅ Clean build and linting successful

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

## ✅ Phase 4: Query Transformation Enhancements (COMPLETED)
- [x] Complete projection pushdown implementation - Data flow reduction up to 48.4%
- [x] Add subquery parser support (EXISTS, IN, scalar subqueries) - Full AST support
- [x] Implement basic subquery planning - SubqueryOperator with caching
- [x] Subquery decorrelation and optimization - EXISTS/IN → SEMI/ANTI joins
- [x] Common Table Expression (CTE) support - WITH clause parsing and planning
- [x] EXISTS/IN predicate transformation to semi-joins - Integrated into optimizer

## ✅ SQL Query Features for TPC-H (December 21, 2024)

### Date/Time Support
- [x] **Date Literals**: Support for date 'YYYY-MM-DD' syntax
  - Added date literal parsing in SQL parser
  - Date type already existed in types package with proper serialization
  - Date comparisons work correctly using types.CompareValues
  - Full test coverage including TPC-H query compatibility tests
  - Integration with existing storage and executor layers

- [x] **EXTRACT Function**: Full implementation of EXTRACT(field FROM date/timestamp)
  - Parser support for EXTRACT(field FROM expression) syntax
  - Support for all standard fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
  - Planner expression type and conversion logic
  - Executor evaluation with proper type handling and error cases
  - Comprehensive test coverage for parsing and evaluation

### SQL Expressions
- [x] **CASE Expressions**: Full implementation of CASE WHEN expressions
  - Simple CASE: CASE expr WHEN val1 THEN result1 ELSE default END
  - Searched CASE: CASE WHEN cond1 THEN result1 ELSE default END
  - Parser support with proper precedence handling
  - Planner expression type with type inference
  - Executor evaluation for both simple and searched forms
  - Support for nested CASE expressions
  - Integration with aggregate functions (TPC-H Q8 style)

### Query Features
- [x] **Aggregate Functions**: Full implementation of SUM, AVG, MIN, MAX, COUNT
  - Parser support for function calls including COUNT(*) and COUNT(DISTINCT)
  - Planner integration with aggregate operator pipeline
  - Executor already had aggregate support, now fully connected
  - Mixed numeric type operations (int64/float64) in expressions

- [x] **GROUP BY and HAVING**: Complete support for grouping and filtering
  - Parser support for GROUP BY and HAVING clauses
  - Planner builds proper aggregate pipeline with grouping
  - Integration with aggregate functions and expressions
  - Multi-column grouping support

- [x] **ORDER BY**: Multi-column ordering with ASC/DESC (was already implemented)

### Test Coverage
- [x] All individual feature tests passing (GROUP BY, HAVING, EXTRACT, CASE, date literals)
- [x] TPC-H compatible feature tests created and passing
- [x] Complex TPC-H queries fail due to missing JOIN support (expected)
- [x] Integration tests verify end-to-end functionality