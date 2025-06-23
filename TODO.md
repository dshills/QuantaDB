# QuantaDB TODO List

## Project Status Summary

**Phase 1-6: COMPLETED** ✅
- Core Storage Engine with MVCC, WAL, and B+Tree Indexes
- Full SQL Parser with comprehensive statement support
- Query Planner with optimization framework
- Query Executor with all major operators
- PostgreSQL Wire Protocol compatibility
- All core SQL data types implemented (including BYTEA)
- TPC-H benchmark infrastructure ready

**Current Phase: Feature Completion**
- All critical crashes have been fixed ✅
- Ready for TPC-H benchmarks and feature additions
- See `TODO-DETAILED.md` for comprehensive task list

**Status Update (December 2024)**
- Fixed all linting issues (97 → 0) ✅
- Fixed optimizer subquery decorrelation ✅
- Fixed JOIN column resolution for qualified names ✅
- Fixed aggregate expressions in projections ✅
- Fixed GROUP BY server crash ✅
- Implemented DISTINCT support ✅
- Implemented BYTEA binary data type ✅
- Fixed column resolution in filter predicates ✅
- Fixed date/time deserialization for proper arithmetic ✅
- Added interval type support in planner ✅
- LIMIT/OFFSET already implemented ✅
- Implemented CASCADE DELETE for foreign keys ✅
- Implemented full CHECK constraint expression parsing ✅
- **Fixed Index-Query Integration** ✅
  - INSERT/UPDATE/DELETE now maintain indexes
  - CREATE INDEX populates existing data
  - Query planner already uses indexes for optimization
- **Fixed Date/Time Indexing** ✅
  - B+Tree indexes now support DATE and TIMESTAMP columns
  - Enables TPC-H data loading with date column indexes
- All critical blockers resolved - ready for TPC-H!

**Key Achievements:**
- ✅ PostgreSQL-compatible database from scratch
- ✅ ACID transactions with MVCC isolation
- ✅ Write-Ahead Logging with crash recovery
- ✅ B+Tree indexes with full query integration
- ✅ Cost-based query optimizer framework
- ✅ Prepared statements and parameterized queries
- ✅ Full JOIN support (INNER, LEFT, RIGHT, FULL, CROSS)
- ✅ Aggregate functions with GROUP BY/HAVING
- ✅ Subqueries, IN/EXISTS, and CTEs
- ✅ Foreign Keys with CASCADE DELETE, SET NULL, SET DEFAULT
- ✅ CHECK constraints with full expression support
- ✅ COPY protocol for bulk data loading
- ✅ BYTEA binary data type with PostgreSQL compatibility

## Phase 6: Data Types & Advanced Features (COMPLETED) ✅

### Data Types
- [x] **BYTEA**: Binary data type (PostgreSQL-compatible implementation complete)

## Next Priority Items

**Current Status**: All core data types implemented! Major crashes fixed! GROUP BY, JOIN resolution, aggregate expressions, and DISTINCT now work.

### Immediate Priorities (Week 1)  
1. **CASCADE DELETE and CHECK Constraints** ✅ COMPLETED
   - CASCADE DELETE action for foreign keys ✅
   - SET NULL and SET DEFAULT actions ✅
   - CHECK constraint expression parsing with full support ✅
   - Expression caching for performance ✅
   - Support for complex expressions, functions, operators ✅

### After Critical Fixes (Week 2-3)
2. **Index-Query Integration** ✅ COMPLETED
   - Indexes are now properly maintained during DML operations ✅
   - CREATE INDEX populates existing data ✅
   - Query planner already had index scan operators ✅
   - Cost-based optimization for index selection already exists ✅

## 📋 Implementation Plans

See detailed plans in `docs/planning/`:
- **Overall Strategy**: `implementation-plan-dec-2024.md`
- **Phase 1 Technical Details**: `phase1-critical-fixes-plan.md`  
- **Quick Reference**: `implementation-checklist.md`

## Phase 7: Performance & Benchmarking

### TPC-H Benchmark Status
- [x] **TPC-H Infrastructure**: Complete benchmark framework
  - Schema definitions for all 8 TPC-H tables
  - Data generator with configurable scale factors
  - 4 implemented queries (Q3, Q5, Q8, Q10)
  - Benchmark runner with performance measurement
  - SQL loader utility for data import
- [ ] **Complete TPC-H Suite**: Implement remaining 18 queries
  - Currently 4/22 queries implemented
  - Need to add Q1, Q2, Q4, Q6, Q7, Q9, Q11-Q22
  - Some queries require additional SQL features (see below)

### Performance Infrastructure
- [ ] Performance regression detection framework
- [ ] Cost model validation and calibration
- [ ] Query plan comparison and analysis tools
- [ ] Automated benchmark CI/CD integration

### SQL Features for Remaining TPC-H Queries
- [ ] **Window Functions**: Required for Q2 (rank), Q17, Q18, Q20
- [ ] **Correlated Subqueries in SELECT**: Q2, Q17, Q20, Q21, Q22
- [ ] **Multiple Subqueries**: Q21, Q22 have complex nested subqueries
- [x] **LIMIT/OFFSET**: Already implemented - Q18 can use LIMIT 100
- [ ] **Additional Aggregate Functions**: STDDEV (Q17)
- [x] **EXTRACT Function**: Already implemented for date/time operations (Q8)
- [x] **CASE Expressions**: Already implemented for conditional logic (Q8)
- [ ] **Table Aliases**: Q8 needs support for `nation n1, nation n2` syntax
- [ ] **Query Optimization**: Many queries need better join ordering and index usage

## Technical Debt & Architecture Improvements

### High Priority
- [ ] **Error Handling**: Replace string-based error matching in connection.go with proper error types

### Medium Priority
- [ ] **CLI Tools**: Implement functionality for `cmd/quantactl` and `cmd/test-client` (see TODO.md files in those directories)
- [ ] **Integration Tests**: Add comprehensive integration tests for disk-backed storage
- [ ] **Extended Query Protocol Tests**: Add tests for timeout, SSL, and protocol error scenarios
- [ ] **Module Decoupling**: Reduce tight coupling between network, parser, planner, executor, and storage modules

### Low Priority
- [ ] **In-Memory Engine Concurrency**: Replace single RWMutex with more granular locking for better scalability
- [ ] **Buffer Pool Eviction Policy**: Add configurable eviction policy to handle memory pressure
- [ ] **Connection Backpressure**: Add queuing mechanism for connection handling beyond MaxConnections
- [ ] **Parallel Abstractions**: Consolidate in-memory engine and MVCC storage backend abstractions

## Future Roadmap (from ROADMAP.md)

### Enterprise Features
- Authentication & user management
- Role-based access control
- Backup & recovery tools
- Monitoring & metrics
- Admin dashboard

### Advanced SQL
- Common Table Expressions (CTEs)
- Window functions
- Stored procedures & triggers
- JSON/JSONB support
- Array types

### Distributed Features
- Streaming replication
- Horizontal sharding
- Raft consensus
- Distributed transactions

