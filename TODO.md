# QuantaDB TODO List

## Project Status Summary

**Phase 1-6: COMPLETED** ✅
- Core Storage Engine with MVCC, WAL, and B+Tree Indexes
- Full SQL Parser with comprehensive statement support
- Query Planner with optimization framework
- Query Executor with all major operators
- PostgreSQL Wire Protocol compatibility
- All core SQL data types implemented
- TPC-H benchmark infrastructure ready

**Current Phase: Critical Bug Fixes**
- Storage type mismatches (int32 vs int64) blocking basic operations
- GROUP BY crashes preventing aggregate queries
- JOIN column resolution errors
- See `TODO-DETAILED.md` for comprehensive task list

**Status Update (December 2024)**
- Fixed all linting issues (97 → 0)
- Fixed optimizer subquery decorrelation
- Multiple critical issues blocking TPC-H benchmarks

**Key Achievements:**
- ✅ PostgreSQL-compatible database from scratch
- ✅ ACID transactions with MVCC isolation
- ✅ Write-Ahead Logging with crash recovery
- ✅ B+Tree indexes (created but not used in queries yet)
- ✅ Cost-based query optimizer framework
- ✅ Prepared statements and parameterized queries
- ✅ Full JOIN support (INNER, LEFT, RIGHT, FULL, CROSS)
- ✅ Aggregate functions with GROUP BY/HAVING
- ✅ Subqueries, IN/EXISTS, and CTEs
- ✅ Foreign Keys and CHECK constraints
- ✅ COPY protocol for bulk data loading

## Phase 6: Data Types & Advanced Features (IN PROGRESS)

### Data Types
- [ ] **BYTEA**: Binary data type (only remaining core type)

## 🚨 CRITICAL: Fix Server Crashes First!

**Current Status**: TPC-H benchmark reveals critical bugs that prevent basic queries from working.

### Immediate Priorities (Week 1-2)
1. **Fix GROUP BY Server Crash** 🔥
   - Server crashes with SIGSEGV on any GROUP BY query
   - Blocking all aggregate testing
   - See: `docs/planning/phase1-critical-fixes-plan.md`

2. **Fix JOIN Column Resolution** 🔥 
   - "column c_custkey not resolved" errors in JOIN queries
   - Affects most TPC-H queries
   - Root cause: Column resolver not handling qualified names

3. **Support Aggregate Expressions in Projection** 🔥
   - "unsupported expression type: *planner.AggregateExpr"
   - Needed for expressions like SUM(a)/SUM(b)
   - TPC-H Q8 fails due to this

### Next Steps (Week 3-4)  
4. **Add DISTINCT Support**
   - Required by several TPC-H queries
   - Currently: "unexpected token in expression: DISTINCT"

5. **Implement LIMIT/OFFSET**
   - TPC-H Q18 uses LIMIT 100
   - Basic SQL feature gap

6. **Complete BYTEA Data Type**
   - Only remaining core PostgreSQL data type

### After Critical Fixes (Week 5-7)
7. **Index-Query Integration**
   - Indexes are created but not used by query planner
   - Implement IndexScanOperator and cost estimation
   - Update optimizer to choose index scans when beneficial
   - **Prerequisite**: Basic queries must work first

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
- [ ] **LIMIT/OFFSET**: Q18 uses LIMIT 100
- [ ] **Additional Aggregate Functions**: STDDEV (Q17)
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

