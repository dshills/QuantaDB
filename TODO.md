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
- **Fixed Float32 Support** ✅
  - Added float32 cases to SUM/AVG aggregates
  - Added float32 comparison support in expressions
  - Fixed TPC-H queries with FLOAT columns
- **Implemented LIKE Operator** ✅
  - Added SQL LIKE pattern matching with % and _ wildcards
  - Enables Q14 (Promotion Effect) and other pattern-based queries
- **TPC-H Progress** 🚀
  - Successfully loaded complete TPC-H dataset (scale 0.01)
  - 10/22 queries working (45% coverage) ✅
  - Q1, Q3, Q4, Q5, Q6, Q10, Q12, Q13, Q14, Q19 all functional
  - Indexes provide significant performance improvements
  - SUBSTRING function verified working

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

**Current Status**: TPC-H at 45% coverage! 10/22 queries fully working. Key blockers: correlated subqueries, window functions.

### Immediate Priorities - Next TPC-H Queries

1. **Quick Wins (Should Work Now)** 🟢
   - Q7 (Volume Shipping) - Only needs complex joins
   - Q9 (Product Type Profit) - Complex expressions only
   - Q16 (Parts/Supplier) - NOT IN with subquery

2. **Completed Tasks** ✅
   - Q1 type issues fixed
   - Q3 optimized with indexes
   - Q5, Q10, Q12, Q19 tested and working
   - Q14 with LIKE operator complete

### Week 2 - Core SQL Features
1. **Table Alias Enhancement** 🔴 HIGH
   - Support multiple aliases for same table
   - Blocks Q8, Q21

2. **Correlated Subqueries in SELECT** 🔴 HIGH
   - Implement correlation resolution
   - Blocks Q2, Q17, Q20, Q21, Q22 (5 queries!)

3. **Statistical Functions** 🟡 MEDIUM
   - Implement STDDEV aggregate
   - Blocks Q17

### Week 3 - Advanced Features
1. **Window Functions Framework** 🟡 MEDIUM
   - Start with ROW_NUMBER() OVER
   - Blocks Q2, Q17, Q18, Q20

2. **Performance Infrastructure** 🟢 LOW
   - Add query plan caching
   - Implement hash joins
   - Better statistics collection

## 📋 Implementation Plans

See detailed plans in `docs/planning/`:
- **Overall Strategy**: `implementation-plan-dec-2024.md`
- **Phase 1 Technical Details**: `phase1-critical-fixes-plan.md`  
- **Quick Reference**: `implementation-checklist.md`

## Phase 7: Performance & Benchmarking

### TPC-H Benchmark Status
- [x] **TPC-H Infrastructure**: Complete benchmark framework
  - Schema definitions for all 8 TPC-H tables
  - Data generator with configurable scale factors (fixed DATE literal format)
  - 4 queries in codebase (Q3, Q5, Q8, Q10) - need testing
  - Benchmark runner with performance measurement
  - SQL loader utility for data import
  - Successfully loaded complete dataset at scale 0.01
- [ ] **Complete TPC-H Suite**: Implement all 22 queries

#### TPC-H Query Implementation Status
| Query | Name | Status | Blockers |
|-------|------|--------|----------|
| Q1 | Pricing Summary Report | ✅ Working | Type issues fixed |
| Q2 | Minimum Cost Supplier | ❌ Not Started | Correlated subquery, window functions |
| Q3 | Shipping Priority | ✅ Working | Optimized with indexes |
| Q4 | Order Priority Checking | ✅ Working | EXISTS subquery |
| Q5 | Local Supplier Volume | ✅ Working | 6-way join |
| Q6 | Forecasting Revenue Change | ✅ Working | None |
| Q7 | Volume Shipping | ❌ Not Started | Complex joins only |
| Q8 | National Market Share | 🕰️ Implemented | Multiple table aliases (nation n1, n2) |
| Q9 | Product Type Profit Measure | ❌ Not Started | Complex expressions only |
| Q10 | Returned Item Reporting | ✅ Working | GROUP BY multiple columns |
| Q11 | Important Stock Identification | ❌ Not Started | Correlated subquery in HAVING |
| Q12 | Shipping Modes and Order Priority | ✅ Working | IN operator, CASE |
| Q13 | Customer Distribution | ✅ Working | LEFT OUTER JOIN |
| Q14 | Promotion Effect | ✅ Working | LIKE operator |
| Q15 | Top Supplier Query | ❌ Not Started | Views or CTEs |
| Q16 | Parts/Supplier Relationship | ❌ Not Started | NOT IN with subquery |
| Q17 | Small-Quantity-Order Revenue | ❌ Not Started | Correlated subquery, STDDEV |
| Q18 | Large Volume Customer | ❌ Not Started | IN with subquery, window functions |
| Q19 | Discounted Revenue | ✅ Working | Complex OR conditions |
| Q20 | Potential Part Promotion | ❌ Not Started | Correlated subquery, EXISTS |
| Q21 | Suppliers Who Kept Orders Waiting | ❌ Not Started | Multiple correlated subqueries, table aliases |
| Q22 | Global Sales Opportunity | ❌ Not Started | Correlated subquery, SUBSTRING |

**Legend**: ✅ Working | ⚠️ Partial | 🕰️ Implemented but untested | ❌ Not Started

### Performance Infrastructure
- [ ] Performance regression detection framework
- [ ] Cost model validation and calibration
- [ ] Query plan comparison and analysis tools
- [ ] Automated benchmark CI/CD integration

### Advanced SQL Features for TPC-H Completion

#### High Priority (Blocks most queries)
- [ ] **Table Alias Enhancement**: Support same table multiple times
  - Required for Q8 (`nation n1, nation n2`), Q21
  - Basic aliases work ✅, but not multiple aliases for same table
- [ ] **Correlated Subqueries in SELECT**: Critical for 5 queries
  - Required for Q2, Q17, Q20, Q21, Q22
  - Example: `SELECT (SELECT AVG(x) FROM t2 WHERE t2.id = t1.id) FROM t1`
- [ ] **Statistical Aggregate Functions**
  - STDDEV() / STDDEV_POP() / STDDEV_SAMP() - Required for Q17
  - VARIANCE() / VAR_POP() / VAR_SAMP()
- [ ] **ALL/ANY/SOME Operators**: For advanced comparisons
  - Required for Q20, Q21
  - Example: `WHERE p_size = ANY (SELECT ...)`

#### Medium Priority (Enables remaining queries)
- [ ] **Window Functions Framework**: Required for 4 queries
  - ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) - Q2, Q17, Q18, Q20
  - RANK() OVER (...)
  - DENSE_RANK() OVER (...)
  - Running aggregates with OVER clause
- [ ] **Common Table Expressions (WITH clause)**
  - Simplifies complex queries like Q15
  - Non-recursive CTEs first
- [ ] **Views Support**
  - Alternative to CTEs for Q15
  - CREATE VIEW / DROP VIEW
- [ ] **String Functions**
  - SUBSTRING() - Required for Q22
  - String concatenation (||)
  - Already have: LIKE pattern matching ✅

#### Low Priority (Nice to have)
- [ ] **Set Operations**
  - UNION / UNION ALL
  - INTERSECT / EXCEPT
- [ ] **Advanced GROUP BY**
  - ROLLUP / CUBE / GROUPING SETS
  - Not required for TPC-H but useful
- [ ] **Additional Window Functions**
  - LAG() / LEAD()
  - FIRST_VALUE() / LAST_VALUE()
  - NTILE()

#### Already Implemented ✅
- [x] Basic subqueries (EXISTS, IN, NOT EXISTS, NOT IN)
- [x] Scalar subqueries in WHERE
- [x] CASE expressions (simple and searched)
- [x] EXTRACT function for dates
- [x] INTERVAL arithmetic
- [x] BETWEEN operator
- [x] Basic table aliases (e.g., `customer c`)
- [x] Derived tables in FROM clause
- [x] LIMIT/OFFSET
- [x] All basic aggregates (SUM, COUNT, AVG, MIN, MAX)
- [x] Arithmetic in aggregate expressions

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

