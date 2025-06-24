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
- Correlated subqueries implemented! ✅
- 95% TPC-H coverage (21/22 queries working) ✅
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
- **Fixed EXISTS correlation extraction in decorrelation** ✅
  - Decorrelation now properly extracts correlation predicates from subqueries
  - EXISTS/NOT EXISTS queries with correlation now return correct results
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
- **Implemented STDDEV Aggregate** ✅
  - Added population standard deviation aggregate function
  - Enables Q17 (partially - still needs correlated subqueries)
- **TPC-H Progress** 🚀
  - Successfully loaded complete TPC-H dataset (scale 0.01)
  - 21/22 queries working (95% coverage) ✅
  - Q1, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q16, Q19 all functional
  - Q2, Q17, Q22 now working with correlated subqueries! ✅
  - Q15, Q18, Q20 also working! ✅
  - Indexes provide significant performance improvements
  - HAVING clauses with aggregate expressions now working ✅
  - Non-correlated scalar subqueries in WHERE working ✅
  - Correlated subqueries (EXISTS/NOT EXISTS) working ✅
  - Correlated scalar subqueries in WHERE working ✅

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

**Current Status**: TPC-H at 95% coverage! 21/22 queries fully working. Only Q21 remaining.

### Immediate Priorities - Next TPC-H Queries

1. **Completed Tasks** ✅
   - Q1 type issues fixed
   - Q3 optimized with indexes
   - Q5, Q10, Q12, Q19 tested and working
   - Q14 with LIKE operator complete
   - Q9, Q16 implemented (complex expressions, NOT IN)
   - Q11 working (non-correlated subquery in HAVING)
   - STDDEV aggregate function implemented

### Week 2 - Core SQL Features
1. **Table Alias Enhancement** ✅ COMPLETED
   - Fixed cross product bug with multiple aliases
   - Q7 and Q8 now working

2. **Correlated Subqueries** ✅ COMPLETED
   - Implemented correlation resolution
   - EXISTS/NOT EXISTS with correlation working
   - Scalar correlated subqueries in WHERE working
   - Q2, Q17, Q22 now functional!

3. **Statistical Functions** ✅ COMPLETED
   - STDDEV aggregate implemented
   - Q17 still blocked by correlated subqueries

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
| Q2 | Minimum Cost Supplier | ✅ Working | Correlated subquery implemented |
| Q3 | Shipping Priority | ✅ Working | Optimized with indexes |
| Q4 | Order Priority Checking | ✅ Working | EXISTS subquery |
| Q5 | Local Supplier Volume | ✅ Working | 6-way join |
| Q6 | Forecasting Revenue Change | ✅ Working | None |
| Q7 | Volume Shipping | ✅ Working | Cross product bug fixed |
| Q8 | National Market Share | ✅ Working | Arithmetic operators fixed |
| Q9 | Product Type Profit Measure | ✅ Working | Complex expressions, 6-way join |
| Q10 | Returned Item Reporting | ✅ Working | GROUP BY multiple columns |
| Q11 | Important Stock Identification | ✅ Working | Subquery in HAVING (non-correlated) |
| Q12 | Shipping Modes and Order Priority | ✅ Working | IN operator, CASE |
| Q13 | Customer Distribution | ✅ Working | LEFT OUTER JOIN |
| Q14 | Promotion Effect | ✅ Working | LIKE operator |
| Q15 | Top Supplier Query | ✅ Working | Subqueries in FROM clause, scalar MAX |
| Q16 | Parts/Supplier Relationship | ✅ Working | NOT IN with subquery, COUNT DISTINCT |
| Q17 | Small-Quantity-Order Revenue | ✅ Working | Correlated subquery implemented |
| Q18 | Large Volume Customer | ✅ Working | IN with GROUP BY/HAVING subquery |
| Q19 | Discounted Revenue | ✅ Working | Complex OR conditions |
| Q20 | Potential Part Promotion | ✅ Working | Nested IN + correlated scalar subquery |
| Q21 | Suppliers Who Kept Orders Waiting | ❌ Not Started | Multiple correlated EXISTS/NOT EXISTS |
| Q22 | Global Sales Opportunity | ✅ Working | Correlated EXISTS, SUBSTRING implemented |

**Legend**: ✅ Working | ⚠️ Partial | 🕰️ Implemented but untested | ❌ Not Started

### Performance Infrastructure
- [ ] Performance regression detection framework
- [ ] Cost model validation and calibration
- [ ] Query plan comparison and analysis tools
- [ ] Automated benchmark CI/CD integration

### Advanced SQL Features for TPC-H Completion

#### High Priority (Blocks most queries)
- [x] **Table Alias Enhancement**: Support same table multiple times ✅
  - Fixed cross product bug that affected multiple aliases
  - Q7 and Q8 now working correctly
- [x] **Correlated Subqueries**: Implemented ✅
  - EXISTS/NOT EXISTS with correlation working
  - Scalar correlated subqueries in WHERE working
  - Q2, Q17, Q22 now functional
- [x] **Statistical Aggregate Functions**
  - STDDEV() - Implemented (population standard deviation) ✅
  - [ ] STDDEV_SAMP() - Sample standard deviation
  - [ ] VARIANCE() / VAR_POP() / VAR_SAMP()
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
- [x] **String Functions**
  - SUBSTRING() - Implemented ✅ 
  - String concatenation (||) - Implemented ✅
  - LIKE pattern matching - Implemented ✅

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
- [x] STDDEV aggregate function (population standard deviation)
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

