# QuantaDB Detailed TODO List
Last Updated: December 2024

## 🔥 CRITICAL: Immediate Fixes (Blocking Basic Functionality)

### 1. Storage Type Mismatch Issue [HIGH PRIORITY]
**Problem**: int32 vs int64 serialization causing widespread test failures
**Symptoms**: 
- "expected int32, got int64" errors in multiple tests
- Affects: INSERT operations, foreign keys, transactions
**Files to Fix**:
- `internal/storage/page.go` - SerializeRow function
- `internal/sql/executor/insert.go` - Type conversion logic
- `internal/sql/types/types.go` - Value type handling
**Solution**: Standardize all integer handling to int64 throughout storage layer

### 2. GROUP BY Server Crash [HIGH PRIORITY]
**Problem**: Server crashes with SIGSEGV on GROUP BY queries
**Symptoms**: 
- Simple COUNT(*) works, but GROUP BY crashes
- Blocking all aggregate query testing
**Files to Fix**:
- `internal/sql/executor/group_by.go` - Add nil checks and proper initialization
- `internal/network/connection.go` - Ensure result handling safety
**Solution**: See `docs/planning/phase1-critical-fixes-plan.md` for detailed fix

### 3. JOIN Column Resolution [HIGH PRIORITY]
**Problem**: "column not resolved" errors in JOIN queries
**Symptoms**:
- Qualified column names (table.column) not working
- Affects most TPC-H queries
**Files to Fix**:
- `internal/sql/planner/resolve.go` - Column resolver logic
- `internal/sql/planner/join.go` - Schema merging in joins
**Solution**: Implement proper qualified name resolution

### 4. Aggregate Expressions in Projection [HIGH PRIORITY]
**Problem**: Cannot use expressions like SUM(a)/SUM(b) in SELECT
**Symptoms**:
- "unsupported expression type: *planner.AggregateExpr"
- TPC-H Q8 fails
**Files to Fix**:
- `internal/sql/executor/builder.go` - Add aggregate expression support
- `internal/sql/executor/projection.go` - Handle complex expressions

### 5. CHECK Constraint Parser Issue [HIGH PRIORITY]
**Problem**: Parser fails on CHECK constraints with comparison operators
**Symptoms**:
- "unexpected token in expression: >" error
**Files to Fix**:
- `internal/sql/parser/parser.go` - CHECK constraint parsing logic
- `internal/sql/parser/expression.go` - Expression parsing in constraints

## 📋 SQL Feature Gaps (Required for TPC-H)

### 6. DISTINCT Support [MEDIUM PRIORITY]
**Required by**: Several TPC-H queries
**Implementation**:
- Add DISTINCT operator to executor
- Update planner to recognize DISTINCT
- Consider hash-based vs sort-based implementation

### 7. LIMIT/OFFSET Support [MEDIUM PRIORITY]
**Required by**: TPC-H Q18 (LIMIT 100)
**Implementation**:
- Add LIMIT/OFFSET to parser AST
- Implement LimitOperator in executor
- Update planner to handle LIMIT/OFFSET

### 8. BYTEA Data Type [MEDIUM PRIORITY]
**Status**: Only remaining core PostgreSQL type
**Implementation**:
- Add BYTEA to types system
- Implement serialization/deserialization
- Add escape/unescape functions

## 🔧 Performance & Optimization

### 9. Index-Query Integration [MEDIUM PRIORITY]
**Current State**: Indexes created but not used by queries
**Implementation**:
- Complete IndexScanOperator
- Add cost estimation for index scans
- Update optimizer to choose index scans
**Note**: Requires basic queries to work first

### 10. Complete TPC-H Benchmark Suite [LOW PRIORITY]
**Status**: 4/22 queries implemented
**Remaining Features Needed**:
- Window functions (for Q2, Q17, Q18, Q20)
- Correlated subqueries in SELECT
- Multiple nested subqueries
- Additional aggregates (STDDEV)

## 🛠️ Technical Debt & Quality

### 11. Error Handling Improvements [LOW PRIORITY]
**Issue**: String-based error matching in connection.go
**Solution**: Create proper error types and constants

### 12. Test Infrastructure [LOW PRIORITY]
- Fix network test user ordering issue
- Add integration tests for disk-backed storage
- Extended query protocol test coverage

### 13. CLI Tools Implementation [LOW PRIORITY]
**quantactl**: Management CLI
- status, get, put, delete, query commands
- PostgreSQL wire protocol client

**test-client**: Testing tool
- Protocol compliance testing
- Performance benchmarking
- Load testing capabilities

## 📊 Progress Tracking

### Completed Recently ✅
- Fixed linting issues (97 → 0 errors)
- Fixed optimizer plan comparison for subquery decorrelation
- Standardized integer types in many places
- Added build tags for test programs

### In Progress 🚧
- Investigating storage type mismatch issues
- Planning GROUP BY crash fix

### Blocked ❌
- TPC-H benchmark testing (blocked by GROUP BY crash)
- Performance optimization (blocked by basic functionality)
- Index usage testing (blocked by query issues)

## 🎯 Recommended Execution Order

1. **Week 1**: Fix storage type mismatches (#1)
   - This affects many tests and basic functionality
   - Should unblock other development

2. **Week 1-2**: Fix GROUP BY crash (#2)
   - Critical for any aggregate testing
   - Detailed plan already exists

3. **Week 2**: Fix JOIN resolution (#3) and aggregate expressions (#4)
   - Both needed for TPC-H queries
   - Can be worked on in parallel

4. **Week 3**: Add DISTINCT (#6) and LIMIT/OFFSET (#7)
   - Basic SQL features
   - Relatively straightforward

5. **Week 4**: CHECK constraints (#5) and BYTEA (#8)
   - Complete core SQL support

6. **Week 5+**: Index integration (#9) and remaining features
   - Performance optimization phase
   - Requires stable base functionality

## 📝 Notes

- Always run `make test` after changes to ensure no regressions
- Use `make lint` before committing
- Update this TODO list as issues are resolved
- Consider adding integration tests for each fixed issue
- Document any API changes or new features