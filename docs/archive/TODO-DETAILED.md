# QuantaDB Detailed TODO List
Last Updated: December 2024

## 🔥 CRITICAL: Immediate Fixes (Blocking Basic Functionality)

### ✅ COMPLETED FIXES (December 2024)

1. **Storage Type Mismatch Issue** - FIXED ✅
   - Standardized all integer handling to int64
   - Fixed serialization/deserialization consistency

2. **GROUP BY Server Crash** - FIXED ✅
   - Added proper nil checks and initialization
   - Server no longer crashes on GROUP BY queries

3. **JOIN Column Resolution** - FIXED ✅
   - Implemented qualified column name resolution
   - table.column syntax now works properly

4. **Aggregate Expressions in Projection** - FIXED ✅
   - Added support for complex aggregate expressions
   - SUM(a)/SUM(b) type expressions now work

5. **Column Resolution in Filter Predicates** - FIXED ✅
   - Filter operators now properly resolve column references
   - WHERE clauses work with all column types

6. **Date/Time Arithmetic** - FIXED ✅
   - Fixed date/time deserialization to return time.Time objects
   - Date + Interval operations now work correctly
   - Added interval type support in planner

### 🚧 REMAINING CRITICAL ISSUES

### 1. CHECK Constraint Expression Parsing [HIGH PRIORITY]
**Problem**: Limited expression parser for CHECK constraints
**Symptoms**:
- "unexpected token in expression: >" error
- Cannot parse comparison operators in CHECK expressions
**Files to Fix**:
- `internal/sql/parser/parser.go` - CHECK constraint parsing logic
- `internal/sql/parser/expression.go` - Expression parsing in constraints

## 📋 SQL Feature Gaps (Required for TPC-H)

### ✅ COMPLETED SQL FEATURES (December 2024)

1. **DISTINCT Support** - COMPLETED ✅
   - Hash-based deduplication implemented
   - Fully integrated with query planner

2. **BYTEA Data Type** - COMPLETED ✅
   - PostgreSQL-compatible binary data type
   - Full serialization/deserialization support

### 🚧 REMAINING SQL FEATURES

### 1. LIMIT/OFFSET Support [MEDIUM PRIORITY]
**Required by**: TPC-H Q18 (LIMIT 100)
**Implementation**:
- Add LIMIT/OFFSET to parser AST
- Implement LimitOperator in executor
- Update planner to handle LIMIT/OFFSET

### 2. CASCADE DELETE [MEDIUM PRIORITY]
**Required by**: Foreign key constraints
**Implementation**:
- Add CASCADE action support to constraint validator
- Implement recursive deletion logic
- Handle other actions (SET NULL, SET DEFAULT)

## 🔧 Performance & Optimization

### ✅ Index-Query Integration - COMPLETED ✅
**Status**: Full B+Tree index integration with cost-based optimization
- IndexScanOperator fully implemented
- Cost estimation for index vs sequential scans
- Optimizer chooses best access path based on statistics

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