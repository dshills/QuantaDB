# QuantaDB Implementation Plan - December 2024

## Overview
This plan addresses the remaining TODO items with a focus on fixing critical bugs first, then adding features needed for TPC-H benchmarks, followed by performance optimizations.

## Current State Analysis

### Critical Issues (from TPC-H benchmark run)
1. **GROUP BY crashes server** - Most critical issue preventing basic queries
2. **JOIN column resolution failures** - Columns not resolved in aggregation context
3. **Aggregate expressions in projection** - Cannot use aggregates in complex expressions
4. **Missing SQL features** - DISTINCT, LIMIT/OFFSET
5. **Index integration** - Indexes created but not used by query planner

### Working Features
- Basic COUNT(*) queries
- Simple table scans
- Data loading (all TPC-H tables loaded successfully)
- PostgreSQL wire protocol

## Implementation Phases

### Phase 1: Critical Bug Fixes (1-2 weeks)
**Goal**: Fix server crashes and basic query execution

#### 1.1 Fix GROUP BY Implementation
- **Issue**: Server crashes with SIGSEGV on GROUP BY queries
- **Root Cause**: Likely null pointer in aggregation operator
- **Tasks**:
  - Debug GroupByOperator in `internal/sql/executor/group_by.go`
  - Add nil checks for result handling
  - Ensure proper schema propagation
  - Add comprehensive GROUP BY tests

#### 1.2 Fix JOIN Column Resolution
- **Issue**: "column c_custkey not resolved" in JOIN queries with aggregation
- **Root Cause**: Column resolver not handling qualified names in JOIN context
- **Tasks**:
  - Fix column resolution in `internal/sql/planner/resolve.go`
  - Update JOIN operator to properly propagate column information
  - Handle table aliases and qualified column names
  - Add JOIN + aggregation tests

#### 1.3 Support Aggregate Expressions in Projection
- **Issue**: "unsupported expression type: *planner.AggregateExpr"
- **Tasks**:
  - Extend projection builder to handle AggregateExpr
  - Implement aggregate expression evaluation
  - Support division of aggregates (e.g., SUM(x)/SUM(y))

### Phase 2: Essential SQL Features (1-2 weeks)
**Goal**: Add missing SQL features needed for TPC-H

#### 2.1 DISTINCT Support
- **Tasks**:
  - Add DISTINCT parser support
  - Implement DistinctOperator in executor
  - Integrate with query planner
  - Handle DISTINCT in aggregates (COUNT(DISTINCT x))

#### 2.2 LIMIT/OFFSET Implementation
- **Tasks**:
  - Add LIMIT/OFFSET to parser
  - Implement LimitOperator in executor
  - Update planner to generate limit nodes
  - Test with ORDER BY + LIMIT

#### 2.3 BYTEA Data Type
- **Tasks**:
  - Add BYTEA to type system
  - Implement serialization/deserialization
  - Add binary literal parsing
  - Test with INSERT/SELECT

### Phase 3: Index Integration (2-3 weeks)
**Goal**: Make query planner use indexes

#### 3.1 Index Scan Operator
- **Tasks**:
  - Implement IndexScanOperator in executor
  - Add index lookup methods to storage layer
  - Handle index-only scans
  - Support range scans

#### 3.2 Query Planner Integration
- **Tasks**:
  - Update planner to consider indexes in `buildScan`
  - Implement cost estimation for index scans
  - Add index selectivity estimation
  - Choose between sequential and index scan based on cost

#### 3.3 Index Statistics
- **Tasks**:
  - Collect index statistics during ANALYZE
  - Store index cardinality and distribution
  - Use statistics in cost estimation

### Phase 4: TPC-H Query Implementation (2-3 weeks)
**Goal**: Implement remaining TPC-H queries

#### 4.1 Simple Queries (Q1, Q6)
- Basic aggregation with filters
- Test GROUP BY fixes

#### 4.2 Subquery Patterns (Q4)
- EXISTS subqueries
- Test subquery decorrelation

#### 4.3 Complex Queries (Q2, Q7, Q9, etc.)
- Multi-way joins
- Nested subqueries
- Complex aggregations

### Phase 5: Advanced SQL Features (3-4 weeks)
**Goal**: Add features for remaining TPC-H queries

#### 5.1 Window Functions
- **Required for**: Q2, Q17, Q18, Q20
- **Tasks**:
  - Add window function syntax to parser
  - Implement WindowOperator
  - Support PARTITION BY and ORDER BY
  - Implement ROW_NUMBER, RANK, etc.

#### 5.2 Correlated Subqueries in SELECT
- **Required for**: Q2, Q17, Q20, Q21, Q22
- **Tasks**:
  - Extend subquery support in projection
  - Handle correlation in SELECT clause
  - Optimize with decorrelation where possible

#### 5.3 Additional Aggregate Functions
- **Tasks**:
  - Implement STDDEV, VARIANCE
  - Add more numeric functions
  - Support aggregate DISTINCT

### Phase 6: Performance Optimization (2-3 weeks)
**Goal**: Optimize query execution

#### 6.1 Join Order Optimization
- Implement dynamic programming join ordering
- Consider join selectivity
- Use table statistics

#### 6.2 Cost Model Calibration
- Measure actual operator costs
- Calibrate cost constants
- Validate with benchmarks

#### 6.3 Query Plan Caching
- Cache prepared statement plans
- Invalidate on schema changes
- Measure cache hit rates

### Phase 7: Infrastructure & Testing (Ongoing)
**Goal**: Improve development infrastructure

#### 7.1 Performance Regression Framework
- Automated benchmark runs
- Performance tracking over time
- Alert on regressions

#### 7.2 Integration Tests
- Comprehensive test suite
- Driver compatibility tests
- Stress testing

#### 7.3 CLI Tools
- Implement quantactl for admin tasks
- Add test-client for debugging
- Create performance analysis tools

## Priority Order

1. **Week 1-2**: Phase 1 (Critical Bug Fixes)
   - Must fix GROUP BY and JOIN issues first
   - These block all other testing

2. **Week 3-4**: Phase 2 (Essential SQL Features)
   - DISTINCT and LIMIT are needed for many queries
   - BYTEA completes type system

3. **Week 5-7**: Phase 3 (Index Integration)
   - Critical for performance
   - Needed before benchmarking

4. **Week 8-10**: Phase 4 (TPC-H Queries)
   - Implement simpler queries first
   - Identify missing features

5. **Week 11-14**: Phase 5 (Advanced SQL)
   - Window functions unlock many queries
   - Correlated subqueries needed for complex patterns

6. **Week 15-17**: Phase 6 (Performance)
   - Optimize based on benchmark results
   - Fine-tune cost model

## Success Metrics

### Phase 1 Success
- All 4 existing TPC-H queries run without crashes
- GROUP BY queries work correctly
- JOIN + aggregation queries succeed

### Phase 3 Success
- Queries use indexes when beneficial
- Index scans faster than table scans for selective queries
- EXPLAIN shows index usage

### Phase 4 Success
- At least 10 TPC-H queries working
- Performance baseline established
- No crashes or incorrect results

### Phase 5 Success
- 18+ TPC-H queries working
- Window functions perform correctly
- Complex queries optimize well

### Final Success
- All 22 TPC-H queries implemented
- Performance competitive with other databases
- Stable under load

## Risk Mitigation

### Technical Risks
- **GROUP BY fix complexity**: May require significant refactoring
  - Mitigation: Add extensive logging, unit tests
  
- **Index integration complexity**: Touches many components
  - Mitigation: Incremental implementation, feature flags

- **Window function complexity**: New operator paradigm
  - Mitigation: Study PostgreSQL implementation, start simple

### Timeline Risks
- **Underestimated complexity**: Features may take longer
  - Mitigation: Weekly progress reviews, adjust scope

- **New bugs discovered**: Testing may reveal more issues
  - Mitigation: Buffer time, fix-first policy

## Next Steps

1. **Immediate** (Today):
   - Set up debugging for GROUP BY crash
   - Create minimal reproducible test case
   - Start investigating root cause

2. **This Week**:
   - Fix GROUP BY implementation
   - Fix JOIN column resolution
   - Add regression tests

3. **Next Week**:
   - Implement DISTINCT support
   - Add LIMIT/OFFSET
   - Begin index integration planning