# QuantaDB Implementation Checklist

## 🚨 Phase 1: Critical Fixes (Must Do First!)

### Fix 1: GROUP BY Crash
- [ ] Add debug logging to GroupByOperator
- [ ] Add nil checks in Next() method
- [ ] Fix result object creation
- [ ] Add nil checks in buildResult()
- [ ] Test with `SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment`
- [ ] Verify no memory leaks

### Fix 2: JOIN Column Resolution  
- [ ] Update ColumnResolver to handle qualified names
- [ ] Fix schema merging in JOIN planning
- [ ] Add table alias tracking
- [ ] Update buildJoin to propagate schemas correctly
- [ ] Test with `SELECT COUNT(*) FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey`
- [ ] Test with GROUP BY on joined tables

### Fix 3: Aggregate Expressions
- [ ] Add AggregateExpr case to buildProjection
- [ ] Implement buildAggregateProjection method
- [ ] Handle binary expressions with aggregates
- [ ] Support division of aggregates
- [ ] Test with `SELECT SUM(l_extendedprice)/SUM(l_quantity) FROM lineitem`
- [ ] Verify TPC-H Q8 works

## 📋 Phase 2: Essential SQL Features

### DISTINCT Implementation
- [ ] Add DISTINCT to parser (select_stmt.go)
- [ ] Create DistinctOperator in executor
- [ ] Add DISTINCT to logical plan
- [ ] Update planner to generate DISTINCT nodes
- [ ] Handle COUNT(DISTINCT col)
- [ ] Test with `SELECT DISTINCT c_mktsegment FROM customer`

### LIMIT/OFFSET Implementation  
- [ ] Add LIMIT/OFFSET to parser grammar
- [ ] Create LimitOperator in executor
- [ ] Add to logical and physical plans
- [ ] Handle LIMIT with ORDER BY
- [ ] Test with `SELECT * FROM customer ORDER BY c_custkey LIMIT 10`
- [ ] Test OFFSET functionality

### BYTEA Data Type
- [ ] Add BYTEA to type system (types/bytea.go)
- [ ] Implement Serialize/Deserialize methods
- [ ] Add to parser type mapping
- [ ] Handle binary literals (E'\\x...')
- [ ] Add to row serialization
- [ ] Test INSERT and SELECT with binary data

## 🔍 Phase 3: Index Integration

### Index Scan Operator
- [ ] Create IndexScanOperator struct
- [ ] Implement Open/Next/Close methods
- [ ] Add index lookup to storage layer
- [ ] Support equality lookups
- [ ] Support range scans
- [ ] Handle index-only scans

### Planner Integration
- [ ] Update buildScan to consider indexes
- [ ] Add index cost estimation
- [ ] Compare index scan vs seq scan costs
- [ ] Update optimizer to choose best plan
- [ ] Add EXPLAIN support for index usage
- [ ] Verify indexes are actually used

### Index Statistics
- [ ] Extend ANALYZE to collect index stats
- [ ] Store index selectivity
- [ ] Calculate index cardinality
- [ ] Use in cost estimation
- [ ] Test cost model accuracy

## 🏃 Phase 4: TPC-H Queries

### Simple Queries First
- [ ] Q1: Basic aggregation with GROUP BY
- [ ] Q6: Simple filtered aggregation  
- [ ] Q4: EXISTS subquery
- [ ] Q12: CASE in aggregates

### Complex Queries
- [ ] Q2: Correlated subquery + MIN
- [ ] Q7: Multi-way JOIN
- [ ] Q9: Complex aggregation
- [ ] Continue with remaining queries...

## 🚀 Phase 5: Advanced Features

### Window Functions
- [ ] Add window function syntax to parser
- [ ] Create WindowOperator
- [ ] Implement partitioning
- [ ] Implement ordering within partitions
- [ ] Add ROW_NUMBER()
- [ ] Add RANK() and DENSE_RANK()
- [ ] Test with TPC-H queries

### Correlated Subqueries in SELECT
- [ ] Extend SubqueryExpr for SELECT clause
- [ ] Handle correlation variables
- [ ] Implement decorrelation optimization
- [ ] Test with TPC-H Q2, Q17, Q20

## 🔧 Phase 6: Performance

### Join Order Optimization
- [ ] Implement cost-based join ordering
- [ ] Use dynamic programming approach
- [ ] Consider join selectivity
- [ ] Test with multi-way joins

### Cost Model Calibration
- [ ] Measure actual operator costs
- [ ] Update cost constants
- [ ] Validate with benchmarks
- [ ] Document cost model

## ✅ Testing Checklist

### After Each Fix
- [ ] Run unit tests
- [ ] Run integration tests
- [ ] Check for memory leaks
- [ ] Run regression tests
- [ ] Update documentation

### Benchmark Testing
- [ ] Load TPC-H data
- [ ] Run all implemented queries
- [ ] Record execution times
- [ ] Compare with baseline
- [ ] Check result correctness

## 🐛 Debug Checklist

### When Debugging Crashes
- [ ] Enable debug logging
- [ ] Run under debugger (dlv or gdb)
- [ ] Add trace points
- [ ] Check for nil pointers
- [ ] Verify schema propagation
- [ ] Check memory allocation

### When Results Are Wrong
- [ ] Verify operator logic
- [ ] Check type conversions
- [ ] Trace data flow
- [ ] Compare with expected results
- [ ] Check NULL handling

## 📊 Progress Tracking

### Week 1-2
- [ ] All Phase 1 fixes complete
- [ ] Basic TPC-H queries working
- [ ] No more crashes

### Week 3-4  
- [ ] DISTINCT working
- [ ] LIMIT/OFFSET working
- [ ] BYTEA implemented

### Week 5-7
- [ ] Indexes being used
- [ ] Performance improved
- [ ] Cost model calibrated

### Week 8+
- [ ] 10+ TPC-H queries working
- [ ] Window functions started
- [ ] Performance acceptable