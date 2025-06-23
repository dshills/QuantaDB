# TPC-H SQL Feature Matrix

This document maps TPC-H queries to required SQL features, helping prioritize implementation.

## Feature Requirements by Query

| Feature | Required By | Priority | Status |
|---------|------------|----------|--------|
| **Table Aliases (same table multiple times)** | Q8, Q21 | HIGH | ❌ Basic aliases ✅, Multiple ❌ |
| **Correlated Subqueries in SELECT** | Q2, Q17, Q20, Q21, Q22 | HIGH | ❌ |
| **Window Functions** | Q2, Q17, Q18, Q20 | MEDIUM | ❌ |
| **STDDEV Aggregate** | Q17 | MEDIUM | ❌ |
| **EXISTS Subquery** | Q4, Q20, Q21 | LOW | ✅ Parser support |
| **NOT EXISTS** | Q21, Q22 | LOW | ✅ Parser support |
| **IN Subquery** | Q12, Q16, Q18 | LOW | ✅ Parser support |
| **NOT IN Subquery** | Q16 | LOW | ✅ Parser support |
| **ALL/ANY/SOME** | Q20, Q21 | MEDIUM | ❌ |
| **Views or CTEs** | Q15 | MEDIUM | ❌ |
| **LEFT OUTER JOIN** | Q13 | LOW | ✅ |
| **SUBSTRING** | Q22 | LOW | ❌ |
| **Complex OR conditions** | Q19 | LOW | ✅ |
| **Correlated subquery in HAVING** | Q11 | MEDIUM | ❌ |

## Queries Grouped by Complexity

### 🟢 Should Work Now (test these first!)
- **Q6** ✅ - Forecasting Revenue Change (confirmed working)
- **Q14** - Promotion Effect (CASE in aggregates)
- **Q12** - Shipping Modes (basic IN operator)
- **Q19** - Discounted Revenue (complex OR conditions)

### 🟡 Need Minor Fixes
- **Q1** - Pricing Summary Report (type coercion issue)
- **Q3** - Shipping Priority (performance optimization needed)
- **Q5** - Local Supplier Volume (needs testing)
- **Q10** - Returned Item Reporting (needs testing)
- **Q8** - National Market Share (multiple table aliases)

### 🟠 Need Correlated Subqueries
- **Q2** - Minimum Cost Supplier (also needs window functions)
- **Q11** - Important Stock Identification (in HAVING clause)
- **Q17** - Small-Quantity-Order Revenue (also needs STDDEV)
- **Q20** - Potential Part Promotion (also needs ALL/ANY)
- **Q21** - Suppliers Who Kept Orders (most complex - multiple correlated)
- **Q22** - Global Sales Opportunity (also needs SUBSTRING)

### 🔴 Need Window Functions
- **Q2** - Minimum Cost Supplier
- **Q17** - Small-Quantity-Order Revenue  
- **Q18** - Large Volume Customer
- **Q20** - Potential Part Promotion

### 🟣 Need Other Features
- **Q4** - Order Priority Checking (EXISTS - should work)
- **Q7** - Volume Shipping (complex joins only)
- **Q9** - Product Type Profit Measure (complex expressions)
- **Q13** - Customer Distribution (LEFT JOIN - should work)
- **Q15** - Top Supplier Query (needs Views/CTEs)
- **Q16** - Parts/Supplier Relationship (NOT IN subquery)

## Implementation Strategy

### Phase 1: Quick Wins (1-2 days)
1. Fix Q1 type issues
2. Test Q12, Q14, Q19 (should work)
3. Test Q4, Q13 (use existing features)
4. Optimize Q3 with indexes

### Phase 2: Table Aliases (2-3 days)
1. Enhance table alias support for same table multiple times
2. Fix Q8
3. Prepare for Q21

### Phase 3: Correlated Subqueries (1 week)
1. Implement correlation resolution in SELECT clause
2. Extend to HAVING clause
3. Unlock Q2, Q11, Q17, Q20, Q21, Q22

### Phase 4: Statistical Functions (2-3 days)
1. Implement STDDEV/VARIANCE
2. Complete Q17

### Phase 5: Window Functions (1-2 weeks)
1. Implement basic window function framework
2. Start with ROW_NUMBER() OVER
3. Add RANK(), DENSE_RANK()
4. Complete Q2, Q18, Q20

### Phase 6: Minor Features (3-4 days)
1. ALL/ANY/SOME operators
2. SUBSTRING function
3. Views or CTEs for Q15

## Expected Progress

With this plan:
- **Week 1**: 10-12 queries working (up from 2)
- **Week 2**: 15-17 queries working
- **Week 3**: 20-22 queries working

## Performance Considerations

Key indexes needed:
```sql
CREATE INDEX idx_customer_mktsegment ON customer(c_mktsegment);
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_partsupp_partkey ON partsupp(ps_partkey);
CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);
```