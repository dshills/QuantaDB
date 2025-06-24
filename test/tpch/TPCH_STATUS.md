# TPC-H Query Implementation Status

## Summary
- **Total Queries**: 22
- **Implemented**: 18
- **Working**: 18
- **Coverage**: 82%
- **Latest**: Correlated subqueries (EXISTS/NOT EXISTS) now working!

## Working Queries

| Query | Name | Status | Notes |
|-------|------|--------|-------|
| Q1 | Pricing Summary Report | ✅ Working | Type coercion fixed |
| Q3 | Shipping Priority | ✅ Working | Optimized with indexes |
| Q4 | Order Priority Checking | ✅ Working | EXISTS subquery |
| Q5 | Local Supplier Volume | ✅ Working | 6-way join |
| Q6 | Forecasting Revenue Change | ✅ Working | Simple aggregation |
| Q9 | Product Type Profit Measure | ✅ Working | Complex expressions, 6-way join |
| Q10 | Returned Item Reporting | ✅ Working | GROUP BY multiple columns |
| Q11 | Important Stock Identification | ✅ Working | Subquery in HAVING (non-correlated) |
| Q12 | Shipping Modes and Order Priority | ✅ Working | IN operator, CASE |
| Q13 | Customer Distribution | ✅ Working | LEFT OUTER JOIN |
| Q14 | Promotion Effect | ✅ Working | LIKE operator |
| Q16 | Parts/Supplier Relationship | ✅ Working | NOT IN with subquery, COUNT DISTINCT |
| Q19 | Discounted Revenue | ✅ Working | Complex OR conditions |
| Q7 | Volume Shipping | ✅ Working | Multiple table aliases |
| Q8 | National Market Share | ✅ Working | Multiple table aliases, CASE expressions |
| Q2 | Minimum Cost Supplier | ✅ Working | Correlated subquery in WHERE |
| Q17 | Small-Quantity-Order Revenue | ✅ Working | Correlated subquery comparing with AVG |
| Q22 | Global Sales Opportunity | ✅ Working | SUBSTRING, correlated EXISTS |

## Not Yet Implemented

| Query | Blocker |
|-------|---------|
| Q15 | WITH clause / CTE (can be rewritten as view) |
| Q18 | IN with subquery + GROUP BY + HAVING |
| Q20 | Correlated subquery + ALL/ANY operators |
| Q21 | Multiple correlated subqueries, complex NOT EXISTS patterns |

## SQL Features Status

### Working ✅
- Basic aggregates (SUM, COUNT, AVG, MIN, MAX)
- STDDEV aggregate function
- GROUP BY / HAVING
- ORDER BY / LIMIT / OFFSET
- All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)
- EXISTS / NOT EXISTS
- IN / NOT IN with value lists
- CASE expressions
- LIKE pattern matching
- SUBSTRING function
- Date arithmetic with INTERVAL
- Complex arithmetic in projections
- Table aliases (multiple instances of same table)
- Non-correlated scalar subqueries in WHERE
- HAVING clauses with aggregate expressions
- Correlated subqueries (EXISTS/NOT EXISTS)
- Correlated scalar subqueries in WHERE
- SUBSTRING function

### Not Implemented ❌
- Window functions (ROW_NUMBER, RANK, etc.)
- ALL/ANY/SOME operators
- Views / CTEs

## Performance Notes
- Indexes significantly improve join performance
- Q3 went from timeout to ~70ms with proper indexes
- All working queries complete in under 2 minutes