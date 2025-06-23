# Semi/Anti Join Design Document

## Overview

Semi and anti joins are specialized join operators used for EXISTS, IN, NOT EXISTS, and NOT IN predicates. Unlike regular joins, they don't produce a cartesian product - they only check for existence.

## Join Types

### Semi Join
- Returns rows from the left table that have at least one match in the right table
- Used for EXISTS and IN predicates
- Stops searching after finding the first match
- No duplicates even if multiple matches exist

### Anti Join
- Returns rows from the left table that have NO matches in the right table
- Used for NOT EXISTS and NOT IN predicates
- Must check all rows to confirm no match exists
- Special NULL handling for NOT IN

## Implementation Strategy

### 1. Hash-based Semi/Anti Join
```
For Semi Join:
1. Build hash table from right input
2. For each left row:
   - Probe hash table
   - If match found, output row and continue
   - If no match, skip row

For Anti Join:
1. Build hash table from right input
2. For each left row:
   - Probe hash table
   - If no match found, output row
   - If match found, skip row
```

### 2. Nested Loop Semi/Anti Join
```
For Semi Join:
1. For each left row:
   - Scan right input until match found
   - If match found, output row and continue to next left row
   - If no match after full scan, skip row

For Anti Join:
1. For each left row:
   - Scan entire right input
   - If no match found in entire scan, output row
   - If any match found, skip row
```

### 3. Sort-Merge Semi/Anti Join
```
For Semi Join:
1. Sort both inputs on join keys
2. Merge:
   - If match found, output left row and advance to next distinct left key
   - If left < right, advance left
   - If left > right, advance right

For Anti Join:
1. Sort both inputs on join keys
2. Merge:
   - If no match for left row, output it
   - Track all unmatched left rows
```

## NULL Handling

### IN/NOT IN with NULLs
- `x IN (1, 2, NULL)` returns TRUE if x=1 or x=2, NULL otherwise
- `x NOT IN (1, 2, NULL)` returns FALSE if x=1 or x=2, NULL otherwise
- Special care needed when right side contains NULLs

### EXISTS/NOT EXISTS
- NULLs in join conditions behave normally (NULL = NULL is NULL, not TRUE)
- Simpler than IN/NOT IN semantics

## Operator Interface

```go
type SemiJoinOperator struct {
    leftChild     Operator
    rightChild    Operator
    joinType      SemiJoinType  // Semi or Anti
    joinKeys      []int         // Column indices for equi-join
    joinCondition ExprEvaluator // Additional conditions
    isCorrelated  bool          // For correlated subqueries
}

type SemiJoinType int
const (
    SemiJoin SemiJoinType = iota
    AntiJoin
)
```

## Query Planner Integration

### Pattern Recognition
```sql
-- EXISTS pattern
SELECT * FROM orders o 
WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id)
→ SemiJoin(orders, order_items, o.id = i.order_id)

-- IN pattern
SELECT * FROM products p 
WHERE p.category_id IN (SELECT id FROM categories WHERE active = true)
→ SemiJoin(products, Filter(categories, active=true), p.category_id = id)

-- NOT EXISTS pattern
SELECT * FROM customers c 
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
→ AntiJoin(customers, orders, c.customer_id = o.customer_id)

-- NOT IN pattern
SELECT * FROM products p 
WHERE p.id NOT IN (SELECT product_id FROM discontinued_items)
→ AntiJoin(products, discontinued_items, p.id = product_id)
```

### Transformation Rules
1. EXISTS(subquery) → SemiJoin
2. IN(subquery) → SemiJoin (with NULL handling)
3. NOT EXISTS(subquery) → AntiJoin
4. NOT IN(subquery) → AntiJoin (with special NULL handling)

## Optimization Strategies

### Early Termination
- Semi joins can stop after finding first match
- Use bloom filters for early filtering
- Push down predicates to reduce right side

### Join Order
- Prefer smaller table on right (for hash joins)
- Consider selectivity of join predicates
- Account for correlation in nested queries

### Algorithm Selection
- Hash join for equality predicates with good hash distribution
- Nested loop for highly selective predicates or correlated queries
- Sort-merge when inputs are pre-sorted

## Testing Strategy

1. **Basic Semi/Anti Join Tests**
   - Simple EXISTS/NOT EXISTS
   - IN/NOT IN with subqueries
   - Multiple join conditions

2. **NULL Handling Tests**
   - NULLs in join columns
   - NULLs in IN/NOT IN lists
   - Three-valued logic validation

3. **Performance Tests**
   - Early termination verification
   - Large dataset handling
   - Correlated vs uncorrelated performance

4. **Integration Tests**
   - Complex queries with multiple semi/anti joins
   - Mixed with regular joins
   - Nested subqueries

## Implementation Plan

1. Create base SemiJoinOperator structure
2. Implement hash-based semi/anti join
3. Add NULL handling for IN/NOT IN
4. Integrate with query planner
5. Add query transformation rules
6. Implement optimization strategies
7. Create comprehensive tests
8. Performance benchmarking