# EXISTS Correlation Bug Analysis

## Problem
EXISTS subqueries with correlation predicates (e.g., `WHERE o.user_id = u.id`) are returning true for all rows, even when they should return false. The issue affects queries like:
```sql
SELECT u.id, u.name, 
       EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) as has_orders
FROM users u;
```

## Root Cause
The `SubqueryDecorrelation` optimizer rule in `decorrelation.go` is not extracting the correlation predicate from the subquery's WHERE clause. Instead, it creates a semi-join with a TRUE condition:

```go
// In transformExists() at line 163-166:
joinCondition := &Literal{
    Value: NewTrueValue(),
    Type:  types.Boolean,
}
```

This means the semi-join will return true if ANY row exists in the orders table, regardless of whether it matches the current user.

## Current Behavior
1. Decorrelation transforms `EXISTS (SELECT ... WHERE correlation)` to a semi-join
2. The join condition is always TRUE instead of the correlation predicate
3. Semi-join returns true if right side has any rows at all
4. All users show `has_orders=true` even if they have no orders

## Required Fix
The `transformExists` method needs to:
1. Analyze the subquery plan to find correlation predicates
2. Extract predicates that reference outer columns (e.g., `o.user_id = u.id`)
3. Use these predicates as the join condition
4. Remove the correlation predicates from the subquery's filter

## Implementation Steps
1. Add a method to extract correlation predicates from a subquery plan
2. Identify which columns reference the outer query
3. Move correlation predicates to become the join condition
4. Leave non-correlated predicates in the subquery

## Example Transformation
Before decorrelation:
```
Filter(EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100))
  Scan(users u)
```

After decorrelation (should be):
```
SemiJoin(o.user_id = u.id)
  Scan(users u)
  Filter(o.amount > 100)
    Scan(orders o)
```

Current (incorrect) transformation:
```
SemiJoin(TRUE)  // Wrong! No correlation
  Scan(users u)
  Filter(o.user_id = u.id AND o.amount > 100)  // Still has correlation
    Scan(orders o)
```

## Impact
- All EXISTS queries with correlation return incorrect results
- Affects semi-joins and anti-joins (NOT EXISTS)
- Critical for correct query execution