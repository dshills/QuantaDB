# Extended Query Protocol Parameter Handling Fix

## Problem Statement
The Extended Query Protocol infrastructure is complete (Parse, Bind, Execute) but parameters don't actually work. Any query with `$1`, `$2` style parameters fails during query planning because the planner doesn't handle `ParameterRef` AST nodes.

## Current State Analysis

### What Works ✅
1. Network layer correctly handles Parse/Bind/Execute messages
2. SQL parser correctly parses parameter placeholders (`$1`, `$2`, etc.)
3. Parameters are properly stored in Portal structs
4. ParameterSubstitutor exists with substitution logic
5. Type inference for parameters is implemented

### What's Broken ❌
1. Query planner throws error when encountering ParameterRef nodes
   - `convertExpression()` in `internal/sql/planner/planner.go:306-432` doesn't handle `*parser.ParameterRef`
   - Falls through to default case: `fmt.Errorf("unsupported expression type: %T", expr)`
2. No integration between Portal parameters and query execution
3. ParameterSubstitutor is never called during execution

## Implementation Approach

The fix requires three main changes:
1. Make the planner accept ParameterRef nodes without error
2. Implement plan traversal to substitute parameters
3. Call the substitution before query execution

## Implementation Plan

### Phase 1: Handle ParameterRef in Query Planner
**Location**: `internal/sql/planner/planner.go:429` (before the default case)

Add a new case to the convertExpression function:

```go
case *parser.ParameterRef:
    // Convert parser.ParameterRef to planner.ParameterRef
    return &ParameterRef{
        Index: e.Index,
        Type:  types.Unknown, // Type will be inferred during bind
    }, nil
```

This allows the planner to create plans with parameter placeholders that will be substituted at execution time.

### Phase 2: Implement SubstituteInPlan Method
**Location**: `internal/sql/parameter.go:23-27`

The `SubstituteInPlan` method exists but is not implemented. We need to implement plan traversal:

```go
// SubstituteInPlan substitutes parameters in a logical plan.
func (s *ParameterSubstitutor) SubstituteInPlan(plan planner.LogicalPlan) (planner.LogicalPlan, error) {
    visitor := &parameterSubstitutionVisitor{substitutor: s}
    return visitor.VisitPlan(plan)
}
```

This requires implementing a visitor pattern to traverse the plan tree and substitute parameters in all expressions.

### Phase 3: Integrate with Portal Execution
**Location**: `internal/network/connection.go:798-802`

Replace the TODO section with actual parameter substitution:

```go
// Apply parameter substitution to the plan
if len(portal.ParamValues) > 0 {
    substitutor := sql.NewParameterSubstitutor(portal.ParamValues)
    plan = substitutor.SubstitutePlan(plan)
}
```

Note: The ParameterSubstitutor already exists but needs a `SubstitutePlan` method that traverses the plan tree and replaces ParameterRef nodes with actual values.

### Phase 4: Testing Strategy

1. **Unit Tests**: Test ParameterRef handling in planner
2. **Integration Tests**: Test full Parse/Bind/Execute flow
3. **Type Tests**: Verify parameter type inference
4. **Driver Tests**: Test with real PostgreSQL drivers

## Test Cases

### Basic Parameter Test
```sql
-- Parse
PARSE stmt1 AS SELECT * FROM users WHERE id = $1;
-- Bind with integer parameter
BIND portal1 TO stmt1 WITH (123);
-- Execute
EXECUTE portal1;
```

### Multiple Parameters
```sql
PARSE stmt2 AS INSERT INTO users (id, name, email) VALUES ($1, $2, $3);
BIND portal2 TO stmt2 WITH (1, 'Alice', 'alice@example.com');
EXECUTE portal2;
```

### Type Inference
```sql
-- Numeric context
SELECT * FROM products WHERE price > $1;

-- String context  
SELECT * FROM users WHERE name LIKE $1;

-- Boolean context
SELECT * FROM users WHERE active = $1;
```

## Success Criteria

1. All parameterized queries execute successfully
2. Parameter substitution maintains correct types
3. PostgreSQL drivers (pq, pgx, psycopg2) work correctly
4. Performance overhead is minimal (<5%)

## Timeline

- Day 1: Implement ParameterRef handling in planner + SubstituteInPlan
- Day 2: Write tests and fix edge cases
- Day 3: Test with real PostgreSQL drivers

## Summary

The Extended Query Protocol infrastructure is well-designed but has a critical missing piece: the query planner doesn't handle ParameterRef nodes. This causes any parameterized query to fail during planning. The fix is straightforward:

1. Add a case for `*parser.ParameterRef` in the planner's `convertExpression` function
2. Implement the `SubstituteInPlan` method to traverse plans and substitute parameters
3. Call the substitutor in `handleExecute` before query execution

Once fixed, QuantaDB will support prepared statements and work with all PostgreSQL drivers.