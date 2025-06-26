# Error Handling Improvement

## Overview

This document describes the improvement made to error handling in connection.go to address the technical debt of string-based error matching.

## Problem

The original code in `internal/network/connection.go` was using string-based error matching to convert internal errors to PostgreSQL-compatible error codes:

```go
if strings.Contains(errStr, "table") && strings.Contains(errStr, "not found") {
    // Extract table name and return error
}
```

This approach is:
- Fragile: Changes to error messages break the matching
- Inefficient: String operations for every error
- Error-prone: Could match false positives
- Hard to maintain: Error patterns spread across the codebase

## Solution Implemented

### Phase 1: Consolidation (Completed)

We've consolidated all string-based error matching in connection.go, making it the single point where error translation happens. This is an intermediate step that:

1. Keeps the existing error messages from planner and executor unchanged
2. Centralizes all error pattern matching in one location
3. Makes it easier to identify all the patterns that need handling

### Phase 2: Typed Errors (Future Work)

The complete solution would involve:

1. **Create typed error types throughout the codebase**:
   ```go
   // In planner package
   type TableNotFoundError struct {
       Schema string
       Table  string
   }
   
   func (e *TableNotFoundError) Error() string {
       return fmt.Sprintf("table %s.%s not found", e.Schema, e.Table)
   }
   ```

2. **Return typed errors from planner and executor**:
   ```go
   // Instead of:
   return fmt.Errorf("table '%s' not found", tableName)
   
   // Use:
   return &TableNotFoundError{Schema: schema, Table: tableName}
   ```

3. **Use errors.Is() and errors.As() for type checking**:
   ```go
   // In connection.go
   if errors.Is(err, planner.ErrTableNotFound) {
       var tErr *planner.TableNotFoundError
       if errors.As(err, &tErr) {
           return errors.TableNotFoundError(tErr.Table)
       }
   }
   ```

## Benefits

### Current (Phase 1)
- All error matching logic is in one place
- Easier to maintain and debug
- Clear view of all error patterns being handled

### Future (Phase 2)
- Type-safe error handling
- No string matching required
- Errors carry structured metadata
- Compile-time safety for error handling
- Better performance (no string operations)

## Implementation Status

- [x] Consolidated string-based error matching in connection.go
- [ ] Create typed error types in planner package
- [ ] Create typed error types in executor package
- [ ] Update planner to return typed errors
- [ ] Update executor to return typed errors
- [ ] Update connection.go to use typed error checking
- [ ] Remove all string-based error matching

## Testing

Error handling should be tested with:
1. Unit tests for each error type
2. Integration tests verifying correct PostgreSQL error codes
3. Client compatibility tests with various PostgreSQL drivers

## References

- Go error handling best practices: https://go.dev/blog/go1.13-errors
- PostgreSQL error codes: https://www.postgresql.org/docs/current/errcodes-appendix.html