# QuantaDB Technical Debt

This document tracks technical debt items identified through code reviews and development.

## Security Issues

### HIGH: Insecure Secret Key Generation
**Location**: `internal/network/connection.go:234`
**Issue**: BackendKeyData uses predictable formula (`c.id * 12345`)
**Impact**: Potential session hijacking if key is used for authentication
**Solution**: Use `crypto/rand` for cryptographically secure random generation
```go
var secretKey int32
err := binary.Read(rand.Reader, binary.BigEndian, &secretKey)
```

## Stability Issues

### HIGH: Missing Write Timeout Application
**Location**: `internal/network/connection.go`
**Issue**: SetWriteTimeout method exists but is never called
**Impact**: Connections can hang indefinitely on write operations, causing resource exhaustion
**Solution**: Apply write deadlines alongside read deadlines in the main message loop

## PostgreSQL Compatibility

### MEDIUM: Generic Error Codes
**Location**: `internal/network/connection.go:492`
**Issue**: All errors use generic "42000" (syntax error) code
**Impact**: Clients cannot programmatically distinguish error types
**Solution**: Map internal errors to specific PostgreSQL SQLSTATE codes

### MEDIUM: Unimplemented Extended Query Protocol
**Location**: `internal/network/connection.go:527-545`
**Issue**: Parse/Bind/Execute are stubs returning "not implemented"
**Impact**: No support for prepared statements, affecting ORMs and performance
**Solution**: Implement full extended query protocol support

### MEDIUM: Missing Table OIDs
**Location**: `internal/network/connection.go:419`
**Issue**: TableOID hardcoded to 0 in RowDescription
**Impact**: Limited client introspection capabilities
**Solution**: Populate from catalog metadata

## Correctness Issues

### MEDIUM: Transaction State Update Timing
**Location**: `internal/network/connection.go:342,366,390`
**Issue**: Setting currentTxn to nil before checking commit/rollback success
**Impact**: Connection state inconsistency on transaction operation failure
**Solution**: Only clear currentTxn after successful operation

## Code Quality

### LOW: Redundant Flush Calls
**Location**: `internal/network/connection.go:105,112`
**Issue**: Explicit flush after sendError which already flushes
**Impact**: None (redundant code)
**Solution**: Remove redundant flush calls

### LOW: Hardcoded Server Version
**Location**: `internal/network/connection.go:212`
**Issue**: Version string hardcoded as "15.0 (QuantaDB 0.1.0)"
**Impact**: Version inconsistency across deployments
**Solution**: Use build-time version injection or configuration

## Tracking Progress

Items are tracked in the main todo list and CURRENT_STATUS.md. This document provides detailed technical context for each issue.

## Query Processing Issues

### MEDIUM: Planner Expression Validation Too Permissive
**Location**: `internal/sql/planner/planner.go:530`
**Issue**: validateExpressionColumns default case silently accepts unknown expression types
**Impact**: Future expression types (e.g., function calls) may have invalid columns that go unvalidated
**Solution**: Change default case to return error for unsupported expression types
```go
default:
    return fmt.Errorf("unsupported expression type in WHERE: %T", expr)
}
```

### MEDIUM: UPDATE Only Supports Literal Values
**Location**: `internal/sql/executor/update.go:140`
**Issue**: UPDATE operator rejects non-literal assignments (e.g., SET col = col + 1)
**Impact**: Limited UPDATE functionality compared to standard SQL
**Solution**: Either:
1. Document limitation clearly in planner and reject early
2. Implement expression evaluation for assignments
```go
// In planUpdate:
for _, assign := range stmt.Assignments {
    if _, ok := assign.Value.(*parser.Literal); !ok {
        return nil, fmt.Errorf("UPDATE only supports literal assignments, got %T", assign.Value)
    }
}
```

## Documentation Issues

### LOW: TODO.md List Numbering
**Location**: `TODO.md:8`
**Issue**: High-priority section numbers inconsistent (1→2→1)
**Impact**: Confusing documentation
**Solution**: Fix numbering to be sequential

## Review History

- 2024-12-18: Initial review after PostgreSQL connection stability fixes
  - Identified by: Zen Code Review (flash model)
  - Main fix validated: SSL negotiation using Peek() is correct
- 2024-12-18: Review after UPDATE/DELETE implementation
  - Identified by: Zen Code Review (o4-mini model)
  - Critical and high issues fixed before commit
  - Medium/low issues documented here for future work