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

## Review History

- 2024-12-18: Initial review after PostgreSQL connection stability fixes
  - Identified by: Zen Code Review (flash model)
  - Main fix validated: SSL negotiation using Peek() is correct