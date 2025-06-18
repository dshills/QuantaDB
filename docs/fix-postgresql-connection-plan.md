# PostgreSQL Client Connection Stability Fix Plan

## Problem Summary
psql and other PostgreSQL clients timeout during the protocol handshake when connecting to QuantaDB. The primary issue is in the SSL negotiation handling within the startup phase.

## Root Causes Identified

### 1. **SSL Request Handling Bug** (Critical)
- Location: `internal/network/connection.go:117-201`
- Issue: Direct reads from buffered reader during SSL detection corrupts subsequent reads
- Impact: Startup message gets corrupted after SSL negotiation

### 2. **Buffered Reader Misuse**
- Using `io.ReadFull()` on a `bufio.Reader` consumes bytes that can't be "unread"
- The manual message reconstruction after SSL check is error-prone

### 3. **Incomplete Error Handling**
- Errors during startup may not be properly flushed to client
- Client doesn't receive error messages before connection closes

### 4. **Hardcoded Timeouts**
- 30-second timeouts are hardcoded instead of using server configuration
- Doesn't respect client expectations

## Implementation Plan

### Phase 1: Fix SSL Request Handling (Priority: Critical)
1. **Refactor `handleStartup()` to use `Peek()` instead of `Read()`**
   - Use `reader.Peek(8)` to check for SSL request without consuming bytes
   - Only discard bytes after confirming SSL request
   - Eliminate manual message reconstruction

2. **Improve SSL Response Flow**
   - Ensure 'N' response is sent immediately for SSL requests
   - Add proper error handling for SSL response write

3. **Add Comprehensive Logging**
   - Log each step of the handshake process
   - Include message lengths, types, and parameters

### Phase 2: Improve Error Handling
1. **Ensure Error Responses Reach Client**
   - Always flush writer after sending errors
   - Add defer statement to flush on function exit

2. **Add Connection State Tracking**
   - Track handshake state (SSL_CHECK, STARTUP, AUTHENTICATED, READY)
   - Log state transitions for debugging

### Phase 3: Configuration and Testing
1. **Use Server Configuration for Timeouts**
   - Replace hardcoded timeouts with config values
   - Add reasonable defaults if not configured

2. **Create Protocol Tests**
   - Unit tests for SSL negotiation
   - Integration tests with actual psql client
   - Test various connection scenarios

## Testing Strategy

### Unit Tests
- Test SSL request detection without consuming bytes
- Test startup message parsing after SSL negotiation
- Test error response formatting

### Integration Tests
- Test connection with psql client
- Test with various PostgreSQL drivers (Go, Python, Java)
- Test SSL and non-SSL connection attempts

### Manual Testing
```bash
# Test basic connection
psql -h localhost -p 5432 -U postgres -d quantadb

# Test with SSL mode
psql "sslmode=disable host=localhost port=5432 user=postgres dbname=quantadb"

# Test with verbose output
PGOPTIONS='--client-min-messages=debug5' psql -h localhost -p 5432 -U postgres -d quantadb
```

## Success Criteria
1. psql can connect without timeout
2. Connection remains stable for extended periods
3. Proper error messages are displayed on connection failure
4. SSL negotiation works correctly (client receives 'N' and continues)
5. Multiple concurrent connections work reliably

## Files to Modify
1. `internal/network/connection.go` - Main connection handling
2. `internal/network/protocol/startup.go` - Startup message parsing
3. `internal/network/server.go` - Server configuration handling
4. `internal/network/connection_test.go` - New/updated tests

## Estimated Timeline
- Phase 1: 2-3 hours (Critical fix)
- Phase 2: 1-2 hours (Error handling)
- Phase 3: 2-3 hours (Config and testing)
- Total: 5-8 hours

## Rollback Plan
If issues arise:
1. Keep original connection.go as backup
2. Use feature flag to toggle between old/new implementation
3. Extensive logging to diagnose any new issues