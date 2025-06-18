# QuantaDB TODO List

## Summary
**Total Items**: 5  
**Completed**: 1 ✅  
**Pending**: 4 (3 High 🔴, 1 Medium 🟡)  
**Last Updated**: December 18, 2024

## Completed Items ✅

### 1. ✅ Fix Insecure Secret Key Generation (December 18, 2024)
**Location**: `internal/network/connection.go:236`
**Issue**: BackendKeyData used predictable formula (`c.id * 12345`) for secret key generation
**Impact**: Potential session hijacking vulnerability
**Solution**: Implemented using `crypto/rand` for cryptographically secure random generation
**Tests**: Added comprehensive security tests in `connection_security_test.go`

## Pending High Priority Items 🔴

### 1. Apply Write Timeouts in Connection Handler
**Location**: `internal/network/connection.go`
**Issue**: SetWriteTimeout method exists but is never called
**Impact**: Connections can hang indefinitely on write operations, causing resource exhaustion
**Solution**: Apply write deadlines alongside read deadlines in the main message loop
**Estimated Time**: 1-2 hours

### 2. Implement UPDATE and DELETE Operations
**Status**: SQL parser ready, needs storage integration
**Components to implement**:
- UpdateOperator with storage backend
- DeleteOperator with storage backend
- MVCC versioning for updates
- Tombstone marking for deletes
**Impact**: Core SQL functionality incomplete without these operations
**Estimated Time**: 8-12 hours

### 3. Add Write-Ahead Logging (WAL)
**Purpose**: Essential for durability and crash recovery
**Components**:
- Design log record format
- Implement log buffer and flushing
- Create recovery manager
- Log all modifications before applying
- Implement checkpoint mechanism
**Impact**: No crash recovery without WAL
**Estimated Time**: 2-3 days

## Pending Medium Priority Items 🟡

### 1. Integrate B+Tree Indexes with Query Planner
**Status**: B+Tree implementation exists but unused
**Tasks**:
- Update query planner to consider indexes
- Implement index-backed scan operators
- Add cost estimation for index vs sequential scan
- Update statistics based on actual data
**Impact**: Poor query performance without index usage
**Estimated Time**: 1-2 days

## Next Steps

**Recommended Order**:
1. **Apply Write Timeouts** (Quick security fix, 1-2 hours)
2. **UPDATE/DELETE Operations** (Core functionality, 8-12 hours)
3. **WAL Implementation** (Critical for production, 2-3 days)
4. **Index Integration** (Performance optimization, 1-2 days)

## Additional Items for Future Consideration

From `docs/TECHNICAL_DEBT.md`:
- **PostgreSQL Compatibility**: Generic error codes, missing extended query protocol
- **Transaction State**: Timing issues with transaction cleanup
- **Code Quality**: Redundant flush calls, hardcoded version strings

From `docs/CURRENT_STATUS.md`:
- **Authentication System**: No user authentication implemented
- **Distributed Features**: Single-node only currently
- **Advanced SQL**: CTEs, Window Functions not supported
- **Monitoring**: No metrics or health endpoints

## Resources

- **Planning Docs**: `docs/` directory contains detailed plans
- **Current Status**: `docs/CURRENT_STATUS.md`
- **Technical Debt**: `docs/TECHNICAL_DEBT.md`
- **Storage Plan**: `docs/STORAGE_INTEGRATION_PLAN.md`