# QuantaDB TODO List

## Summary
**Last Updated**: December 18, 2024
**Project Status**: Core features complete! Ready for performance optimizations and enterprise features.

## Current Sprint (Q1 2025 - Phase 1: Performance Optimization)

### High Priority 🔴

#### 1. Index-Query Planner Integration
**Status**: B+Tree implementation exists but not integrated
**Location**: `internal/sql/planner/` and `internal/index/`
**Tasks**:
- [ ] Update query planner to consider indexes in plan generation
- [ ] Implement index-backed scan operators
- [ ] Add cost estimation for index vs sequential scan  
- [ ] Create statistics collection for index selection
- [ ] Write integration tests for index usage
**Estimated Time**: 1-2 weeks
**Impact**: Significant query performance improvement for indexed columns

#### 2. Extended Query Protocol Implementation
**Status**: Simple query protocol only, no prepared statements
**Location**: `internal/network/` and `internal/sql/`
**Tasks**:
- [ ] Implement Parse message handling
- [ ] Implement Bind message for parameter binding
- [ ] Implement Execute with portal management
- [ ] Add prepared statement caching
- [ ] Test with JDBC/ODBC drivers
**Estimated Time**: 1-2 weeks
**Impact**: Required for most database drivers and ORMs

### Medium Priority 🟡

#### 3. PostgreSQL Error Code Mapping
**Status**: Generic errors, need SQLSTATE codes
**Location**: Throughout codebase, centralize in `internal/errors/`
**Tasks**:
- [ ] Create error code mapping system
- [ ] Map all errors to PostgreSQL SQLSTATE codes
- [ ] Update network layer to send proper error responses
- [ ] Document error codes
**Estimated Time**: 3-4 days
**Impact**: Better client compatibility and debugging

#### 4. Transaction-Storage Full Integration
**Status**: MVCC exists but not fully integrated with storage
**Location**: `internal/txn/` and `internal/storage/`
**Tasks**:
- [ ] Connect transaction manager with storage operations
- [ ] Implement visibility checks in scan operators
- [ ] Add transaction ID to all row operations
- [ ] Implement vacuum process for old versions
**Estimated Time**: 1 week
**Impact**: True ACID compliance with proper isolation

#### 5. Query Optimization Improvements
**Status**: Basic optimizer, needs statistics-based decisions
**Tasks**:
- [ ] Implement table statistics collection
- [ ] Add histogram-based selectivity estimation
- [ ] Implement join reordering based on cost
- [ ] Add predicate pushdown optimization
**Estimated Time**: 2 weeks
**Impact**: Better query plans for complex queries

## Completed Items ✅

### Security & Stability
- ✅ Fixed BackendKeyData generation (crypto/rand)
- ✅ Applied write timeouts in connection handler
- ✅ Resolved SSL negotiation issues

### Core SQL Features  
- ✅ SQL parser with full syntax support
- ✅ Query planner and executor
- ✅ CREATE TABLE with persistence
- ✅ INSERT operations with storage
- ✅ UPDATE operations with MVCC
- ✅ DELETE operations with tombstones
- ✅ SELECT with joins, aggregates, sorting

### Storage & Durability
- ✅ Page-based disk storage  
- ✅ Buffer pool with LRU eviction
- ✅ Write-Ahead Logging (WAL)
- ✅ Crash recovery system
- ✅ Checkpoint mechanism

### Infrastructure
- ✅ PostgreSQL wire protocol v3
- ✅ B+Tree index implementation (not integrated)
- ✅ MVCC transaction support
- ✅ Network connection management

## Future Roadmap (from ROADMAP.md)

### Phase 2: Enterprise Features (Q2 2025)
- Authentication & user management
- Role-based access control
- Backup & recovery tools
- Monitoring & metrics
- Admin dashboard

### Phase 3: Advanced SQL (Q3 2025)
- Common Table Expressions (CTEs)
- Window functions
- Stored procedures & triggers
- JSON/JSONB support
- Array types

### Phase 4: Distributed Features (Q4 2025)
- Streaming replication
- Horizontal sharding
- Raft consensus
- Distributed transactions

## Quick Reference

### Build & Test
```bash
make build          # Build server and CLI
make test          # Run all tests
make test-coverage # Generate coverage report
golangci-lint run  # Run linter
```

### Test Storage Features
```sql
-- All these operations now work with persistence!
CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
UPDATE users SET email = 'newemail@example.com' WHERE id = 1;
DELETE FROM users WHERE id = 1;
SELECT * FROM users;
```

## Contributing

See CONTRIBUTING.md for guidelines. Priority areas:
- Performance optimizations
- PostgreSQL compatibility improvements  
- Test coverage expansion
- Documentation updates

---
*For detailed planning, see `docs/ROADMAP.md` and `docs/CURRENT_STATUS.md`*