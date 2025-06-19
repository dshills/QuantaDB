# QuantaDB TODO List

## Summary
**Last Updated**: December 19, 2024
**Project Status**: Core features complete! Ready for performance optimizations and enterprise features.
**Recent Updates**: 
- Moved all completed items to `docs/COMPLETED.md`
- Focus is now on pending optimizations and future roadmap items

## Current Sprint (Q1 2025 - Phase 1: Performance Optimization)

### Medium Priority 🟡

#### 1. Transaction-Storage Full Integration
**Status**: MVCC exists but not fully integrated with storage
**Location**: `internal/txn/` and `internal/storage/`
**Tasks**:
- [ ] Connect transaction manager with storage operations
- [ ] Implement visibility checks in scan operators
- [ ] Add transaction ID to all row operations
- [ ] Implement vacuum process for old versions
**Estimated Time**: 1 week
**Impact**: True ACID compliance with proper isolation

#### 2. Query Optimization Improvements
**Status**: Statistics collection complete, advanced optimizations pending
**Tasks**:
- [ ] Implement join reordering based on cost
- [ ] Enhance predicate pushdown for more cases
- [ ] Add histogram-based selectivity for range queries
**Estimated Time**: 5 days
**Impact**: Advanced query optimizations using real statistics

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
*Last Updated: December 19, 2024*
*For completed features, see `docs/COMPLETED.md`*
*For detailed planning, see `docs/ROADMAP.md` and `docs/CURRENT_STATUS.md`*