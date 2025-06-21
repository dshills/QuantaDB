# QuantaDB TODO List

## Phase 5: Distributed Query Planning (Next)

**Testing & Validation**:
- [ ] TPC-H benchmark implementation (queries 3, 5, 8, 10)
- [ ] Performance regression detection framework
- [ ] Cost model validation and calibration
- [ ] Query plan comparison and analysis tools

## Technical Debt & Architecture Improvements

### High Priority
- [ ] **Connection Refactoring**: Refactor 900+ line Connection struct into smaller components (see docs/planning/connection-refactor-plan.md)
- [ ] **SSL/TLS Support**: Implement SSL/TLS for secure connections
- [ ] **Authentication System**: Implement proper authentication instead of accepting all connections
- [ ] **Error Handling**: Replace string-based error matching in connection.go with proper error types

### Medium Priority
- [ ] **CLI Tools**: Implement functionality for `cmd/quantactl` and `cmd/test-client` (see TODO.md files in those directories)
- [ ] **Integration Tests**: Add comprehensive integration tests for disk-backed storage
- [ ] **Extended Query Protocol Tests**: Add tests for timeout, SSL, and protocol error scenarios
- [ ] **Module Decoupling**: Reduce tight coupling between network, parser, planner, executor, and storage modules

### Low Priority
- [ ] **In-Memory Engine Concurrency**: Replace single RWMutex with more granular locking for better scalability
- [ ] **Buffer Pool Eviction Policy**: Add configurable eviction policy to handle memory pressure
- [ ] **Connection Backpressure**: Add queuing mechanism for connection handling beyond MaxConnections
- [ ] **Parallel Abstractions**: Consolidate in-memory engine and MVCC storage backend abstractions

## Future Roadmap (from ROADMAP.md)

### Enterprise Features
- Authentication & user management
- Role-based access control
- Backup & recovery tools
- Monitoring & metrics
- Admin dashboard

### Advanced SQL
- Common Table Expressions (CTEs)
- Window functions
- Stored procedures & triggers
- JSON/JSONB support
- Array types

### Distributed Features
- Streaming replication
- Horizontal sharding
- Raft consensus
- Distributed transactions

