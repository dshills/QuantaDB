# PLANNING.md

## Project Status Summary
**Current Version**: v0.1.0-alpha  
**Development Stage**: Pre-production (Core functionality complete)  
**Key Achievement**: PostgreSQL-compatible SQL database with MVCC transactions

### What Works Today
- ✅ Connect with any PostgreSQL client (psql, pgAdmin, etc.)
- ✅ Execute SELECT queries with WHERE, ORDER BY, LIMIT
- ✅ JOIN operations (Hash Join, Nested Loop)
- ✅ Aggregate functions with GROUP BY
- ✅ Transaction support with full MVCC
- ✅ Multiple concurrent connections

### What's Missing for Production
- ❌ Data persistence (memory-only currently)
- ❌ INSERT, UPDATE, DELETE operations
- ❌ Indexes for performance
- ❌ Authentication and security
- ❌ Distributed capabilities

## Project Overview
QuantaDB is a distributed, high-performance SQL database designed for scalability and reliability. It aims to provide:
- Full SQL compatibility (starting with core SQL-92 features)
- ACID compliance
- Distributed consensus for high availability
- Horizontal scaling through sharding
- Built-in replication and fault tolerance
- Support for standard SQL data types and operations

## Architecture
### Core components (API, Data, Service layers, configuration, etc)
- **SQL Parser**: Parses and validates SQL queries, builds AST
- **Query Planner**: Generates optimal execution plans
- **Query Executor**: Executes query plans against storage engine
- **Storage Engine**: Row-oriented storage with B+Tree indexes
- **Catalog Manager**: Manages database schemas, tables, indexes
- **Type System**: Handles SQL data types and conversions
- **Transaction Manager**: ACID transactions with MVCC
- **Cluster Manager**: Handles node discovery, membership, and health checks
- **Consensus Module**: Implements Raft for distributed consensus
- **Network Layer**: PostgreSQL wire protocol + gRPC for inter-node
- **Replication Manager**: Handles data replication and consistency

### Data Model
- Relational model with tables, rows, and columns
- Support for standard SQL types (INTEGER, VARCHAR, TIMESTAMP, etc.)
- Primary keys and unique constraints
- B+Tree indexes for efficient queries
- Foreign key support (future)
- Transaction support with MVCC (Multi-Version Concurrency Control)
- Table partitioning through sharding

## API endpoints
- **SQL Interface**: PostgreSQL wire protocol for SQL queries
- **HTTP API** (future):
  - `/api/v1/query` - Execute SQL queries
  - `/api/v1/health` - Health check
  - `/api/v1/metrics` - Performance metrics
  - `/api/v1/cluster/status` - Cluster status
- **Admin gRPC API**: Internal cluster management

## Technology stack (Language, frameworks, etc)
- **Language**: Go 1.24.4
- **SQL Parser**: Custom recursive descent parser (or consider using vitess parser)
- **Network**: PostgreSQL wire protocol for clients, gRPC for inter-node
- **Serialization**: Protocol Buffers for internal communication
- **Storage Format**: Custom binary format for row storage
- **Indexing**: B+Tree implementation
- **Testing**: Go standard testing package + testify
- **Logging**: slog (Go standard structured logging)
- **Build**: Make + Go modules

## Project structure
```
QuantaDB/
├── cmd/
│   ├── quantadb/         # Main server binary
│   └── quantactl/        # CLI management tool
├── internal/             # Private packages
│   ├── engine/          # Storage engine interface and implementations (COMPLETED)
│   ├── log/             # Structured logging framework (COMPLETED)
│   ├── sql/             # SQL processing
│   │   ├── parser/      # SQL parser with lexer and AST (COMPLETED)
│   │   ├── types/       # SQL type system (COMPLETED)
│   │   ├── planner/     # Query planner (COMPLETED)
│   │   └── executor/    # Query executor (COMPLETED)
│   ├── catalog/         # Table/schema metadata (COMPLETED)
│   ├── txn/             # Transaction management with MVCC (COMPLETED)
│   ├── network/         # PostgreSQL wire protocol (COMPLETED)
│   ├── cluster/         # Distributed systems logic (TODO)
│   └── testutil/        # Testing utilities (COMPLETED)
├── pkg/                 # Public packages
│   ├── client/          # Go client library (TODO)
│   └── protocol/        # Wire protocol definitions (TODO)
├── test/                # Integration tests (TODO)
└── docs/                # Documentation (TODO)
```

## Testing strategy
- Unit tests for all packages with target >80% coverage
- Integration tests for SQL compliance
- Property-based testing for parser and type system
- Benchmark tests for performance-critical paths
- Load testing framework for distributed scenarios
- Current test coverage:
  - Storage engine: 90.4%
  - SQL parser: 83.2%
  - SQL types: 78.6%
  - SQL planner: 58.9%
  - SQL executor: 58.2%
  - Catalog: 78.3%
  - Transaction manager: 77.1%
  - Network layer: Basic tests
  - Logging: 79.2%
  - Testutil: 70.6%

## Development commands
```bash
make build         # Build both server and CLI
make test          # Run tests (quiet, shows only failures)
make test-verbose  # Run tests with detailed output
make test-coverage # Run tests with coverage report
make bench         # Run benchmarks
make fmt           # Format code
make vet           # Run go vet
make lint          # Run golangci-lint
make clean         # Clean build artifacts
```

## Quick Start
```bash
# Clone and build
git clone https://github.com/dshills/QuantaDB
cd QuantaDB
make build

# Start the server
./build/quantadb --port 5432

# Connect with psql (in another terminal)
psql -h localhost -p 5432 -U user -d database

# Try some queries
SELECT * FROM users;
SELECT name, COUNT(*) FROM users GROUP BY name;
SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id;
```

## Environment setup
1. Install Go 1.24.4 or later
2. Clone repository: `git clone https://github.com/dshills/QuantaDB`
3. Install development dependencies: `make dev-deps`
4. Run tests: `make test`
5. Build: `make build`

## Development guidelines
- Start with single-node implementation, design for distribution
- Focus on correctness over performance initially
- Write comprehensive tests for SQL compliance
- Use interfaces for pluggable components
- Follow Go best practices and idiomatic code
- Document SQL dialect differences clearly

## Security considerations
- SQL injection prevention through parameterized queries
- Authentication and authorization system
- TLS for client connections and inter-node communication
- Role-based access control (RBAC)
- Audit logging for all operations
- Encryption at rest (future)

## Current Status
### Completed Components
- **SQL Parser**: Full SQL parsing with AST generation
- **Type System**: Complete SQL data types (INTEGER, VARCHAR, TIMESTAMP, etc.)
- **Query Planner**: Logical and physical query planning with cost-based optimization
- **Query Executor**: Full operator pipeline (scan, filter, join, aggregate, sort, project, limit)
- **Catalog Manager**: Schema and table metadata management
- **Transaction Manager**: MVCC implementation with isolation levels
- **Network Layer**: PostgreSQL wire protocol v3 implementation
- **Storage Engine**: Memory-based storage with pluggable interface
- **Logging Framework**: Structured logging with slog

### Implemented Features
- Basic SQL operations (SELECT with WHERE, ORDER BY, LIMIT)
- JOIN operations (Hash Join, Nested Loop Join)
- Aggregation functions (SUM, COUNT, AVG, MIN, MAX)
- Transaction support (BEGIN, COMMIT, ROLLBACK)
- MVCC with multiple isolation levels
- PostgreSQL client compatibility
- Expression evaluation (arithmetic, logical, comparison)
- Comprehensive test coverage and code quality

### Production Readiness Status
- ✅ Core SQL engine functional
- ✅ PostgreSQL wire protocol implemented
- ✅ Transaction support with MVCC
- ⚠️  Memory-only storage (no persistence)
- ⚠️  No authentication/authorization
- ⚠️  Limited SQL operations (SELECT only)
- ❌ No distributed capabilities
- ❌ No backup/recovery
- ❌ No monitoring/metrics

### Immediate Next Steps
1. **B+Tree Indexing**: Essential for query performance
2. **Persistent Storage**: Replace memory engine with disk-based storage
3. **DML Operations**: Implement INSERT, UPDATE, DELETE
4. **Authentication**: Add user management and auth
5. **Extended Protocol**: Support prepared statements

### Future Roadmap
- Distributed consensus (Raft)
- Replication and sharding
- Query optimization improvements
- Advanced SQL features (CTEs, window functions)
- Monitoring and observability
- Backup and recovery
- Performance optimizations