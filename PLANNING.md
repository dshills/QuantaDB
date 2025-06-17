# PLANNING.md

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
TBD

## Testing strategy
TBD

## Development commands
TBD

## Environment setup
TBD

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

## Future considerations
- JOIN operations (start with nested loop, add hash/merge joins)
- Aggregation functions (SUM, COUNT, AVG, etc.)
- Window functions
- Stored procedures
- Triggers
- Views and materialized views
- Full-text search
- JSON/JSONB data type
- Geospatial support
- Query optimization improvements
- Parallel query execution
- Column-store option for analytics