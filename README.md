# QuantaDB

A PostgreSQL-compatible distributed database written in Go, featuring persistent storage, MVCC transactions, and crash recovery.

## Features

### ✅ Implemented
- **PostgreSQL Wire Protocol**: Compatible with standard PostgreSQL clients (psql, pgAdmin, etc.)
- **SQL Support**: Full SQL parser and executor supporting SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, and more
- **Persistent Storage**: Page-based disk storage with buffer pool management
- **MVCC Transactions**: Multi-version concurrency control with multiple isolation levels
- **Crash Recovery**: Write-Ahead Logging (WAL) with automatic recovery on startup
- **Query Optimization**: Cost-based query planner with join reordering and predicate pushdown
- **B+Tree Indexes**: Complete implementation with full query planner integration
- **Data Types**: All major SQL types including DATE, TIMESTAMP, INTERVAL, and BYTEA
- **Advanced SQL**: GROUP BY, HAVING, DISTINCT, JOINs (all types), subqueries, CTEs

### 🚧 In Development
- CASCADE DELETE and CHECK constraint expression parsing
- LIMIT/OFFSET clauses
- Window functions
- Authentication and user management
- Distributed features (replication, sharding)

## Quick Start

### Prerequisites
- Go 1.21 or higher
- Make (for build automation)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/QuantaDB.git
cd QuantaDB

# Build the server and CLI
make build

# Or build individually
make build-server  # Just the server
make build-ctl     # Just the CLI tool
```

### Running the Server

```bash
# Start with default settings
./build/quantadb

# Or with custom settings
./build/quantadb --data ./mydata --port 5432
```

### Connecting with psql

```bash
# Connect using PostgreSQL client
psql -h localhost -p 5432 -U postgres -d quantadb

# Run some queries
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

INSERT INTO users (id, name, email) VALUES 
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com');

SELECT * FROM users WHERE name LIKE 'A%';

UPDATE users SET email = 'alice@newdomain.com' WHERE id = 1;

DELETE FROM users WHERE id = 2;
```

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│ PostgreSQL      │     │ QuantaDB CLI    │
│ Clients (psql)  │     │ (quantactl)     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     │
        ┌────────────▼────────────┐
        │   Network Layer         │
        │ (PostgreSQL Protocol)   │
        └────────────┬────────────┘
                     │
        ┌────────────▼────────────┐
        │    SQL Parser           │
        │ (Lexer + AST Builder)   │
        └────────────┬────────────┘
                     │
        ┌────────────▼────────────┐
        │   Query Planner         │
        │ (Optimizer + Stats)     │
        └────────────┬────────────┘
                     │
        ┌────────────▼────────────┐
        │   Query Executor        │
        │ (Physical Operators)    │
        └────────────┬────────────┘
                     │
        ┌────────────▼────────────┐
        │  Transaction Manager    │
        │      (MVCC + WAL)       │
        └────────────┬────────────┘
                     │
        ┌────────────▼────────────┐
        │   Storage Engine        │
        │ (Pages + Buffer Pool)   │
        └─────────────────────────┘
```

## Development

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific package tests
go test ./internal/storage -v

# Run benchmarks
make bench
```

### Code Quality

```bash
# Format code
make fmt

# Run linter
golangci-lint run

# Vet code
make vet
```

### Project Structure

```
QuantaDB/
├── cmd/
│   ├── quantadb/         # Main server binary
│   └── quantactl/        # CLI management tool
├── internal/
│   ├── catalog/          # Schema and metadata management
│   ├── index/            # B+Tree index implementation
│   ├── network/          # PostgreSQL wire protocol
│   ├── sql/
│   │   ├── parser/       # SQL lexer and parser
│   │   ├── planner/      # Query planner and optimizer
│   │   └── executor/     # Physical execution operators
│   ├── storage/          # Page-based storage engine
│   ├── txn/              # Transaction manager (MVCC)
│   └── wal/              # Write-Ahead Logging
├── pkg/                  # Public packages (future)
├── docs/                 # Documentation
└── test/                 # Integration tests
```

## Performance

- **In-Memory Operations**: 880K+ TPS for simple queries
- **Disk-Based Storage**: 8KB pages with LRU buffer pool
- **WAL Performance**: Batched writes with configurable sync behavior
- **Concurrent Access**: MVCC allows multiple readers without blocking writers

## Documentation

- [Current Status](docs/CURRENT_STATUS.md) - Detailed component status
- [Roadmap](docs/ROADMAP.md) - Future development plans
- [Architecture](docs/architecture/overview.md) - System design details
- [TODO List](TODO.md) - Current tasks and priorities
- [Contributing](CONTRIBUTING.md) - How to contribute

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on:
- Code style and standards
- Testing requirements
- Pull request process
- Issue reporting

## License

QuantaDB is licensed under the MIT License. See [LICENSE](LICENSE) file for details.

## Acknowledgments

This project was inspired by:
- PostgreSQL's robust architecture and wire protocol
- CMU's Database Systems course materials
- The Go database/sql ecosystem

## Contact

- GitHub Issues: Bug reports and feature requests
- Discussions: Design discussions and questions
- Email: [your-email@example.com]

---

**Note**: QuantaDB is an educational/experimental project and not recommended for production use yet.