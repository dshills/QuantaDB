# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QuantaDB is a PostgreSQL-compatible distributed database written in Go. The project now has persistent storage, SQL query execution, and transaction support.

## Current Status (December 2024)

- ✅ PostgreSQL wire protocol implementation
- ✅ Full SQL parser and query planner
- ✅ Disk-based storage with buffer pool
- ✅ MVCC transaction support
- ✅ B+Tree index implementation
- ✅ Storage integration with UPDATE/DELETE operations
- ✅ Write-Ahead Logging (WAL) with crash recovery
- ❌ Index-Query Planner integration
- ❌ Distributed features

## Tech Stack

- **Language**: Go 1.24.4
- **Module**: github.com/dshills/QuantaDB

## Development Commands

Build and run commands:
- **Build**: `make build` or `make build-server` / `make build-ctl`
- **Run server**: `make run` or `./build/quantadb`
- **Test**: `make test`
- **Test with coverage**: `make test-coverage`
- **Format code**: `make fmt`
- **Vet code**: `make vet`
- **Clean**: `make clean`

Go-specific commands:
- **Test single package**: `go test ./internal/engine -v`
- **Run benchmarks**: `make bench`
- **Install dev dependencies**: `make dev-deps`

## Project Structure

```
QuantaDB/
├── cmd/
│   ├── quantadb/         # Main server binary
│   └── quantactl/        # CLI management tool
├── internal/             # Private packages
│   ├── engine/          # Storage engine interface and implementations
│   ├── cluster/         # Distributed systems logic (TBD)
│   ├── network/         # Network layer (TBD)
│   ├── query/           # Query processing (TBD)
│   ├── config/          # Configuration management (TBD)
│   ├── log/             # Structured logging framework
│   └── testutil/        # Testing utilities
├── pkg/                 # Public packages
│   ├── client/          # Go client library (TBD)
│   └── protocol/        # Wire protocol definitions (TBD)
├── test/                # Integration tests (TBD)
└── docs/                # Documentation (TBD)
```

## Key Components

- **SQL Parser** (`internal/sql/parser/`): Complete SQL parser with lexer and AST
- **Query Planner** (`internal/sql/planner/`): Logical and physical query planning
- **Query Executor** (`internal/sql/executor/`): Physical operators and storage integration
- **Storage Engine** (`internal/storage/`): Page-based disk storage with buffer pool
- **Transaction Manager** (`internal/txn/`): MVCC-based transaction support
- **Network Layer** (`internal/network/`): PostgreSQL wire protocol implementation
- **Index Manager** (`internal/index/`): B+Tree implementation for indexes
- **Catalog** (`internal/catalog/`): Schema and metadata management
- **Write-Ahead Log** (`internal/wal/`): Durability and crash recovery system

## Development Notes

- Follow standard Go project layout conventions
- Use Go modules for dependency management
- Implement comprehensive testing for database reliability
- Consider performance implications in all design decisions
- Storage integration is complete - CREATE TABLE, INSERT, UPDATE, DELETE all work
- PostgreSQL client connections are now stable with proper SSL handling
- WAL provides durability and crash recovery capabilities
- Indexes exist but aren't integrated with query planning yet

## Linting

- Run golangci-lint run before committing code, fix any errors

## Commit Guidelines

- Run tests before committing, fix any errors

## Memory Management

- Use files in docs directory for planning and task mgmt
- When starting a new task review ./TODO.md and planning documents in docs/