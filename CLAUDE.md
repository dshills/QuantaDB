# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QuantaDB is a distributed, high-performance database written in Go. This is an early-stage project with minimal initial structure.

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

- **Storage Engine** (`internal/engine/`): Pluggable storage interface with in-memory implementation
- **Logging** (`internal/log/`): Structured logging using slog with JSON/text output
- **Test Utilities** (`internal/testutil/`): Helper functions for testing

## Development Notes

- Follow standard Go project layout conventions
- Use Go modules for dependency management
- Implement comprehensive testing for database reliability
- Consider performance implications in all design decisions

## Linting

- Run golangci-lint run before committing code, fix any errors

## Commit Guidelines

- Run tests before committing, fix any errors