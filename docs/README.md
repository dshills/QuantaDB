# QuantaDB Documentation

This directory contains comprehensive documentation for QuantaDB organized by category.

## Quick Start

- **[Current Status](CURRENT_STATUS.md)** - Detailed status of all components (Updated Dec 2024)
- **[Roadmap](ROADMAP.md)** - Future development plans
- **[Architecture Overview](architecture/overview.md)** - System design and architecture
- **[Active TODO List](../TODO.md)** - Current tasks and priorities

## Recent Updates (December 2024)

### Fixes and Improvements
- [Test Fixes](archive/test-fixes-december-2024.md) - Column resolution and date arithmetic fixes
- [JOIN Column Resolution](join-column-resolution-fix.md) - Fixed qualified column name resolution
- [GROUP BY Crash Fix](group-by-crash-fix-summary.md) - Fixed server crash on GROUP BY
- [Aggregate Expressions](aggregate-expressions-fix.md) - Fixed aggregate functions in projections
- [DISTINCT Support](distinct-implementation-summary.md) - Added DISTINCT clause support

### PostgreSQL Compatibility
- [libpq Success Story](libpq-compatibility-success.md) - Full PostgreSQL client compatibility achieved
- [Connection Fixes](libpq-fix-summary.md) - SSL negotiation and stability improvements

## Directory Structure

### `/reference/` - Reference Documentation
Technical specifications and design documents:
- `CATALOG_DESIGN.md` - Database catalog system design
- `TECHNICAL_DEBT.md` - Known technical debt and improvement areas
- `driver-compatibility-report.md` - Database driver compatibility analysis
- `error-mapping.md` - Error code mapping and handling

### `/implementation/` - Implementation Guides
Detailed implementation documentation:
- `IMPLEMENTATION_PLAN.md` - Overall implementation strategy
- `QUERY_EXECUTOR_DESIGN.md` - Query execution engine design
- `QUERY_PLANNER_DESIGN.md` - Query planning and optimization
- `phase2-query-optimization.md` - Query optimizer implementation
- `phase3-advanced-indexing.md` - B+Tree index integration

### `/planning/` - Planning Documents
Active development plans:
- `implementation-plan-dec-2024.md` - Current month priorities
- `phase1-critical-fixes-plan.md` - Critical bug fixes
- `query-optimization-roadmap.md` - Optimizer improvements
- `tpch-benchmark-plan.md` - TPC-H benchmark strategy

### `/architecture/` - Architecture Documentation
High-level system architecture:
- `overview.md` - System architecture overview

### `/development/` - Development Documentation
Development processes and workflows:
- `implementation-roadmap.md` - Long-term development roadmap

### `/archive/` - Historical Documents
Completed plans and historical documentation for reference.

## Component Documentation

- [Storage Engine](../internal/storage/README.md) - Page-based storage with buffer pool
- [Transaction Manager](../internal/txn/README.md) - MVCC implementation
- [Network Layer](../internal/network/README.md) - PostgreSQL wire protocol
- [Index Manager](../internal/index/README.md) - B+Tree indexes

## Test Documentation

- [Driver Tests](../test/drivers/README.md) - PostgreSQL driver compatibility tests
- [TPC-H Benchmark](../test/tpch/README.md) - Performance benchmarking

## Documentation Standards

- Update `CURRENT_STATUS.md` when completing major features
- Archive completed planning documents to `/archive/`
- Include implementation status in all documents
- Use clear, descriptive filenames