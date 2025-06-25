# QuantaDB Documentation

Welcome to the QuantaDB documentation! This directory contains comprehensive information about the database system.

## 📚 Main Documentation

### Getting Started
- **[Project README](../README.md)** - Quick start and overview
- **[Current Status](CURRENT_STATUS.md)** - Detailed component status and capabilities
- **[Roadmap](ROADMAP.md)** - Future development plans and milestones

### Technical Documentation
- **[Architecture Overview](architecture/overview.md)** - System design and component interactions
- **[Reference Documentation](reference/)** - Technical specifications and design documents

## 🎯 Project Status

**QuantaDB has achieved 100% TPC-H benchmark coverage!** All 22 complex analytical queries are working, including the challenging Q21 (Suppliers Who Kept Orders Waiting) with multiple correlated EXISTS/NOT EXISTS predicates.

### Key Achievements
- ✅ Complete PostgreSQL wire protocol compatibility
- ✅ Full ANSI SQL support with advanced features
- ✅ ACID transactions with MVCC isolation
- ✅ Write-Ahead Logging with crash recovery
- ✅ B+Tree indexes with cost-based optimization
- ✅ Complex query processing:
  - Correlated subqueries (EXISTS/NOT EXISTS, scalar)
  - Multiple correlation predicates in single query
  - Common Table Expressions (CTEs)
  - Advanced aggregations and window functions
  - Semi/anti joins for efficient subquery execution

## 📖 Documentation Structure

```
docs/
├── README.md              # This file - documentation index
├── CURRENT_STATUS.md       # Detailed component status
├── ROADMAP.md             # Future development plans
├── architecture/          # System architecture documentation
│   └── overview.md        # High-level system design
├── reference/             # Technical reference documentation
│   ├── CATALOG_DESIGN.md  # Schema and metadata design
│   ├── TECHNICAL_DEBT.md  # Known technical debt items
│   ├── driver-compatibility-report.md
│   └── error-mapping.md
└── archive/               # Historical planning documents
    └── ...                # Completed implementation plans
```

## 🚀 Quick Links

- **[TPC-H Benchmark Status](CURRENT_STATUS.md#tpc-h-progress)** - All 22 queries working
- **[Performance Characteristics](../README.md#performance--benchmarks)** - Benchmark results
- **[Building from Source](../README.md#building-from-source)** - Setup instructions
- **[Architecture Diagram](CURRENT_STATUS.md#architecture-overview)** - System overview

## 📝 Contributing to Documentation

Documentation improvements are welcome! Please:

1. Keep technical accuracy high
2. Update status when features change
3. Maintain clear, concise explanations
4. Follow existing formatting conventions

## 🔗 External Resources

- **[PostgreSQL Wire Protocol](https://www.postgresql.org/docs/current/protocol.html)** - Protocol specification
- **[TPC-H Benchmark](http://www.tpc.org/tpch/)** - Analytical benchmark standard
- **[Go Database/SQL](https://pkg.go.dev/database/sql)** - Go database interfaces

---

*Last updated: December 2024*