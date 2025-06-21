# QuantaDB TODO List

## ✅ Phase 4: Query Transformation Enhancements (COMPLETED)
- [x] Complete projection pushdown implementation - Data flow reduction up to 48.4%
- [x] Add subquery parser support (EXISTS, IN, scalar subqueries) - Full AST support
- [x] Implement basic subquery planning - SubqueryOperator with caching
- [x] Subquery decorrelation and optimization - EXISTS/IN → SEMI/ANTI joins
- [x] Common Table Expression (CTE) support - WITH clause parsing and planning
- [x] EXISTS/IN predicate transformation to semi-joins - Integrated into optimizer

## Phase 5: Distributed Query Planning (Next)

**Testing & Validation**:
- [ ] TPC-H benchmark implementation (queries 3, 5, 8, 10)
- [ ] Performance regression detection framework
- [ ] Cost model validation and calibration
- [ ] Query plan comparison and analysis tools

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

