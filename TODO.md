# QuantaDB TODO List

## Recently Completed ✓

### Phase 4: Query Transformation Enhancements (COMPLETED)
- [x] **Subquery Planning**: Basic scalar subquery support in planner
- [x] **Subquery Execution**: SubqueryOperator with result caching
- [x] **Subquery Decorrelation**: Transform correlated subqueries to joins
- [x] **EXISTS/IN Support**: Transform to SEMI/ANTI joins
- [x] **CTE Planning**: WITH clause parsing and planning infrastructure
- [x] **Projection Pushdown**: Push projections through operators

### PostgreSQL Wire Protocol (COMPLETED)
- [x] **lib/pq Compatibility**: Full compatibility with Go PostgreSQL driver
- [x] **Required Parameters**: All 13 required parameter status messages
- [x] **SSL Negotiation**: Proper handling of SSL request/response flow
- [x] **Empty Query Support**: Handle ";" queries for connection testing
- [x] **Message Buffering**: Single flush pattern for startup messages

### TPC-H Infrastructure (COMPLETED)
- [x] **Schema Definition**: All 8 TPC-H tables defined
- [x] **Data Generator**: Configurable scale factor data generation
- [x] **Query Templates**: Queries 3, 5, 8, 10 implemented
- [x] **Benchmark Runner**: Performance measurement framework

### Date/Time Functions (COMPLETED)
- [x] **EXTRACT Function**: Full implementation of EXTRACT(field FROM date/timestamp)
  - Parser support for EXTRACT(field FROM expression) syntax
  - Support for all standard fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND  
  - Planner expression type and conversion logic
  - Executor evaluation with proper type handling and error cases
  - Comprehensive test coverage for parsing and evaluation

### SQL Expressions (COMPLETED)
- [x] **CASE Expressions**: Full implementation of CASE WHEN expressions
  - Simple CASE: CASE expr WHEN val1 THEN result1 ELSE default END
  - Searched CASE: CASE WHEN cond1 THEN result1 ELSE default END
  - Parser support with proper precedence handling
  - Planner expression type with type inference
  - Executor evaluation for both simple and searched forms
  - Support for nested CASE expressions
  - Integration with aggregate functions (TPC-H Q8 style)
- [x] **Subqueries in SELECT**: Scalar subqueries in projection list
  - Parser support for subquery expressions
  - Planner integration with SubqueryExpr
  - Executor subqueryEvaluator connected to SubqueryOperator
  - Support for aggregates without GROUP BY in subqueries
  - Int32 support in aggregate functions (AVG, SUM)
- [x] **Date Arithmetic**: Date/time arithmetic with INTERVAL types
  - INTERVAL data type with months, days, and seconds components
  - Parser support for INTERVAL literals (e.g., INTERVAL '1 day')
  - Date/Timestamp + INTERVAL operations
  - Date/Timestamp - INTERVAL operations
  - Date - Date = INTERVAL (returns days)
  - Timestamp - Timestamp = INTERVAL
  - Interval + Interval operations
  - Interval * Scalar multiplication
  - Comprehensive test coverage for all operations

### JOIN Support (COMPLETED)
- [x] **JOIN Syntax**: Full implementation of SQL JOIN operations
  - Parser support for all standard JOIN types: INNER, LEFT, RIGHT, FULL, CROSS
  - Support for explicit JOIN syntax with ON conditions
  - Support for comma-separated tables (implicit CROSS JOIN)
  - Table aliases with AS keyword or implicit
  - Qualified column references (table.column)
  - Subqueries in FROM clause with aliases
  - Planner integration to build LogicalJoin from parsed AST
  - Leveraged existing executor infrastructure (HashJoinOperator, NestedLoopJoinOperator, MergeJoinOperator)

## Phase 5: SQL Feature Completion (Next)

### Query Features (Required for TPC-H)
  - Modified expression building to pass executor reference for subquery building
  - Connected subqueryEvaluator to build SubqueryOperator from logical plan
  - Fixed planner to handle aggregates without GROUP BY clause
  - Added int32 support to AVG and SUM aggregate functions
- [x] **Aggregate Functions**: Implement SUM, AVG, MIN, MAX with proper type handling (COMPLETED)
  - Parser support for function calls including COUNT(*) and COUNT(DISTINCT)
  - Planner integration with aggregate operator pipeline
  - Executor already had aggregate support, now fully connected
  - Mixed numeric type operations (int64/float64) in expressions
- [x] **GROUP BY**: Support multi-column grouping and HAVING clause (COMPLETED)
  - Parser support for GROUP BY and HAVING clauses
  - Planner builds proper aggregate pipeline with grouping
  - Integration with aggregate functions and expressions
- [x] **ORDER BY**: Implement multi-column ordering with ASC/DESC (COMPLETED - was already implemented)
- [x] **Date Literals**: Support for date 'YYYY-MM-DD' syntax (COMPLETED)
- [x] **EXTRACT Function**: Support EXTRACT(field FROM date/timestamp) for YEAR, MONTH, DAY, HOUR, MINUTE, SECOND (COMPLETED)
- [ ] **String Functions**: SUBSTRING, string concatenation (||)
- [x] **CASE Expressions**: Implement CASE WHEN for conditional logic (COMPLETED)
- [ ] **IN/NOT IN**: Support for value lists and subqueries
- [ ] **EXISTS/NOT EXISTS**: Correlated subquery support

### Protocol & Client Compatibility
- [ ] **Parameterized Queries**: Support $1, $2 style parameters (required by lib/pq)
- [ ] **Extended Query Protocol**: Parse/Bind/Execute message flow
- [ ] **Data Type Serialization**: Fix int64 vs int32 issues in protocol
- [ ] **COPY Protocol**: Bulk data loading support
- [ ] **Prepared Statements**: Named statement caching

### SQL DDL Features
- [ ] **DROP TABLE**: Implement table deletion
- [ ] **ALTER TABLE**: Add/drop columns, change data types
- [ ] **CREATE INDEX**: B+Tree index creation (integrate with planner)
- [ ] **Foreign Keys**: Referential integrity constraints
- [ ] **CHECK Constraints**: Custom validation rules

### Data Types
- [ ] **NUMERIC/DECIMAL**: Arbitrary precision decimal numbers
- [ ] **REAL/DOUBLE PRECISION**: Floating point types
- [ ] **DATE/TIME/TIMESTAMP**: Proper date/time handling
- [ ] **BOOLEAN**: True/false values
- [ ] **TEXT**: Variable-length strings without limit
- [ ] **BYTEA**: Binary data type

## Phase 6: Performance & Benchmarking

**Testing & Validation**:
- [ ] Complete TPC-H benchmark suite (all 22 queries)
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

