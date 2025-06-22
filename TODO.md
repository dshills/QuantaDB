# QuantaDB TODO List

## Project Status Summary

**Phase 1-6: COMPLETED** ✅
- Core Storage Engine with MVCC, WAL, and B+Tree Indexes
- Full SQL Parser with comprehensive statement support
- Query Planner with optimization framework
- Query Executor with all major operators
- PostgreSQL Wire Protocol compatibility
- All core SQL data types implemented
- TPC-H benchmark infrastructure ready

**Current Phase: Phase 7 - Performance & Benchmarking**
- TPC-H benchmark suite completion (4/22 queries done)
- Performance optimization and measurement
- Cost model calibration

**Key Achievements:**
- ✅ PostgreSQL-compatible database from scratch
- ✅ ACID transactions with MVCC isolation
- ✅ Write-Ahead Logging with crash recovery
- ✅ B+Tree indexes (created but not used in queries yet)
- ✅ Cost-based query optimizer framework
- ✅ Prepared statements and parameterized queries
- ✅ Full JOIN support (INNER, LEFT, RIGHT, FULL, CROSS)
- ✅ Aggregate functions with GROUP BY/HAVING
- ✅ Subqueries, IN/EXISTS, and CTEs
- ✅ Foreign Keys and CHECK constraints
- ✅ COPY protocol for bulk data loading

## Recently Completed (December 2024) ✓

### TPC-H Benchmark Support
- [x] **INSERT Parameter Support**: Fixed INSERT operator to support parameter references ($1, $2, etc.)
  - Added handling for ParameterRef expressions in addition to literals
  - Created evaluation context with parameter values from ExecContext
  - Enables prepared statements with INSERT to work correctly
  - Critical for TPC-H data loading with parameterized queries
- [x] **FLOAT/DOUBLE PRECISION Data Types**: Added floating-point support
  - Implemented FLOAT (32-bit) and DOUBLE PRECISION (64-bit) types
  - Parser support for FLOAT, REAL (alias), and DOUBLE PRECISION
  - Complete DataType interface with serialization/deserialization
  - Type conversion methods AsFloat() and AsDouble()
  - Required for TPC-H decimal columns
- [x] **COUNT(*) Aggregate Function**: Already implemented, verified working
  - Special handling for Star expressions in aggregate builder
  - Converts COUNT(*) to COUNT(1) internally
- [x] **Index Manager Configuration**: Fixed CREATE INDEX execution
  - Integrated index manager into server initialization
  - Passed through connection context to query executors
  - Fixed CreateIndexOperator result handling logic
  - CREATE INDEX now works successfully

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
### Network Layer Refactoring (COMPLETED)
- [x] **Connection Refactoring**: Broke down 1,381-line Connection struct into 6 components
  - BasicProtocolHandler: Protocol message handling and I/O operations
  - BasicAuthenticationHandler: Authentication methods (cleartext, MD5, none)
  - BasicTransactionManager: Transaction lifecycle management
  - BasicQueryExecutor: Query parsing, planning, and execution
  - BasicPreparedStatementManager: Extended query protocol support
  - BasicResultFormatter: Result formatting and transmission
  - Proper separation of concerns with clear interfaces
  - Improved testability and maintainability
- [x] **SSL/TLS Support**: Secure connection support
  - Server-level SSL configuration with cert/key files
  - SSL upgrade negotiation before connection handling
  - Support for both SSL and non-SSL connections
  - Proper buffering for non-SSL connections after SSL check
  - RequireSSL option to reject non-SSL connections
- [x] **Authentication System**: Replace accept-all with proper auth
  - UserStore interface for credential management
  - InMemoryUserStore implementation
  - Support for "none", "password", and "md5" authentication methods
  - PostgreSQL-compatible MD5 authentication (md5(md5(password + username) + salt))
  - Configurable authentication method per server
  - Example demonstrating password authentication
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
- [x] **String Functions**: String concatenation and SUBSTRING
  - String concatenation operator (||) with proper NULL handling
  - SUBSTRING(string FROM start [FOR length]) function
  - Parser support with TokenConcat and SubstringExpr AST node
  - Planner conversion of SubstringExpr to FunctionCall
  - Executor substringEvaluator implementation
  - Support for 1-based indexing per SQL standard
  - Proper handling of edge cases (negative positions, excessive lengths)
  - Integration with parser_expression.go for testing
  - Comprehensive test coverage including NULL handling
- [x] **IN/NOT IN Expressions**: Value lists and subquery support  
  - Parser support for IN (value_list) and IN (subquery) syntax
  - Planner conversion from parser.InExpr to planner.InExpr with complete convertExpression support
  - Executor evaluation with proper SQL NULL semantics
  - Support for empty IN lists (always evaluates to false)
  - Value list evaluation: comparison with proper type handling
  - Subquery evaluation through existing inSubqueryEvaluator and inValuesEvaluator
  - Proper NULL handling: NULL IN anything = NULL, correct three-valued logic
  - Integration with parser_expression.go for direct expression evaluation
  - Comprehensive test coverage for all cases including edge cases
- [x] **EXISTS/NOT EXISTS Expressions**: Correlated subquery support
  - Parser support for EXISTS (subquery) and NOT EXISTS (subquery) syntax
  - Complete AST node (ExistsExpr) with Subquery and Not fields
  - Planner conversion from parser.ExistsExpr to planner.ExistsExpr
  - Executor existsEvaluator with SubqueryOperator integration
  - Support for both simple EXISTS and correlated EXISTS patterns
  - SubqueryDecorrelation optimization: EXISTS → SEMI JOIN, NOT EXISTS → ANTI JOIN
  - Integration with parser_expression.go (error for subquery evaluation as expected)
  - Fixed ProjectionPushdown optimizer to handle nil schemas in join operations
  - Comprehensive test coverage for parsing, planning, and optimization
- [x] **Parameterized Queries**: Support $1, $2 style parameters for lib/pq compatibility
  - Parser support for ParameterRef AST nodes with $1, $2, etc. syntax  
  - Complete parameter type inference from query context
  - ParameterSubstitutor for replacing placeholders with actual values
  - Support for parameter substitution in all logical plan types (Filter, Project, Sort, Join)
  - Binary and text format parameter value parsing from wire protocol
  - PostgreSQL-compatible boolean parsing ('t'/'f' format)
  - Parameter count validation and out-of-range error handling
  - Integration with network layer for Parse/Bind message handling
  - Comprehensive test coverage for parsing, substitution, and wire protocol integration
- [x] **Extended Query Protocol**: Parse/Bind/Execute message flow
  - Complete Parse message handling with SQL parsing and parameter type inference
  - Bind message handling with parameter value parsing and portal creation
  - Execute message support with result set streaming and row limits
  - ParameterDescription and RowDescription message generation
  - Support for both named and unnamed statements/portals
  - Close message handling for statement and portal cleanup
  - ExtendedQuerySession for managing prepared statements and portals
  - Parameter format handling (text/binary) in Bind messages
  - Result format handling for Execute responses
  - Full integration with planner and executor pipeline through parameter substitution
- [x] **Data Type Serialization**: Fix int64 vs int32 issues in protocol
  - Fixed parser to create int32 values for integers within int32 range (±2,147,483,647)
  - Updated parameter parsing to use int32 for PostgreSQL INTEGER type consistency
  - Fixed parameter value parsing with proper bounds checking for INTEGER type
  - Updated all tests to expect int32 instead of int64 for standard integer values
  - Maintained int64 support for values requiring BIGINT (outside int32 range)
  - Ensured string serialization consistency between int32 and int64 values
  - Fixed wire protocol data type consistency for PostgreSQL compatibility
  - All integer literals now properly map to PostgreSQL INTEGER (int32) by default

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

### Phase 5: SQL Feature Completion (COMPLETED)
- [x] **Foreign Keys**: Referential integrity constraints
- [x] **CHECK Constraints**: Custom validation rules
- [x] **All other Phase 5 features completed previously**: Aggregate functions, GROUP BY, HAVING, Date literals, EXTRACT, String functions, CASE expressions, IN/NOT IN, EXISTS/NOT EXISTS, Parameterized queries, Extended query protocol, COPY protocol, Prepared statements, DROP TABLE, ALTER TABLE, CREATE INDEX


## Phase 6: Data Types & Advanced Features (COMPLETED)

### Data Types (All Core Types Complete) ✅
- [x] **INTEGER/BIGINT/SMALLINT**: Integer types with proper int32/int64/int16 handling
- [x] **NUMERIC/DECIMAL**: Arbitrary precision decimal numbers (implemented with big.Rat)
- [x] **REAL/FLOAT/DOUBLE PRECISION**: Floating point types (completed Dec 2024)
- [x] **VARCHAR/CHAR/TEXT**: String types with optional length limits
- [x] **DATE/TIMESTAMP**: Date and time handling with PostgreSQL compatibility
- [x] **BOOLEAN**: True/false values with proper serialization
- [x] **INTERVAL**: Time interval support for date arithmetic
- [ ] **BYTEA**: Binary data type (only remaining core type)

## Next Immediate Steps

1. **Index-Query Integration**: Indexes are created but not used by query planner
   - Implement index scan operator usage in planner
   - Add cost estimation for index scans vs table scans
   - Update optimizer to choose index scans when beneficial

2. **Run TPC-H Benchmarks**: With all features implemented
   - Load TPC-H data at various scale factors
   - Measure query performance for implemented queries (Q3, Q5, Q8, Q10)
   - Identify performance bottlenecks

3. **Implement Additional TPC-H Queries**: Start with simpler ones
   - Q1: Simple aggregation with GROUP BY
   - Q6: Simple filtered aggregation
   - Q4: EXISTS subquery pattern
   - Q12: CASE expressions in aggregates

## Phase 7: Performance & Benchmarking

### TPC-H Benchmark Status
- [x] **TPC-H Infrastructure**: Complete benchmark framework
  - Schema definitions for all 8 TPC-H tables
  - Data generator with configurable scale factors
  - 4 implemented queries (Q3, Q5, Q8, Q10)
  - Benchmark runner with performance measurement
  - SQL loader utility for data import
- [ ] **Complete TPC-H Suite**: Implement remaining 18 queries
  - Currently 4/22 queries implemented
  - Need to add Q1, Q2, Q4, Q6, Q7, Q9, Q11-Q22
  - Some queries require additional SQL features (see below)

### Performance Infrastructure
- [ ] Performance regression detection framework
- [ ] Cost model validation and calibration
- [ ] Query plan comparison and analysis tools
- [ ] Automated benchmark CI/CD integration

### SQL Features for Remaining TPC-H Queries
- [ ] **Window Functions**: Required for Q2 (rank), Q17, Q18, Q20
- [ ] **Correlated Subqueries in SELECT**: Q2, Q17, Q20, Q21, Q22
- [ ] **Multiple Subqueries**: Q21, Q22 have complex nested subqueries
- [ ] **LIMIT/OFFSET**: Q18 uses LIMIT 100
- [ ] **Additional Aggregate Functions**: STDDEV (Q17)
- [ ] **Query Optimization**: Many queries need better join ordering and index usage

## Technical Debt & Architecture Improvements

### Recently Completed ✓
- [x] **Planner Bug Fix**: Fixed infinite loop creating nested LogicalProject nodes that prevented index optimization
  - Added cycle detection and improved convergence checking in optimizer
  - Reduced plan depth from 103 to 3 through proper optimization termination
  - Enhanced optimizer with plan structure tracking to prevent infinite loops
- [x] **Connection Refactoring**: Refactored 900+ line Connection struct into smaller components
  - Broke down into 6 specialized components with clear separation of concerns
  - ProtocolHandler: Low-level message routing and network operations  
  - AuthenticationHandler: Connection startup and authentication
  - QueryExecutor: SQL query processing through parser/planner/executor
  - TransactionManager: Transaction lifecycle management  
  - ExtendedQueryHandler: Parse/Bind/Execute protocol support
  - ResultFormatter: Converting results to PostgreSQL wire format
  - Created RefactoredConnection with proper component wiring
  - Maintained all existing functionality while improving code organization
- [x] **SSL/TLS Support**: Implemented SSL/TLS for secure connections
  - Added SSL configuration to server Config struct
  - Implemented certificate loading and custom TLS config support
  - Created handleSSLUpgrade with proper SSL request detection
  - Support for both SSL-enabled and SSL-disabled connections
  - Added comprehensive SSL tests with certificate generation
  - Maintained backward compatibility with existing non-SSL connections
- [x] **Authentication System**: Implemented proper authentication instead of accepting all connections
  - Support for multiple auth methods: "none", "password", "md5"
  - UserStore interface for pluggable user credential storage
  - InMemoryUserStore implementation with thread-safe operations
  - MD5 password hashing following PostgreSQL specification
  - Proper salt generation and challenge-response authentication
  - Enhanced server configuration with AuthMethod and UserStore
  - Complete PostgreSQL protocol compliance for authentication flow

### High Priority
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

