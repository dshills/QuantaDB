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
- [x] **String Functions**: SUBSTRING, string concatenation (||) (COMPLETED)
- [x] **CASE Expressions**: Implement CASE WHEN for conditional logic (COMPLETED)
- [x] **IN/NOT IN**: Support for value lists and subqueries (COMPLETED)
- [x] **EXISTS/NOT EXISTS**: Correlated subquery support (COMPLETED)

### Protocol & Client Compatibility
- [x] **Parameterized Queries**: Support $1, $2 style parameters (required by lib/pq) (COMPLETED)
- [x] **Extended Query Protocol**: Parse/Bind/Execute message flow (COMPLETED)
- [x] **Data Type Serialization**: Fix int64 vs int32 issues in protocol (COMPLETED)
- [x] **COPY Protocol**: Bulk data loading support (COMPLETED)
  - Parser support for COPY statement with FROM/TO, STDIN/STDOUT/file, column lists, and WITH options
  - AST node CopyStmt with support for CSV, TEXT, and BINARY formats (BINARY not yet implemented)
  - Token additions for COPY-related keywords (COPY, TO, FROM, STDIN, STDOUT, DELIMITER, FORMAT, CSV, BINARY)
  - Planner support with LogicalCopy plan type
  - Executor CopyOperator implementation for bulk data import (COPY FROM)
  - Support for TEXT and CSV formats with configurable delimiters
  - Support for HEADER option in CSV format
  - Proper NULL value handling (\\N in TEXT format)
  - Escape sequence processing for TEXT format
  - Error handling with line number reporting
  - Integration with storage backend for efficient row insertion
  - SetReader/SetWriter methods for STDIN/STDOUT integration with network layer
  - TODO: COPY TO implementation for data export
  - TODO: BINARY format support
  - TODO: Network layer integration for COPY DATA protocol messages
- [x] **Prepared Statements**: Named statement caching (COMPLETED)
  - Parser support for PREPARE, EXECUTE, and DEALLOCATE statements
  - AST nodes for PrepareStmt, ExecuteStmt, and DeallocateStmt
  - Support for parameter type specifications in PREPARE
  - Parser functions for handling parameter lists and AS clause
  - Planner support with LogicalPrepare, LogicalExecute, and LogicalDeallocate plan types
  - Executor operators: PrepareOperator, ExecuteOperator, DeallocateOperator
  - Basic implementation stores prepared statements in ExecContext
  - Support for SQL-level prepared statements (different from protocol-level)
  - Comprehensive test coverage for parser and planner
  - TODO: Full EXECUTE implementation with parameter substitution
  - TODO: Integration with Extended Query Protocol for protocol-level prepared statements

### SQL DDL Features
- [x] **DROP TABLE**: Implement table deletion
  - Parser support for DROP TABLE syntax (already existed)
  - Planner support with LogicalDropTable plan type (already existed)
  - Executor DropTableOperator implementation with storage integration
  - Proper error handling for non-existent tables
  - Integration with catalog to remove table metadata
  - Integration with storage backend to remove table data
  - Comprehensive test coverage for unit and integration testing
  - End-to-end testing with PostgreSQL wire protocol
- [x] **ALTER TABLE**: Add/drop columns, change data types
  - Parser support for ALTER TABLE ADD COLUMN and DROP COLUMN syntax
  - AST nodes for AlterTableStmt with action types (ADD/DROP)
  - Planner support with LogicalAlterTableAddColumn and LogicalAlterTableDropColumn plan types
  - Executor AlterTableAddColumnOperator and AlterTableDropColumnOperator implementations
  - Catalog interface extension with AddColumn and DropColumn methods
  - MemoryCatalog implementation of column addition and removal
  - Constraint processing for NOT NULL and other column constraints
  - Proper error handling for duplicate columns, non-existent tables/columns
  - Prevention of dropping the last column from a table
  - Comprehensive test coverage for parser, executor, and integration
  - Support for optional COLUMN keyword in ADD and required in DROP
- [x] **CREATE INDEX**: B+Tree index creation and planner integration
  - Parser support for CREATE INDEX syntax (already existed)
  - Planner support with LogicalCreateIndex plan type (already existed)
  - Executor CreateIndexOperator implementation with storage integration
  - IndexManager integration for B+Tree index creation
  - Catalog interface extension with CreateIndex and DropIndex methods
  - MemoryCatalog implementation of index creation and removal
  - Proper error handling for duplicate indexes, non-existent tables/columns
  - Comprehensive test coverage for unit and integration testing
  - End-to-end testing with PostgreSQL wire protocol
  - Full index scan optimization infrastructure in planner (IndexScan, CompositeIndexScan, IndexOnlyScan)
  - Complete executor support for all index scan types
  - Cost-based optimization for index selection
  - NOTE: Index usage optimization now available (planner bug fixed)
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

