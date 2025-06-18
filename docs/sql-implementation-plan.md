# SQL Implementation Plan for QuantaDB

## Phase 1: Foundation (Current Focus)

### 1. SQL Type System
Create the foundational data types that all SQL operations will use.

**Location**: `internal/sql/types/`

```go
type DataType interface {
    Name() string
    Size() int
    Compare(a, b Value) int
    Serialize(v Value) []byte
    Deserialize([]byte) Value
}
```

**Initial Types**:
- INTEGER (int32/int64)
- VARCHAR(n)
- BOOLEAN
- TIMESTAMP
- DECIMAL(p,s)

### 2. Table Schema & Catalog
Define how tables and their metadata are represented.

**Location**: `internal/catalog/`

```go
type Table struct {
    ID       uint32
    Name     string
    Columns  []Column
    Indexes  []Index
    Constraints []Constraint
}

type Column struct {
    Name     string
    Type     DataType
    Nullable bool
    Default  Value
}
```

### 3. SQL Parser
Build a parser that converts SQL text into an Abstract Syntax Tree (AST).

**Location**: `internal/sql/parser/`

**Initial Support**:
- CREATE TABLE
- INSERT INTO ... VALUES
- SELECT ... FROM ... WHERE
- Basic expressions and predicates

### 4. Storage Engine Redesign
Convert from key-value to row-based storage.

**Location**: `internal/storage/`

**Components**:
- Row format with null bitmap
- Page-based storage (8KB pages)
- B+Tree implementation for indexes
- Table heap files

### 5. Query Executor
Execute parsed SQL statements against the storage engine.

**Location**: `internal/sql/executor/`

**Initial Operators**:
- Table Scan
- Index Scan
- Filter (WHERE)
- Project (SELECT columns)
- Insert
- Simple nested loop join

## Phase 2: Core SQL Features

### 6. Query Planner
Convert AST into optimized execution plans.

**Location**: `internal/sql/planner/`

- Cost-based optimization
- Statistics collection
- Join order selection

### 7. Transaction Manager
MVCC-based transaction support.

**Location**: `internal/transaction/`

- BEGIN/COMMIT/ROLLBACK
- Isolation levels
- Deadlock detection

### 8. PostgreSQL Wire Protocol
Allow standard SQL clients to connect.

**Location**: `internal/network/pgwire/`

- Authentication
- Query protocol
- Prepared statements
- Result set streaming

## Phase 3: Advanced Features

### 9. Advanced SQL Support
- Aggregate functions
- GROUP BY/HAVING
- Subqueries
- Common Table Expressions (WITH)
- Window functions

### 10. Distributed Features
- Table partitioning
- Distributed transactions
- Query routing
- Data replication

## Implementation Order

1. **Week 1**: SQL types and table schema
2. **Week 2**: Basic SQL parser (CREATE TABLE, INSERT, simple SELECT)
3. **Week 3**: Row-based storage engine
4. **Week 4**: Basic query executor
5. **Week 5**: PostgreSQL wire protocol basics

## Success Metrics

- Can create tables with various data types
- Can insert and query data using standard SQL
- Passes basic SQL compliance tests
- Compatible with standard PostgreSQL drivers

## Design Decisions

1. **Why PostgreSQL wire protocol?**
   - Instant compatibility with existing tools
   - Well-documented protocol
   - Supports advanced features we'll need

2. **Why row-based storage?**
   - Natural fit for OLTP workloads
   - Simpler to implement initially
   - Can add columnar later for analytics

3. **Why custom parser vs library?**
   - Full control over SQL dialect
   - Better error messages
   - Learning opportunity
   - Can switch to vitess parser later if needed