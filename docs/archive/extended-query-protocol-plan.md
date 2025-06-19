# Extended Query Protocol Implementation Plan

## Overview
Implement PostgreSQL's Extended Query Protocol to enable prepared statements, parameter binding, and compatibility with database drivers (JDBC, ODBC, psycopg2).

## Current State
- ✅ Simple Query Protocol implemented
- ❌ No prepared statement support
- ❌ No parameter binding
- ❌ Limited driver compatibility

## Goals
1. Full Extended Query Protocol support
2. Prepared statement caching for performance
3. Parameter binding for security
4. JDBC/ODBC driver compatibility

## Architecture

### Core Components

#### 1. PreparedStatement
```go
type PreparedStatement struct {
    Name         string
    SQL          string
    ParseTree    *parser.Statement
    ParamTypes   []types.DataType
    ResultFields []FieldDescription
    QueryPlan    planner.Plan // Optional cached plan
}
```

#### 2. Portal
```go
type Portal struct {
    Name           string
    Statement      *PreparedStatement
    ParamValues    []types.Value
    ParamFormats   []int16 // 0=text, 1=binary
    ResultFormats  []int16
    CurrentRow     int64   // For Execute with limit
}
```

#### 3. ExtendedQuerySession
```go
type ExtendedQuerySession struct {
    Statements map[string]*PreparedStatement
    Portals    map[string]*Portal
    // Unnamed portal is destroyed on Parse
    UnnamedPortal *Portal
}
```

### Message Flow
```
Client                          Server
  |                               |
  |-------- Parse --------------->|  Create PreparedStatement
  |<------- ParseComplete ---------|
  |                               |
  |-------- Bind ---------------->|  Create Portal with params
  |<------- BindComplete ----------|
  |                               |
  |-------- Execute ------------->|  Execute Portal
  |<------- DataRow(s) -----------|
  |<------- CommandComplete ------|
  |                               |
  |-------- Sync ---------------->|  Commit/Rollback
  |<------- ReadyForQuery ---------|
```

## Implementation Phases

### Phase 1: Core Infrastructure (2-3 days)

#### Task 1.1: Extend Parser for Parameters
- Add `TokenParam` to lexer for `$1`, `$2`, etc.
- Create `ParameterRef` expression type
- Update `parseExpression` to handle parameters
- **Test**: Parse "SELECT * FROM users WHERE id = $1"

#### Task 1.2: Create Core Data Structures
- Implement `PreparedStatement` struct
- Implement `Portal` struct  
- Create thread-safe `StatementCache`
- Create `PortalManager`
- **Test**: Unit tests for each component

#### Task 1.3: Session State Management
- Create `ExtendedQuerySession` per connection
- Integrate with `ClientHandler`
- Handle unnamed portal lifecycle
- **Test**: Session state persistence

### Phase 2: Message Handlers (2-3 days)

#### Task 2.1: Parse Message ('P')
```go
func handleParse(session *ExtendedQuerySession, msg *ParseMessage) error {
    // 1. Parse SQL with parameters
    // 2. Infer parameter types if possible
    // 3. Create PreparedStatement
    // 4. Store in session
    // 5. Send ParseComplete
}
```

#### Task 2.2: Bind Message ('B')
```go
func handleBind(session *ExtendedQuerySession, msg *BindMessage) error {
    // 1. Get PreparedStatement
    // 2. Validate parameter count/types
    // 3. Create Portal with values
    // 4. Store in session
    // 5. Send BindComplete
}
```

#### Task 2.3: Execute Message ('E')
```go
func handleExecute(session *ExtendedQuerySession, msg *ExecuteMessage) error {
    // 1. Get Portal
    // 2. Execute with row limit
    // 3. Send DataRows
    // 4. Send CommandComplete
    // 5. Handle suspension if limit reached
}
```

#### Task 2.4: Describe Message ('D')
- Describe prepared statement (parameter types)
- Describe portal (result columns)
- Send ParameterDescription
- Send RowDescription

#### Task 2.5: Close Message ('C')
- Close statement or portal
- Free resources
- Send CloseComplete

### Phase 3: Integration (2-3 days)

#### Task 3.1: Parameter Substitution
- Modify executor to accept parameters
- Implement parameter substitution visitor
- Handle type coercion
- **Test**: Execute queries with parameters

#### Task 3.2: Type Inference
- Infer types from query context
- Handle ambiguous cases
- Support explicit type casts
- **Test**: Various parameter type scenarios

#### Task 3.3: Query Plan Caching
- Decide when to cache plans
- Handle parameter-sensitive plans
- Implement cache invalidation
- **Test**: Performance benchmarks

### Phase 4: Testing & Compatibility (2-3 days)

#### Task 4.1: PostgreSQL Compatibility Tests
- Test with `psql` PREPARE/EXECUTE
- Test all parameter types
- Test error scenarios
- Compare behavior with PostgreSQL

#### Task 4.2: Driver Testing
- JDBC driver integration test
- Python psycopg2 tests
- Go pgx driver tests
- Document any limitations

#### Task 4.3: Performance Testing
- Benchmark prepared vs simple queries
- Measure parsing overhead reduction
- Test with high parameter count
- Profile memory usage

## Technical Decisions

### 1. Parameter Type Inference
- **Option A**: Infer at Parse time (PostgreSQL approach)
- **Option B**: Defer to Bind time
- **Decision**: Start with Parse-time inference, add Bind-time fallback

### 2. Plan Caching
- **Option A**: Cache at Parse time
- **Option B**: Re-plan at each Execute
- **Decision**: Cache plans, invalidate on schema changes

### 3. Binary Format Support
- **Phase 1**: Text format only
- **Phase 2**: Add binary format for common types
- Focus on compatibility over optimization initially

## Success Criteria

1. **Functional Requirements**
   - [ ] JDBC PreparedStatement works
   - [ ] psql PREPARE/EXECUTE works
   - [ ] All PostgreSQL parameter types supported
   - [ ] Proper error handling with SQLSTATE

2. **Performance Requirements**
   - [ ] 2x faster for repeated queries
   - [ ] < 1ms overhead for Parse
   - [ ] Efficient parameter substitution

3. **Compatibility Requirements**
   - [ ] 90% PostgreSQL protocol compliance
   - [ ] Major drivers work (JDBC, psycopg2, pgx)
   - [ ] Existing simple protocol still works

## Risk Mitigation

1. **Complexity Risk**: Start with minimal viable protocol
2. **Compatibility Risk**: Test early with real drivers
3. **Performance Risk**: Profile and optimize critical paths
4. **Scope Creep**: Defer advanced features (cursors, binary formats)

## Example Usage

```sql
-- PostgreSQL compatible prepared statements
PREPARE user_by_id AS SELECT * FROM users WHERE id = $1;
EXECUTE user_by_id(123);

-- JDBC example
PreparedStatement ps = conn.prepareStatement("SELECT * FROM users WHERE age > ? AND city = ?");
ps.setInt(1, 25);
ps.setString(2, "Seattle");
ResultSet rs = ps.executeQuery();
```

## Timeline
- **Total**: 8-11 days
- **Week 1**: Core infrastructure + Parse/Bind
- **Week 2**: Execute/Describe/Close + Testing

## Next Steps
1. Review and approve plan
2. Create GitHub issues for each task
3. Begin Phase 1 implementation
4. Daily progress updates