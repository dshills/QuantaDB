# Implementation Plan for Top Priority Items

## 1. Query Planner

### Overview
The query planner transforms SQL AST into executable query plans. It will optimize queries and choose the best execution strategy.

### Architecture
```
internal/sql/planner/
├── planner.go          # Main planner interface and implementation
├── plan.go             # Query plan nodes (Scan, Filter, Join, etc.)
├── optimizer.go        # Query optimization rules
├── cost.go             # Cost-based optimization
└── planner_test.go     # Tests
```

### Key Components

#### Plan Nodes
```go
// PlanNode represents a node in the query execution plan
type PlanNode interface {
    Children() []PlanNode
    Schema() *Schema
    Cost() Cost
}

// Common plan nodes:
- TableScan: Read from a table
- IndexScan: Read using an index
- Filter: Apply WHERE conditions
- Project: SELECT specific columns
- Sort: ORDER BY implementation
- Limit: LIMIT/OFFSET implementation
- HashJoin: Join implementation (future)
- NestedLoopJoin: Simple join (initial)
```

#### Implementation Steps
1. Define PlanNode interface and basic nodes
2. Implement simple rule-based planner for initial queries
3. Add predicate pushdown optimization
4. Implement cost model (rows, I/O, CPU)
5. Add statistics collection for better planning

### Milestones
- [ ] Week 1: Basic planner with TableScan, Filter, Project
- [ ] Week 2: Add Sort, Limit, and simple optimizations
- [ ] Week 3: Cost model and statistics framework
- [ ] Week 4: Testing and optimization rules

## 2. Query Executor

### Overview
Executes query plans against the storage engine, implementing the volcano model (iterator-based execution).

### Architecture
```
internal/sql/executor/
├── executor.go         # Main executor and context
├── operators.go        # Operator implementations
├── expressions.go      # Expression evaluation
├── aggregates.go       # Aggregate functions (future)
└── executor_test.go    # Tests
```

### Key Components

#### Execution Model
```go
// Operator interface (Volcano model)
type Operator interface {
    Open(ctx *ExecContext) error
    Next() (*Row, error)
    Close() error
}

// Operators mirror plan nodes:
- ScanOperator
- FilterOperator
- ProjectOperator
- SortOperator
- LimitOperator
```

#### Implementation Steps
1. Define Operator interface and execution context
2. Implement basic operators (Scan, Filter, Project)
3. Add expression evaluation engine
4. Implement Sort and Limit operators
5. Add transaction context support

### Milestones
- [ ] Week 1: Basic executor framework and Scan operator
- [ ] Week 2: Filter and Project operators with expressions
- [ ] Week 3: Sort and Limit operators
- [ ] Week 4: Integration with planner and testing

## 3. Catalog/Metadata System

### Overview
Manages database schemas, tables, indexes, and statistics. Critical for query planning and validation.

### Architecture
```
internal/catalog/
├── catalog.go          # Catalog interface
├── table.go            # Table metadata
├── schema.go           # Schema definitions
├── index.go            # Index metadata
├── stats.go            # Table/column statistics
└── catalog_test.go     # Tests
```

### Key Components

#### Catalog Interface
```go
type Catalog interface {
    CreateTable(schema *TableSchema) error
    GetTable(name string) (*Table, error)
    DropTable(name string) error
    ListTables() ([]string, error)
    
    CreateIndex(index *IndexDef) error
    GetIndex(table, name string) (*Index, error)
    
    UpdateStatistics(table string) error
}
```

#### Storage Format
- System tables approach (like PostgreSQL)
- Tables: _tables, _columns, _indexes, _stats
- Bootstrapping process for system tables

### Implementation Steps
1. Define catalog interfaces and structures
2. Implement in-memory catalog for testing
3. Design system tables schema
4. Implement persistent catalog storage
5. Add statistics collection and management

### Milestones
- [ ] Week 1: Catalog interfaces and in-memory implementation
- [ ] Week 2: System tables design and bootstrapping
- [ ] Week 3: Persistent storage integration
- [ ] Week 4: Statistics framework

## 4. Row-Based Storage Engine

### Overview
Redesign storage engine for row-oriented storage with B+Tree indexes, replacing current key-value design.

### Architecture
```
internal/engine/
├── rowstore/
│   ├── page.go         # Page layout (8KB pages)
│   ├── tuple.go        # Tuple format and access
│   ├── heap.go         # Heap file implementation
│   ├── buffer.go       # Buffer pool manager
│   └── rowstore_test.go
├── btree/
│   ├── btree.go        # B+Tree implementation
│   ├── node.go         # B+Tree nodes
│   ├── iterator.go     # Index scanning
│   └── btree_test.go
```

### Key Components

#### Page Layout
```
Page (8KB):
┌─────────────────┐
│ Page Header     │ (24 bytes)
├─────────────────┤
│ Line Pointers   │ (4 bytes each)
├─────────────────┤
│ Free Space      │
├─────────────────┤
│ Tuple Data      │ (variable length)
└─────────────────┘
```

#### Tuple Format
```
Tuple:
┌─────────────────┐
│ Header (8 bytes)│ (null bitmap, etc.)
├─────────────────┤
│ Column Data     │ (fixed/variable)
└─────────────────┘
```

### Implementation Steps
1. Design page and tuple formats
2. Implement heap file with page management
3. Add buffer pool for caching
4. Implement B+Tree for indexes
5. Integrate with query executor

### Milestones
- [ ] Week 1: Page and tuple format implementation
- [ ] Week 2: Heap file and basic operations
- [ ] Week 3: Buffer pool manager
- [ ] Week 4: B+Tree index implementation
- [ ] Week 5: Integration and testing

## 5. Transaction Manager with MVCC

### Overview
Provides ACID transactions using Multi-Version Concurrency Control (MVCC) for concurrent access.

### Architecture
```
internal/transaction/
├── transaction.go      # Transaction interface
├── mvcc.go            # MVCC implementation
├── timestamp.go       # Timestamp ordering
├── lock.go            # Lock manager (if needed)
├── log.go             # Write-ahead log
└── recovery.go        # Crash recovery
```

### Key Components

#### MVCC Design
```go
// Transaction visibility
type TransactionID uint64
type Timestamp uint64

// Tuple header additions for MVCC
type MVCCHeader struct {
    XMin TransactionID  // Insert transaction
    XMax TransactionID  // Delete transaction
    CMin uint32        // Insert command ID
    CMax uint32        // Delete command ID
}
```

#### Transaction States
- Active
- Committed
- Aborted

### Implementation Steps
1. Design transaction ID generation
2. Add MVCC headers to tuples
3. Implement visibility rules
4. Add transaction manager
5. Implement WAL for durability

### Milestones
- [ ] Week 1: Transaction ID and MVCC headers
- [ ] Week 2: Visibility rules implementation
- [ ] Week 3: Transaction manager
- [ ] Week 4: WAL implementation
- [ ] Week 5: Recovery and testing

## Integration Timeline

### Phase 1 (Weeks 1-4): Foundation
- Query Planner basic implementation
- Query Executor framework
- In-memory catalog
- Basic storage engine redesign

### Phase 2 (Weeks 5-8): Core Features
- Planner optimizations
- Complete executor operators
- Persistent catalog
- B+Tree indexes
- Basic MVCC

### Phase 3 (Weeks 9-12): Advanced Features
- Cost-based optimization
- Statistics and cardinality estimation
- Buffer pool tuning
- Full transaction support
- WAL and recovery

## Testing Strategy

### Unit Tests
- Each component heavily unit tested
- Mock interfaces for isolation
- Property-based testing for complex algorithms

### Integration Tests
- End-to-end SQL query tests
- Transaction isolation tests
- Concurrent access tests
- Performance benchmarks

### SQL Compliance Tests
- Create comprehensive SQL test suite
- Test against SQL-92 standard
- Edge cases and error conditions

## Performance Goals

- Single-table scan: 1M rows/second
- Index lookup: < 1ms for point queries
- Transaction overhead: < 10% for OLTP workloads
- Recovery time: < 30 seconds for 10GB database

## Risk Mitigation

1. **Complexity**: Start simple, iterate
2. **Performance**: Profile early and often
3. **Correctness**: Extensive testing at each layer
4. **Integration**: Clear interfaces between components

## Next Steps

1. Review and refine this plan
2. Set up project structure for new components
3. Begin with query planner implementation
4. Create integration test framework early