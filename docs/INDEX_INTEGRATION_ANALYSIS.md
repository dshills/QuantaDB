# Index Integration Analysis - Current State

## Executive Summary

The codebase already has significant index infrastructure in place but lacks the critical integration between the query planner and index system. Most components exist but are not connected.

## What Already Exists ✅

### 1. Index Implementation (`internal/index/`)
- **B+Tree Index**: Fully functional with insert, delete, search, range operations
- **Index Manager**: Manages all indexes, tracks by schema.table.index_name
- **Key Encoder**: Converts SQL values to sortable byte arrays
- **Index Interface**: Well-defined with all necessary operations

### 2. Catalog Support (`internal/catalog/`)
- **Index Metadata**: Table struct includes `Indexes []*Index` field
- **Index Types**: BTreeIndex and HashIndex enums defined
- **Constraints**: Primary key and unique constraints defined
- **Statistics**: TableStats and ColumnStats structures exist

### 3. Query Planner Infrastructure (`internal/sql/planner/`)
- **IndexScan Logical Node**: Already defined in `index_scan.go`
- **Index Selection Functions**:
  - `canUseIndexForFilter()`: Checks if index matches predicates
  - `extractIndexBounds()`: Extracts key ranges from predicates
  - `tryIndexScan()`: Stub for converting scan to index scan
- **IndexSelection Optimizer Rule**: Exists but Apply() is not implemented

### 4. Storage Backend (`internal/sql/executor/`)
- **DiskStorageBackend**: Page-based storage with RowID (PageID + SlotID)
- **Row Iterator**: Interface for scanning tables
- **Buffer Pool Integration**: All disk access goes through buffer pool

## What's Missing ❌

### 1. Parser Support
- No CREATE INDEX statement parsing
- No DROP INDEX statement parsing
- Need to add to SQL grammar and AST

### 2. Index Selection Logic
- `IndexSelection.Apply()` is empty - needs implementation
- No pattern matching for scan+filter → index scan
- No cost comparison logic

### 3. Physical Index Scan
- No `PhysicalIndexScan` node
- No `IndexScanOperator` in executor
- No index-to-heap fetch logic

### 4. Storage Integration
- Index Manager not connected to storage backend
- No index persistence (indexes are in-memory only)
- No index updates during INSERT/UPDATE/DELETE

### 5. Cost Model
- Very basic cost estimation (blocks × 1.0)
- No index-specific cost calculations
- No selectivity-based optimization

## Integration Points

### 1. CREATE INDEX Flow
```
SQL Parser → AST → Planner → Executor → Index Manager → Storage
```

### 2. Query Optimization Flow
```
LogicalScan + Filter → IndexSelection Rule → IndexScan → Physical Planning → IndexScanOperator
```

### 3. Data Modification Flow
```
INSERT/UPDATE/DELETE → Storage Backend → Index Manager → Update All Indexes
```

## Key Discoveries

1. **Index Infrastructure is Ready**: The B+Tree and Index Manager are complete and functional
2. **Planner Has Hooks**: The planner already has index-aware code, just not connected
3. **Storage Uses RowIDs**: Perfect for index integration (PageID + SlotID)
4. **Statistics Framework Exists**: Just needs to be populated and used

## Implementation Strategy

### Phase 1: Parser and DDL (Day 1)
1. Add CREATE/DROP INDEX to SQL grammar
2. Create AST nodes for index operations
3. Implement executor for CREATE/DROP INDEX
4. Connect to Index Manager

### Phase 2: Storage Integration (Day 2)
1. Persist index metadata in catalog
2. Store index data files alongside table files
3. Update indexes on INSERT/UPDATE/DELETE
4. Load indexes on startup

### Phase 3: Query Optimization (Days 3-4)
1. Implement `IndexSelection.Apply()` rule
2. Pattern match scan+filter combinations
3. Create IndexScan logical nodes
4. Add cost comparison logic

### Phase 4: Physical Execution (Days 5-6)
1. Create `PhysicalIndexScan` node
2. Implement `IndexScanOperator`
3. Add index-to-heap fetch logic
4. Handle index-only scans

### Phase 5: Cost Model (Day 7)
1. Enhance cost calculations for index scans
2. Use selectivity estimates from statistics
3. Compare sequential vs index scan costs
4. Add index depth to cost model

## Code Locations for Modifications

1. **Parser**: `internal/sql/parser/parser.go` - Add CREATE INDEX
2. **AST**: `internal/sql/ast/ddl.go` - Add CreateIndexStmt
3. **Planner**: `internal/sql/planner/optimizer.go:260` - Implement Apply()
4. **Executor**: `internal/sql/executor/operator.go` - Add IndexScanOperator
5. **Storage**: `internal/sql/executor/storage_backend.go` - Add index updates
6. **Physical**: `internal/sql/executor/physical.go:207` - Modify optimizeScan()

## Risk Assessment

**Low Risk**: Most infrastructure exists, just needs connection
**Medium Risk**: Storage integration complexity with WAL
**High Risk**: Cost model accuracy for correct plan selection

## Next Steps

1. Start with CREATE INDEX parser implementation
2. Design index storage format
3. Implement the IndexSelection optimizer rule
4. Create physical execution operators

The good news is that ~70% of the required code already exists. We just need to connect the pieces and implement the missing 30%.