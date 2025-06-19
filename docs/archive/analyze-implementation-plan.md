# ANALYZE Command Implementation Plan

## Overview
Implement PostgreSQL-compatible ANALYZE command to collect table and column statistics for cost-based query optimization.

## Architecture

### 1. SQL Parser Changes
**Files**: `internal/sql/parser/lexer.go`, `internal/sql/parser/parser.go`, `internal/sql/ast/statement.go`

- Add ANALYZE token to lexer
- Add ANALYZE grammar rule: `ANALYZE [TABLE] table_name [(column_name [, ...])]`
- Create AnalyzeStmt AST node

### 2. Query Planner Integration
**File**: `internal/sql/planner/planner.go`

- Add case for AnalyzeStmt in statement planning
- Create logical plan node for ANALYZE operation
- Pass through to executor without optimization

### 3. Executor Implementation
**Files**: `internal/sql/executor/analyze.go` (new), `internal/sql/executor/operators.go`

- Create AnalyzeOperator that:
  - Scans entire table
  - Collects row count
  - For each column:
    - Tracks null count
    - Builds distinct value set (with sampling for large tables)
    - Constructs histogram of value distribution
    - Calculates average column width

### 4. Statistics Storage
**Files**: `internal/catalog/stats.go`, `internal/catalog/catalog.go`

- Extend existing TableStatistics and ColumnStatistics structs
- Add methods to Catalog:
  - `UpdateTableStatistics(tableID, stats)`
  - `UpdateColumnStatistics(tableID, columnID, stats)`
- Persist statistics in catalog storage

### 5. Planner Statistics Usage
**Files**: `internal/sql/planner/cost.go`, `internal/sql/planner/optimizer.go`

- Update selectivity estimation to use real statistics
- Enhance cost calculations with accurate row counts
- Use histogram data for range predicate selectivity

## Implementation Steps

### Phase 1: Parser Support (Day 1)
1. Add ANALYZE token and keyword
2. Implement grammar rule
3. Create AST node structure
4. Write parser tests

### Phase 2: Basic ANALYZE Execution (Day 2)
1. Create AnalyzeOperator skeleton
2. Implement table scan for statistics
3. Collect basic stats (row count, null count)
4. Integration with planner

### Phase 3: Advanced Statistics (Day 3)
1. Implement histogram generation
2. Add distinct value estimation (HyperLogLog for large tables)
3. Calculate column widths
4. Handle different data types

### Phase 4: Storage & Integration (Day 4)
1. Extend catalog storage for statistics
2. Implement statistics persistence
3. Update planner to use stored statistics
4. Performance optimization for large tables

## Testing Strategy

### Unit Tests
- Parser tests for ANALYZE syntax
- Statistics calculation tests
- Histogram generation tests
- Selectivity estimation with real statistics

### Integration Tests
- ANALYZE on various table sizes
- Query plan changes after ANALYZE
- Statistics persistence across restarts
- Concurrent ANALYZE operations

## Performance Considerations

### Sampling for Large Tables
- Use reservoir sampling for tables > 100K rows
- Configurable sample size (default 30K rows)
- Trade accuracy for performance

### Incremental Updates
- Future: Track modifications since last ANALYZE
- Auto-ANALYZE based on change threshold

## PostgreSQL Compatibility

### Supported Features
- Basic ANALYZE syntax
- Table and column statistics
- Histogram-based selectivity
- Manual ANALYZE execution

### Not Implemented (Future)
- ANALYZE VERBOSE
- Auto-vacuum with ANALYZE
- Statistics target per column
- Multi-column statistics

## Example Usage

```sql
-- Analyze entire table
ANALYZE users;

-- Analyze specific columns
ANALYZE users (id, email);

-- View statistics (future)
SELECT * FROM pg_stats WHERE tablename = 'users';
```

## Success Metrics
- Query plans use actual row counts
- Better index vs sequential scan decisions
- Improved join order selection
- Accurate memory allocation for operations