# CASCADE DELETE and CHECK Constraints Implementation Plan

## Overview
This document outlines the implementation plan for CASCADE DELETE functionality in foreign keys and full CHECK constraint expression parsing in QuantaDB.

## Current State Analysis

### Foreign Key Constraints
- ✅ Data structures support all referential actions (CASCADE, RESTRICT, SET NULL, SET DEFAULT)
- ✅ Parser recognizes CASCADE syntax
- ✅ Basic referential integrity works (blocks deletes if referenced)
- ❌ CASCADE DELETE not implemented in executor
- ❌ Other referential actions (SET NULL, SET DEFAULT) not implemented

### CHECK Constraints
- ✅ Basic CHECK constraint structure exists
- ✅ Parser can parse CHECK syntax
- ✅ Simple expression evaluation works
- ❌ Complex expressions not supported
- ❌ Expression parsing happens on every validation (performance issue)

## Implementation Plan

### Phase 1: CASCADE DELETE Implementation

1. **Update ValidateDelete method** in `constraints.go`:
   - Instead of just blocking deletes, check the referential action
   - If CASCADE, collect all referencing rows to delete
   - If SET NULL, collect rows to update
   - If SET DEFAULT, collect rows to set to default values

2. **Create CascadeDeleteHandler**:
   - New struct to handle cascade operations
   - Methods:
     - `CollectCascadeDeletes()` - Find all rows to delete
     - `ExecuteCascadeDeletes()` - Delete collected rows
     - `ExecuteSetNull()` - Update referencing rows to NULL
     - `ExecuteSetDefault()` - Update referencing rows to default

3. **Update DeleteOperator**:
   - Integrate CascadeDeleteHandler
   - Execute cascade actions after constraint validation
   - Ensure proper transaction handling

### Phase 2: CHECK Constraint Expression Parsing

1. **Create Expression Cache**:
   - Cache parsed expressions to avoid re-parsing
   - Key: constraint expression string
   - Value: parsed AST

2. **Enhance Expression Evaluator**:
   - Support all SQL operators and functions
   - Handle complex boolean logic
   - Support column references with proper scoping
   - Add support for:
     - Comparison operators (>, <, >=, <=, =, !=)
     - Logical operators (AND, OR, NOT)
     - IN/NOT IN
     - BETWEEN
     - IS NULL/IS NOT NULL
     - String functions (LENGTH, UPPER, LOWER)
     - Numeric functions (ABS, etc.)

3. **Update validateCheck method**:
   - Use expression cache
   - Better error messages
   - Support for more complex expressions

### Phase 3: Performance Optimizations

1. **Index Usage for Foreign Keys**:
   - Use indexes when checking foreign key constraints
   - Create implicit indexes on foreign key columns

2. **Batch Operations**:
   - Batch cascade deletes for better performance
   - Use prepared statements for repeated operations

## Implementation Details

### CASCADE DELETE Algorithm

```go
type CascadeDeleteHandler struct {
    catalog         catalog.Catalog
    storage         StorageBackend
    deletedRows     map[tableRowKey]bool
    pendingDeletes  []cascadeDelete
}

type cascadeDelete struct {
    table    *catalog.Table
    rowID    uint64
    row      *Row
    fkAction ReferentialAction
}

func (h *CascadeDeleteHandler) ProcessDelete(table *catalog.Table, row *Row, rowID uint64) error {
    // 1. Mark this row as deleted
    // 2. Find all tables that reference this table
    // 3. For each referencing table:
    //    - Find rows that reference the deleted row
    //    - Based on ON DELETE action:
    //      * CASCADE: Add to pending deletes
    //      * SET NULL: Update referencing columns to NULL
    //      * SET DEFAULT: Update to default values
    //      * RESTRICT/NO ACTION: Return error if references exist
    // 4. Process pending deletes recursively
}
```

### CHECK Constraint Expression Cache

```go
type ExpressionCache struct {
    cache map[string]parser.Expression
    mu    sync.RWMutex
}

func (c *ExpressionCache) GetOrParse(expr string) (parser.Expression, error) {
    c.mu.RLock()
    if cached, ok := c.cache[expr]; ok {
        c.mu.RUnlock()
        return cached, nil
    }
    c.mu.RUnlock()
    
    // Parse expression
    parsed, err := parser.NewParser(expr).ParseExpression()
    if err != nil {
        return nil, err
    }
    
    c.mu.Lock()
    c.cache[expr] = parsed
    c.mu.Unlock()
    
    return parsed, nil
}
```

## Testing Plan

### CASCADE DELETE Tests
1. Simple cascade delete (parent -> child)
2. Multi-level cascade (grandparent -> parent -> child)
3. Multiple children tables
4. Circular references (should be prevented)
5. Mixed referential actions (CASCADE on one FK, RESTRICT on another)
6. Transaction rollback with cascades

### CHECK Constraint Tests
1. Simple comparisons (price > 0)
2. Complex boolean logic ((price > 0 AND price < 1000) OR special = true)
3. String operations (LENGTH(name) > 5)
4. NULL handling (price IS NOT NULL AND price > 0)
5. Multiple CHECK constraints on same table
6. CHECK constraints with subqueries (if supported)

## Migration Considerations
- Existing tables with foreign keys will automatically get CASCADE support
- No schema changes required
- CHECK constraints will work with more expressions

## Performance Impact
- CASCADE DELETE will be slower than simple deletes
- CHECK constraint caching will improve INSERT/UPDATE performance
- Index usage will significantly improve foreign key validation

## Timeline
- Phase 1 (CASCADE DELETE): 2-3 days
- Phase 2 (CHECK Constraints): 2-3 days
- Phase 3 (Optimizations): 1-2 days
- Testing and bug fixes: 2-3 days

Total: ~10 days