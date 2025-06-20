# Catalog Design

## Overview

The catalog system manages all metadata about database objects including tables, columns, indexes, constraints, and statistics. It serves as the source of truth for schema information and is critical for query planning and execution.

## Architecture

### System Tables Approach

Similar to PostgreSQL, we'll use system tables to store metadata. This provides a uniform interface and allows querying metadata using SQL.

### System Tables Schema

```sql
-- System namespace
CREATE SCHEMA _system;

-- Tables metadata
CREATE TABLE _system._tables (
    table_id        INTEGER PRIMARY KEY,
    schema_name     VARCHAR(128) NOT NULL,
    table_name      VARCHAR(128) NOT NULL,
    table_type      VARCHAR(32) NOT NULL, -- 'TABLE', 'VIEW', 'SYSTEM'
    row_count       BIGINT DEFAULT 0,
    page_count      BIGINT DEFAULT 0,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    UNIQUE(schema_name, table_name)
);

-- Columns metadata
CREATE TABLE _system._columns (
    table_id        INTEGER NOT NULL,
    column_id       INTEGER NOT NULL,
    column_name     VARCHAR(128) NOT NULL,
    data_type       VARCHAR(64) NOT NULL,
    type_params     VARCHAR(256), -- JSON for type parameters
    is_nullable     BOOLEAN NOT NULL DEFAULT TRUE,
    default_value   TEXT,
    ordinal_position INTEGER NOT NULL,
    PRIMARY KEY (table_id, column_id),
    FOREIGN KEY (table_id) REFERENCES _system._tables(table_id)
);

-- Indexes metadata
CREATE TABLE _system._indexes (
    index_id        INTEGER PRIMARY KEY,
    table_id        INTEGER NOT NULL,
    index_name      VARCHAR(128) NOT NULL,
    index_type      VARCHAR(32) NOT NULL, -- 'BTREE', 'HASH'
    is_unique       BOOLEAN NOT NULL DEFAULT FALSE,
    is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP NOT NULL,
    UNIQUE(table_id, index_name),
    FOREIGN KEY (table_id) REFERENCES _system._tables(table_id)
);

-- Index columns
CREATE TABLE _system._index_columns (
    index_id        INTEGER NOT NULL,
    column_id       INTEGER NOT NULL,
    ordinal_position INTEGER NOT NULL,
    sort_order      VARCHAR(4), -- 'ASC', 'DESC'
    PRIMARY KEY (index_id, ordinal_position),
    FOREIGN KEY (index_id) REFERENCES _system._indexes(index_id)
);

-- Table statistics
CREATE TABLE _system._table_stats (
    table_id        INTEGER PRIMARY KEY,
    row_count       BIGINT NOT NULL,
    page_count      BIGINT NOT NULL,
    avg_row_size    INTEGER,
    last_analyzed   TIMESTAMP,
    FOREIGN KEY (table_id) REFERENCES _system._tables(table_id)
);

-- Column statistics
CREATE TABLE _system._column_stats (
    table_id        INTEGER NOT NULL,
    column_id       INTEGER NOT NULL,
    null_count      BIGINT,
    distinct_count  BIGINT,
    avg_width       INTEGER,
    min_value       TEXT,
    max_value       TEXT,
    histogram       TEXT, -- JSON histogram data
    last_analyzed   TIMESTAMP,
    PRIMARY KEY (table_id, column_id),
    FOREIGN KEY (table_id, column_id) REFERENCES _system._columns(table_id, column_id)
);
```

## Go Implementation

### Core Interfaces

```go
package catalog

import (
    "github.com/dshills/QuantaDB/internal/sql/types"
)

// Catalog manages database metadata
type Catalog interface {
    // Table operations
    CreateTable(schema *TableSchema) (*Table, error)
    GetTable(schemaName, tableName string) (*Table, error)
    DropTable(schemaName, tableName string) error
    ListTables(schemaName string) ([]*Table, error)
    
    // Index operations
    CreateIndex(index *IndexSchema) (*Index, error)
    GetIndex(schemaName, tableName, indexName string) (*Index, error)
    DropIndex(schemaName, tableName, indexName string) error
    
    // Statistics operations
    UpdateTableStats(schemaName, tableName string) error
    GetTableStats(schemaName, tableName string) (*TableStats, error)
    
    // Schema operations
    CreateSchema(name string) error
    DropSchema(name string) error
    ListSchemas() ([]string, error)
}

// TableSchema defines a table structure
type TableSchema struct {
    SchemaName  string
    TableName   string
    Columns     []ColumnDef
    Constraints []Constraint
}

// ColumnDef defines a column
type ColumnDef struct {
    Name         string
    DataType     types.DataType
    IsNullable   bool
    DefaultValue types.Value
    Constraints  []ColumnConstraint
}

// Table represents a table with metadata
type Table struct {
    ID          int64
    SchemaName  string
    TableName   string
    Columns     []*Column
    Indexes     []*Index
    Constraints []Constraint
    Stats       *TableStats
}

// Column represents a column with metadata
type Column struct {
    ID              int64
    Name            string
    DataType        types.DataType
    OrdinalPosition int
    IsNullable      bool
    DefaultValue    types.Value
    Stats           *ColumnStats
}

// Index represents an index
type Index struct {
    ID         int64
    Name       string
    Type       IndexType
    IsUnique   bool
    IsPrimary  bool
    Columns    []IndexColumn
}

// IndexColumn represents a column in an index
type IndexColumn struct {
    Column    *Column
    SortOrder SortOrder
}
```

### Statistics Management

```go
// TableStats holds table-level statistics
type TableStats struct {
    RowCount     int64
    PageCount    int64
    AvgRowSize   int
    LastAnalyzed time.Time
}

// ColumnStats holds column-level statistics
type ColumnStats struct {
    NullCount     int64
    DistinctCount int64
    AvgWidth      int
    MinValue      types.Value
    MaxValue      types.Value
    Histogram     *Histogram
    LastAnalyzed  time.Time
}

// Histogram for selectivity estimation
type Histogram struct {
    Type    HistogramType // EQUI_HEIGHT, EQUI_WIDTH
    Buckets []HistogramBucket
}

type HistogramBucket struct {
    LowerBound   types.Value
    UpperBound   types.Value
    Frequency    int64
    DistinctCount int64
}

// StatsCollector analyzes tables and updates statistics
type StatsCollector interface {
    AnalyzeTable(table *Table) (*TableStats, error)
    AnalyzeColumn(table *Table, column *Column) (*ColumnStats, error)
    BuildHistogram(table *Table, column *Column) (*Histogram, error)
}
```

### Bootstrapping

```go
// Bootstrap initializes system tables
type Bootstrapper struct {
    engine storage.Engine
}

func (b *Bootstrapper) Bootstrap() error {
    // 1. Create system schema
    if err := b.createSystemSchema(); err != nil {
        return err
    }
    
    // 2. Create system tables
    systemTables := []TableSchema{
        tablesSchema(),
        columnsSchema(),
        indexesSchema(),
        // ... other system tables
    }
    
    for _, schema := range systemTables {
        if err := b.createSystemTable(schema); err != nil {
            return err
        }
    }
    
    // 3. Insert metadata for system tables themselves
    return b.insertSystemMetadata()
}

// Hard-coded schemas for system tables
func tablesSchema() TableSchema {
    return TableSchema{
        SchemaName: "_system",
        TableName:  "_tables",
        Columns: []ColumnDef{
            {Name: "table_id", DataType: types.Integer, IsNullable: false},
            {Name: "schema_name", DataType: types.Varchar(128), IsNullable: false},
            {Name: "table_name", DataType: types.Varchar(128), IsNullable: false},
            // ... other columns
        },
    }
}
```

### Implementation Strategies

#### In-Memory Catalog (Phase 1)

```go
// MemoryCatalog for development and testing
type MemoryCatalog struct {
    mu      sync.RWMutex
    schemas map[string]*Schema
    tables  map[string]*Table  // "schema.table" -> Table
    indexes map[string]*Index  // "schema.table.index" -> Index
    nextID  int64
}

func (c *MemoryCatalog) CreateTable(schema *TableSchema) (*Table, error) {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    key := fmt.Sprintf("%s.%s", schema.SchemaName, schema.TableName)
    if _, exists := c.tables[key]; exists {
        return nil, ErrTableExists
    }
    
    table := &Table{
        ID:         c.nextID,
        SchemaName: schema.SchemaName,
        TableName:  schema.TableName,
        Columns:    make([]*Column, 0, len(schema.Columns)),
    }
    c.nextID++
    
    // Create columns
    for i, colDef := range schema.Columns {
        column := &Column{
            ID:              c.nextID,
            Name:            colDef.Name,
            DataType:        colDef.DataType,
            OrdinalPosition: i + 1,
            IsNullable:      colDef.IsNullable,
            DefaultValue:    colDef.DefaultValue,
        }
        c.nextID++
        table.Columns = append(table.Columns, column)
    }
    
    c.tables[key] = table
    return table, nil
}
```

#### Persistent Catalog (Phase 2)

```go
// PersistentCatalog stores metadata in system tables
type PersistentCatalog struct {
    engine   storage.Engine
    executor executor.Executor
    cache    *CatalogCache
}

func (c *PersistentCatalog) CreateTable(schema *TableSchema) (*Table, error) {
    // Begin transaction
    tx := c.engine.BeginTransaction()
    defer tx.Rollback()
    
    // Insert into _system._tables
    tableID := c.nextTableID()
    insertTableSQL := `
        INSERT INTO _system._tables 
        (table_id, schema_name, table_name, table_type, created_at, updated_at)
        VALUES (?, ?, ?, 'TABLE', NOW(), NOW())
    `
    
    if err := c.executor.Execute(tx, insertTableSQL, 
        tableID, schema.SchemaName, schema.TableName); err != nil {
        return nil, err
    }
    
    // Insert columns
    for i, col := range schema.Columns {
        columnID := c.nextColumnID()
        insertColSQL := `
            INSERT INTO _system._columns
            (table_id, column_id, column_name, data_type, is_nullable, ordinal_position)
            VALUES (?, ?, ?, ?, ?, ?)
        `
        
        if err := c.executor.Execute(tx, insertColSQL,
            tableID, columnID, col.Name, col.DataType.Name(), 
            col.IsNullable, i+1); err != nil {
            return nil, err
        }
    }
    
    // Create physical table
    if err := c.createPhysicalTable(tx, schema); err != nil {
        return nil, err
    }
    
    // Commit transaction
    if err := tx.Commit(); err != nil {
        return nil, err
    }
    
    // Update cache
    table := c.buildTableObject(tableID, schema)
    c.cache.Put(table)
    
    return table, nil
}
```

### Catalog Cache

```go
// CatalogCache reduces metadata lookups
type CatalogCache struct {
    mu         sync.RWMutex
    tables     map[string]*Table      // schema.table -> Table
    tablesByID map[int64]*Table       // table_id -> Table
    ttl        time.Duration
}

func (c *CatalogCache) Get(schemaName, tableName string) (*Table, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    key := fmt.Sprintf("%s.%s", schemaName, tableName)
    table, ok := c.tables[key]
    return table, ok
}

func (c *CatalogCache) Invalidate(schemaName, tableName string) {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    key := fmt.Sprintf("%s.%s", schemaName, tableName)
    delete(c.tables, key)
}
```

## Usage Examples

### Creating a Table

```go
catalog := NewCatalog(engine)

// Define table schema
schema := &TableSchema{
    SchemaName: "public",
    TableName:  "users",
    Columns: []ColumnDef{
        {
            Name:       "id",
            DataType:   types.Integer,
            IsNullable: false,
            Constraints: []ColumnConstraint{PrimaryKey{}},
        },
        {
            Name:       "name",
            DataType:   types.Varchar(100),
            IsNullable: false,
        },
        {
            Name:       "email",
            DataType:   types.Varchar(255),
            IsNullable: false,
            Constraints: []ColumnConstraint{Unique{}},
        },
        {
            Name:       "created_at",
            DataType:   types.Timestamp,
            IsNullable: false,
            DefaultValue: types.NewValue(time.Now()),
        },
    },
}

// Create table
table, err := catalog.CreateTable(schema)
if err != nil {
    return fmt.Errorf("failed to create table: %w", err)
}
```

### Querying Metadata

```go
// Get table information
table, err := catalog.GetTable("public", "users")
if err != nil {
    return fmt.Errorf("table not found: %w", err)
}

// Access columns
for _, col := range table.Columns {
    fmt.Printf("Column: %s, Type: %s, Nullable: %v\n", 
        col.Name, col.DataType.Name(), col.IsNullable)
}

// Get statistics
stats, err := catalog.GetTableStats("public", "users")
if err == nil {
    fmt.Printf("Row count: %d, Page count: %d\n", 
        stats.RowCount, stats.PageCount)
}
```

## Testing Strategy

### Unit Tests

```go
func TestCatalogCreateTable(t *testing.T) {
    catalog := NewMemoryCatalog()
    
    schema := &TableSchema{
        SchemaName: "test",
        TableName:  "users",
        Columns: []ColumnDef{
            {Name: "id", DataType: types.Integer, IsNullable: false},
            {Name: "name", DataType: types.Varchar(50), IsNullable: true},
        },
    }
    
    table, err := catalog.CreateTable(schema)
    assert.NoError(t, err)
    assert.NotNil(t, table)
    assert.Equal(t, "users", table.TableName)
    assert.Len(t, table.Columns, 2)
    
    // Verify columns
    assert.Equal(t, "id", table.Columns[0].Name)
    assert.Equal(t, types.Integer, table.Columns[0].DataType)
    assert.False(t, table.Columns[0].IsNullable)
}

func TestCatalogDuplicateTable(t *testing.T) {
    catalog := NewMemoryCatalog()
    
    schema := &TableSchema{
        SchemaName: "test",
        TableName:  "users",
        Columns:    []ColumnDef{{Name: "id", DataType: types.Integer}},
    }
    
    _, err := catalog.CreateTable(schema)
    assert.NoError(t, err)
    
    // Try to create duplicate
    _, err = catalog.CreateTable(schema)
    assert.Equal(t, ErrTableExists, err)
}
```

## Future Enhancements

1. **Partitioned Tables**: Support for table partitioning
2. **Foreign Keys**: Foreign key constraint management
3. **Views**: Materialized and regular views
4. **Functions**: User-defined functions catalog
5. **Permissions**: Access control metadata
6. **Event Triggers**: DDL event notifications
7. **Schema Versioning**: Track schema changes over time