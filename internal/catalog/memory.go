package catalog

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const defaultSchemaName = "public"

// MemoryCatalog is an in-memory implementation of the Catalog interface.
// It's useful for testing and development.
type MemoryCatalog struct {
	mu      sync.RWMutex
	schemas map[string]*schema
	tables  map[string]*Table // "schema.table" -> Table
	indexes map[string]*Index // "schema.table.index" -> Index
	nextID  int64
}

// schema represents a database schema.
type schema struct {
	name   string
	tables map[string]*Table
}

// NewMemoryCatalog creates a new in-memory catalog.
func NewMemoryCatalog() *MemoryCatalog {
	c := &MemoryCatalog{
		schemas: make(map[string]*schema),
		tables:  make(map[string]*Table),
		indexes: make(map[string]*Index),
		nextID:  1,
	}

	// Create default public schema
	c.schemas[defaultSchemaName] = &schema{
		name:   defaultSchemaName,
		tables: make(map[string]*Table),
	}

	return c
}

// CreateSchema creates a new schema.
func (c *MemoryCatalog) CreateSchema(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.schemas[name]; exists {
		return fmt.Errorf("schema %q already exists", name)
	}

	c.schemas[name] = &schema{
		name:   name,
		tables: make(map[string]*Table),
	}

	return nil
}

// DropSchema drops a schema and all its tables.
func (c *MemoryCatalog) DropSchema(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if name == defaultSchemaName {
		return fmt.Errorf("cannot drop %s schema", defaultSchemaName)
	}

	schema, exists := c.schemas[name]
	if !exists {
		return fmt.Errorf("schema %q does not exist", name)
	}

	// Drop all tables in the schema
	for tableName := range schema.tables {
		key := fmt.Sprintf("%s.%s", name, tableName)
		delete(c.tables, key)

		// Drop all indexes for the table
		for indexKey := range c.indexes {
			if strings.HasPrefix(indexKey, key+".") {
				delete(c.indexes, indexKey)
			}
		}
	}

	delete(c.schemas, name)
	return nil
}

// ListSchemas returns a list of all schemas.
func (c *MemoryCatalog) ListSchemas() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schemas := make([]string, 0, len(c.schemas))
	for name := range c.schemas {
		schemas = append(schemas, name)
	}

	return schemas, nil
}

// CreateTable creates a new table.
func (c *MemoryCatalog) CreateTable(tableSchema *TableSchema) (*Table, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate schema exists
	schemaName := tableSchema.SchemaName
	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	schema, exists := c.schemas[schemaName]
	if !exists {
		return nil, fmt.Errorf("schema %q does not exist", schemaName)
	}

	// Check if table already exists
	key := fmt.Sprintf("%s.%s", schemaName, tableSchema.TableName)
	if _, exists := c.tables[key]; exists {
		return nil, fmt.Errorf("table %q already exists", key)
	}

	// Create table
	table := &Table{
		ID:         c.nextID,
		SchemaName: schemaName,
		TableName:  tableSchema.TableName,
		Columns:    make([]*Column, 0, len(tableSchema.Columns)),
		Indexes:    make([]*Index, 0),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	c.nextID++

	// Create columns
	for i, colDef := range tableSchema.Columns {
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

		// Handle column constraints
		for _, constraint := range colDef.Constraints {
			switch constraint := constraint.(type) {
			case NotNullConstraint:
				column.IsNullable = false
			case DefaultConstraint:
				column.DefaultValue = constraint.Value
			}
		}
	}

	// Handle table constraints
	table.Constraints = tableSchema.Constraints
	for _, constraint := range tableSchema.Constraints {
		switch con := constraint.(type) {
		case PrimaryKeyConstraint:
			// Create primary key index
			index := &Index{
				ID:        c.nextID,
				Name:      fmt.Sprintf("%s_pkey", tableSchema.TableName),
				TableID:   table.ID,
				Type:      BTreeIndex,
				IsUnique:  true,
				IsPrimary: true,
				Columns:   make([]IndexColumn, 0, len(con.Columns)),
				CreatedAt: time.Now(),
			}
			c.nextID++

			// Add columns to index
			for i, colName := range con.Columns {
				col := c.findColumn(table, colName)
				if col == nil {
					return nil, fmt.Errorf("column %q not found for primary key", colName)
				}
				index.Columns = append(index.Columns, IndexColumn{
					Column:    col,
					SortOrder: Ascending,
					Position:  i,
				})
			}

			table.Indexes = append(table.Indexes, index)
			indexKey := fmt.Sprintf("%s.%s", key, index.Name)
			c.indexes[indexKey] = index
		}
	}

	// Initialize empty statistics
	table.Stats = &TableStats{
		RowCount:     0,
		PageCount:    0,
		AvgRowSize:   0,
		LastAnalyzed: time.Time{},
	}

	// Store table
	c.tables[key] = table
	schema.tables[tableSchema.TableName] = table

	return table, nil
}

// GetTable retrieves a table by name.
func (c *MemoryCatalog) GetTable(schemaName, tableName string) (*Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	table, exists := c.tables[key]
	if !exists {
		return nil, fmt.Errorf("table %q does not exist", key)
	}

	return table, nil
}

// DropTable drops a table and all its indexes.
func (c *MemoryCatalog) DropTable(schemaName, tableName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	schema, exists := c.schemas[schemaName]
	if !exists {
		return fmt.Errorf("schema %q does not exist", schemaName)
	}

	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	table, exists := c.tables[key]
	if !exists {
		return fmt.Errorf("table %q does not exist", key)
	}

	// Drop all indexes
	for _, index := range table.Indexes {
		indexKey := fmt.Sprintf("%s.%s", key, index.Name)
		delete(c.indexes, indexKey)
	}

	// Remove from schema
	delete(schema.tables, tableName)

	// Remove from catalog
	delete(c.tables, key)

	return nil
}

// AddColumn adds a column to an existing table.
func (c *MemoryCatalog) AddColumn(schemaName, tableName string, column ColumnDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	table, exists := c.tables[key]
	if !exists {
		return fmt.Errorf("table %q does not exist", key)
	}

	// Check if column already exists
	for _, existingCol := range table.Columns {
		if existingCol.Name == column.Name {
			return fmt.Errorf("column %q already exists in table %q", column.Name, key)
		}
	}

	// Add the column to the table
	newColumn := &Column{
		ID:              int64(len(table.Columns) + 1), // Simple ID assignment
		Name:            column.Name,
		DataType:        column.DataType,
		OrdinalPosition: len(table.Columns) + 1,
		IsNullable:      column.IsNullable,
		DefaultValue:    column.DefaultValue,
	}

	table.Columns = append(table.Columns, newColumn)

	return nil
}

// DropColumn removes a column from an existing table.
func (c *MemoryCatalog) DropColumn(schemaName, tableName, columnName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	table, exists := c.tables[key]
	if !exists {
		return fmt.Errorf("table %q does not exist", key)
	}

	// Find the column to remove
	columnIndex := -1
	for i, col := range table.Columns {
		if col.Name == columnName {
			columnIndex = i
			break
		}
	}

	if columnIndex == -1 {
		return fmt.Errorf("column %q does not exist in table %q", columnName, key)
	}

	// Cannot drop the last column
	if len(table.Columns) == 1 {
		return fmt.Errorf("cannot drop the last column from table %q", key)
	}

	// Remove the column from the slice
	table.Columns = append(table.Columns[:columnIndex], table.Columns[columnIndex+1:]...)

	return nil
}

// ListTables returns all tables in a schema.
func (c *MemoryCatalog) ListTables(schemaName string) ([]*Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	schema, exists := c.schemas[schemaName]
	if !exists {
		return nil, fmt.Errorf("schema %q does not exist", schemaName)
	}

	tables := make([]*Table, 0, len(schema.tables))
	for _, table := range schema.tables {
		tables = append(tables, table)
	}

	return tables, nil
}

// CreateIndex creates a new index on a table.
func (c *MemoryCatalog) CreateIndex(indexSchema *IndexSchema) (*Index, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the table without locking (we already have the lock)
	schemaName := indexSchema.SchemaName
	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	key := fmt.Sprintf("%s.%s", schemaName, indexSchema.TableName)
	table, exists := c.tables[key]
	if !exists {
		return nil, fmt.Errorf("table %q does not exist", key)
	}

	// Check if index already exists
	indexKey := fmt.Sprintf("%s.%s.%s", schemaName, indexSchema.TableName, indexSchema.IndexName)
	if _, exists := c.indexes[indexKey]; exists {
		return nil, fmt.Errorf("index %q already exists", indexSchema.IndexName)
	}

	// Create index
	index := &Index{
		ID:        c.nextID,
		Name:      indexSchema.IndexName,
		TableID:   table.ID,
		Type:      indexSchema.Type,
		IsUnique:  indexSchema.IsUnique,
		IsPrimary: false,
		Columns:   make([]IndexColumn, 0, len(indexSchema.Columns)),
		CreatedAt: time.Now(),
	}
	c.nextID++

	// Add columns
	for i, colDef := range indexSchema.Columns {
		col := c.findColumn(table, colDef.ColumnName)
		if col == nil {
			return nil, fmt.Errorf("column %q not found", colDef.ColumnName)
		}

		index.Columns = append(index.Columns, IndexColumn{
			Column:    col,
			SortOrder: colDef.SortOrder,
			Position:  i,
		})
	}

	// Add include columns (for covering indexes)
	for i, colName := range indexSchema.IncludeColumns {
		col := c.findColumn(table, colName)
		if col == nil {
			return nil, fmt.Errorf("include column %q not found", colName)
		}

		// Ensure column is not already in key columns
		for _, keyCol := range index.Columns {
			if keyCol.Column.Name == colName {
				return nil, fmt.Errorf("column %q cannot be both a key column and include column", colName)
			}
		}

		index.IncludeColumns = append(index.IncludeColumns, IndexColumn{
			Column:    col,
			SortOrder: Ascending, // Include columns don't affect sort order
			Position:  len(indexSchema.Columns) + i,
		})
	}

	// Store index
	table.Indexes = append(table.Indexes, index)
	c.indexes[indexKey] = index

	return index, nil
}

// GetIndex retrieves an index by name.
func (c *MemoryCatalog) GetIndex(schemaName, tableName, indexName string) (*Index, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	key := fmt.Sprintf("%s.%s.%s", schemaName, tableName, indexName)
	index, exists := c.indexes[key]
	if !exists {
		return nil, fmt.Errorf("index %q does not exist", key)
	}

	return index, nil
}

// DropIndex drops an index.
func (c *MemoryCatalog) DropIndex(schemaName, tableName, indexName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	// Get the table
	tableKey := fmt.Sprintf("%s.%s", schemaName, tableName)
	table, exists := c.tables[tableKey]
	if !exists {
		return fmt.Errorf("table %q does not exist", tableKey)
	}

	// Find and remove the index
	indexKey := fmt.Sprintf("%s.%s", tableKey, indexName)
	index, exists := c.indexes[indexKey]
	if !exists {
		return fmt.Errorf("index %q does not exist", indexName)
	}

	if index.IsPrimary {
		return fmt.Errorf("cannot drop primary key index")
	}

	// Remove from table's index list
	for i, idx := range table.Indexes {
		if idx.ID == index.ID {
			table.Indexes = append(table.Indexes[:i], table.Indexes[i+1:]...)
			break
		}
	}

	// Remove from catalog
	delete(c.indexes, indexKey)

	return nil
}

// UpdateTableStats updates statistics for a table.
func (c *MemoryCatalog) UpdateTableStats(schemaName, tableName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if schemaName == "" {
		schemaName = defaultSchemaName
	}

	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	table, exists := c.tables[key]
	if !exists {
		return fmt.Errorf("table %q does not exist", key)
	}

	// In a real implementation, this would analyze the actual table data
	// For now, just update the timestamp
	if table.Stats == nil {
		table.Stats = &TableStats{}
	}
	table.Stats.LastAnalyzed = time.Now()

	return nil
}

// GetTableStats retrieves statistics for a table.
func (c *MemoryCatalog) GetTableStats(schemaName, tableName string) (*TableStats, error) {
	table, err := c.GetTable(schemaName, tableName)
	if err != nil {
		return nil, err
	}

	if table.Stats == nil {
		return &TableStats{}, nil
	}

	return table.Stats, nil
}

// findColumn finds a column in a table by name.
func (c *MemoryCatalog) findColumn(table *Table, columnName string) *Column {
	for _, col := range table.Columns {
		if col.Name == columnName {
			return col
		}
	}
	return nil
}

// IndexSchema defines the structure for creating an index.
type IndexSchema struct {
	SchemaName     string
	TableName      string
	IndexName      string
	Type           IndexType
	IsUnique       bool
	Columns        []IndexColumnDef // Key columns used for ordering
	IncludeColumns []string         // Non-key columns for covering indexes
}

// IndexColumnDef defines a column in an index.
type IndexColumnDef struct {
	ColumnName string
	SortOrder  SortOrder
}

// GetColumnByName returns a column by name from a table.
func (t *Table) GetColumnByName(name string) *Column {
	for _, col := range t.Columns {
		if col.Name == name {
			return col
		}
	}
	return nil
}

// GetPrimaryKey returns the primary key index if it exists.
func (t *Table) GetPrimaryKey() *Index {
	for _, idx := range t.Indexes {
		if idx.IsPrimary {
			return idx
		}
	}
	return nil
}

// UpdateTableStatistics updates statistics for a table by ID.
func (c *MemoryCatalog) UpdateTableStatistics(tableID int64, stats *TableStats) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find table by ID
	for _, table := range c.tables {
		if table.ID == tableID {
			table.Stats = stats
			table.UpdatedAt = time.Now()
			return nil
		}
	}

	return fmt.Errorf("table with ID %d not found", tableID)
}

// UpdateColumnStatistics updates statistics for a column.
func (c *MemoryCatalog) UpdateColumnStatistics(tableID int64, columnID int64, stats *ColumnStats) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find table by ID
	for _, table := range c.tables {
		if table.ID == tableID {
			// Find column by ID
			for _, col := range table.Columns {
				if col.ID == columnID {
					col.Stats = stats
					table.UpdatedAt = time.Now()
					return nil
				}
			}
			return fmt.Errorf("column with ID %d not found in table %d", columnID, tableID)
		}
	}

	return fmt.Errorf("table with ID %d not found", tableID)
}
