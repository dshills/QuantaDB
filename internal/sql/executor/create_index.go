package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CreateIndexOperator executes CREATE INDEX statements.
type CreateIndexOperator struct {
	indexName      string
	tableName      string
	schemaName     string
	columns        []string
	includeColumns []string
	unique         bool
	indexType      string
	catalog        catalog.Catalog
	storage        StorageBackend
	indexMgr       *index.Manager
	executed       bool
	resultReturned bool
	schema         *Schema
	ctx            *ExecContext
}

// NewCreateIndexOperator creates a new CREATE INDEX operator.
func NewCreateIndexOperator(
	schemaName, tableName, indexName string,
	columns []string,
	includeColumns []string,
	unique bool,
	indexType string,
	cat catalog.Catalog,
	storage StorageBackend,
	indexMgr *index.Manager,
) *CreateIndexOperator {
	// Schema for CREATE INDEX result
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &CreateIndexOperator{
		indexName:      indexName,
		tableName:      tableName,
		schemaName:     schemaName,
		columns:        columns,
		includeColumns: includeColumns,
		unique:         unique,
		indexType:      indexType,
		catalog:        cat,
		storage:        storage,
		indexMgr:       indexMgr,
		executed:       false,
		schema:         schema,
	}
}

// Open initializes the CREATE INDEX operation.
func (c *CreateIndexOperator) Open(ctx *ExecContext) error {
	c.ctx = ctx
	
	// Check if table exists
	table, err := c.catalog.GetTable(c.schemaName, c.tableName)
	if err != nil {
		return fmt.Errorf("table '%s.%s' does not exist", c.schemaName, c.tableName)
	}

	// Validate columns exist in table
	for _, colName := range c.columns {
		found := false
		for _, col := range table.Columns {
			if col.Name == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column '%s' does not exist in table '%s.%s'",
				colName, c.schemaName, c.tableName)
		}
	}

	// Validate include columns exist in table
	for _, colName := range c.includeColumns {
		found := false
		for _, col := range table.Columns {
			if col.Name == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("include column '%s' does not exist in table '%s.%s'",
				colName, c.schemaName, c.tableName)
		}

		// Ensure include column is not already a key column
		for _, keyColName := range c.columns {
			if keyColName == colName {
				return fmt.Errorf("column '%s' cannot be both a key column and include column", colName)
			}
		}
	}

	// Check if index already exists
	for _, idx := range table.Indexes {
		if idx.Name == c.indexName {
			return fmt.Errorf("index '%s' already exists on table '%s.%s'",
				c.indexName, c.schemaName, c.tableName)
		}
	}

	// Map index type string to catalog type
	var idxType catalog.IndexType
	switch c.indexType {
	case "BTREE", "":
		idxType = catalog.BTreeIndex
	case "HASH":
		idxType = catalog.HashIndex
	default:
		return fmt.Errorf("unsupported index type: %s", c.indexType)
	}

	// Create the actual index using index manager
	err = c.indexMgr.CreateIndex(c.schemaName, c.tableName, c.indexName, c.columns, c.unique)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// Create index metadata for catalog
	// Create IndexColumnDef for each column
	indexColumns := make([]catalog.IndexColumnDef, len(c.columns))
	for i, colName := range c.columns {
		indexColumns[i] = catalog.IndexColumnDef{
			ColumnName: colName,
			SortOrder:  catalog.Ascending,
		}
	}

	// Create IndexSchema for catalog
	indexSchema := &catalog.IndexSchema{
		SchemaName:     c.schemaName,
		TableName:      c.tableName,
		IndexName:      c.indexName,
		Type:           idxType,
		IsUnique:       c.unique,
		Columns:        indexColumns,
		IncludeColumns: c.includeColumns,
	}

	// Add index to catalog
	_, err = c.catalog.CreateIndex(indexSchema)
	if err != nil {
		// Rollback: drop the index from index manager
		rollbackErr := c.indexMgr.DropIndex(c.schemaName, c.tableName, c.indexName)
		if rollbackErr != nil {
			// Log the rollback error but return the original error
			return fmt.Errorf("failed to create index in catalog: %w (rollback also failed: %v)", err, rollbackErr)
		}
		return fmt.Errorf("failed to create index in catalog: %w", err)
	}

	// Populate index with existing data
	if err := c.populateIndex(ctx, table); err != nil {
		// Rollback: drop the index from both catalog and index manager
		catalogErr := c.catalog.DropIndex(c.schemaName, c.tableName, c.indexName)
		indexErr := c.indexMgr.DropIndex(c.schemaName, c.tableName, c.indexName)
		if catalogErr != nil || indexErr != nil {
			return fmt.Errorf("failed to populate index: %w (rollback errors: catalog=%v, index=%v)", err, catalogErr, indexErr)
		}
		return fmt.Errorf("failed to populate index: %w", err)
	}

	c.executed = true

	// Invalidate result cache for this table since index affects query performance
	c.ctx.InvalidateResultCacheForTable(c.schemaName, c.tableName)

	// Update statistics
	// Note: DDL operations don't read rows, so we don't update RowsRead

	return nil
}

// Next returns the result of CREATE INDEX
func (c *CreateIndexOperator) Next() (*Row, error) {
	if !c.executed {
		return nil, fmt.Errorf("CREATE INDEX not executed")
	}

	// Check if we already returned the result
	if c.resultReturned {
		return nil, nil // nolint:nilnil // EOF - standard iterator pattern
	}

	// Mark result as returned
	c.resultReturned = true

	result := fmt.Sprintf("Index '%s' created on table '%s.%s'",
		c.indexName, c.schemaName, c.tableName)

	return &Row{
		Values: []types.Value{
			types.NewTextValue(result),
		},
	}, nil
}

// Close cleans up after CREATE INDEX
func (c *CreateIndexOperator) Close() error {
	c.executed = false
	c.resultReturned = false
	return nil
}

// Schema returns the operator schema
func (c *CreateIndexOperator) Schema() *Schema {
	return c.schema
}

// populateIndex scans the table and inserts all existing rows into the index
func (c *CreateIndexOperator) populateIndex(ctx *ExecContext, table *catalog.Table) error {
	// Use the transaction snapshot timestamp if available
	var snapshotTS int64
	if ctx != nil {
		snapshotTS = ctx.SnapshotTS
	}

	// Scan the table to get all existing rows
	iterator, err := c.storage.ScanTable(table.ID, snapshotTS)
	if err != nil {
		return fmt.Errorf("failed to scan table for index population: %w", err)
	}
	defer iterator.Close()

	// Process each row
	rowCount := 0
	for {
		row, rowID, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("failed to read row during index population: %w", err)
		}
		if row == nil {
			break // End of table
		}

		// Convert row values to map format expected by index manager
		rowMap := make(map[string]types.Value, len(table.Columns))
		for colIdx, col := range table.Columns {
			rowMap[col.Name] = row.Values[colIdx]
		}

		// Insert into the index
		if err := c.indexMgr.InsertIntoIndexes(c.schemaName, c.tableName, rowMap, rowID.Bytes()); err != nil {
			return fmt.Errorf("failed to insert row %d into index: %w", rowCount, err)
		}

		rowCount++
	}

	return nil
}

// DropIndexOperator executes DROP INDEX statements
type DropIndexOperator struct {
	indexName  string
	tableName  string
	schemaName string
	catalog    catalog.Catalog
	indexMgr   *index.Manager
	executed   bool
	schema     *Schema
	ctx        *ExecContext
}

// NewDropIndexOperator creates a new DROP INDEX operator
func NewDropIndexOperator(
	schemaName, tableName, indexName string,
	cat catalog.Catalog,
	indexMgr *index.Manager,
) *DropIndexOperator {
	// Schema for DROP INDEX result
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &DropIndexOperator{
		indexName:  indexName,
		tableName:  tableName,
		schemaName: schemaName,
		catalog:    cat,
		indexMgr:   indexMgr,
		executed:   false,
		schema:     schema,
	}
}

// Open initializes the DROP INDEX operation
func (d *DropIndexOperator) Open(ctx *ExecContext) error {
	d.ctx = ctx
	
	// If table name is not specified, search all tables
	if d.tableName == "" {
		// Search for index in all tables
		tables, err := d.catalog.ListTables(d.schemaName)
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		found := false
		for _, tbl := range tables {
			for _, idx := range tbl.Indexes {
				if idx.Name == d.indexName {
					d.tableName = tbl.TableName
					found = true
					break
				}
			}
			if found {
				break
			}
		}

		if !found {
			return fmt.Errorf("index '%s' not found", d.indexName)
		}
	}

	// Get index from catalog
	idx, err := d.catalog.GetIndex(d.schemaName, d.tableName, d.indexName)
	if err != nil {
		return fmt.Errorf("index '%s' not found on table '%s.%s': %w",
			d.indexName, d.schemaName, d.tableName, err)
	}

	// Don't allow dropping primary key index
	if idx.IsPrimary {
		return fmt.Errorf("cannot drop primary key index '%s'", d.indexName)
	}

	// Save index schema for potential rollback
	indexSchema := &catalog.IndexSchema{
		SchemaName: d.schemaName,
		TableName:  d.tableName,
		IndexName:  d.indexName,
		Type:       idx.Type,
		IsUnique:   idx.IsUnique,
		Columns:    make([]catalog.IndexColumnDef, len(idx.Columns)),
	}
	for i, col := range idx.Columns {
		indexSchema.Columns[i] = catalog.IndexColumnDef{
			ColumnName: col.Column.Name,
			SortOrder:  col.SortOrder,
		}
	}

	// Drop from catalog first
	err = d.catalog.DropIndex(d.schemaName, d.tableName, d.indexName)
	if err != nil {
		return fmt.Errorf("failed to drop index from catalog: %w", err)
	}

	// Drop the actual index
	err = d.indexMgr.DropIndex(d.schemaName, d.tableName, d.indexName)
	if err != nil {
		// Try to restore in catalog
		_, restoreErr := d.catalog.CreateIndex(indexSchema)
		if restoreErr != nil {
			return fmt.Errorf("failed to drop index: %w (catalog restore also failed: %v)", err, restoreErr)
		}
		return fmt.Errorf("failed to drop index: %w", err)
	}

	d.executed = true

	// Invalidate result cache for this table since index affects query performance
	d.ctx.InvalidateResultCacheForTable(d.schemaName, d.tableName)

	return nil
}

// Next returns the result of DROP INDEX
func (d *DropIndexOperator) Next() (*Row, error) {
	if !d.executed {
		return nil, fmt.Errorf("DROP INDEX not executed")
	}

	// Return result once
	if d.executed {
		d.executed = false // Ensure we only return once

		result := fmt.Sprintf("Index '%s' dropped from table '%s.%s'",
			d.indexName, d.schemaName, d.tableName)

		return &Row{
			Values: []types.Value{
				types.NewTextValue(result),
			},
		}, nil
	}

	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
}

// Close cleans up after DROP INDEX
func (d *DropIndexOperator) Close() error {
	d.executed = false
	return nil
}

// Schema returns the operator schema
func (d *DropIndexOperator) Schema() *Schema {
	return d.schema
}
