package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CreateIndexOperator executes CREATE INDEX statements.
type CreateIndexOperator struct {
	indexName  string
	tableName  string
	schemaName string
	columns    []string
	unique     bool
	indexType  string
	catalog    catalog.Catalog
	storage    StorageBackend
	indexMgr   *index.Manager
	executed   bool
	schema     *Schema
}

// NewCreateIndexOperator creates a new CREATE INDEX operator.
func NewCreateIndexOperator(
	schemaName, tableName, indexName string,
	columns []string,
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
		indexName:  indexName,
		tableName:  tableName,
		schemaName: schemaName,
		columns:    columns,
		unique:     unique,
		indexType:  indexType,
		catalog:    cat,
		storage:    storage,
		indexMgr:   indexMgr,
		executed:   false,
		schema:     schema,
	}
}

// Open initializes the CREATE INDEX operation.
func (c *CreateIndexOperator) Open(ctx *ExecContext) error {
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
		SchemaName: c.schemaName,
		TableName:  c.tableName,
		IndexName:  c.indexName,
		Type:       idxType,
		IsUnique:   c.unique,
		Columns:    indexColumns,
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

	// TODO: Populate index with existing data
	// This would involve scanning the table and inserting all rows into the index
	// For now, the index will only work for new data

	c.executed = true

	// Update statistics
	// Note: DDL operations don't read rows, so we don't update RowsRead

	return nil
}

// Next returns the result of CREATE INDEX
func (c *CreateIndexOperator) Next() (*Row, error) {
	if !c.executed {
		return nil, fmt.Errorf("CREATE INDEX not executed")
	}

	// Return result once
	if c.executed {
		c.executed = false // Ensure we only return once

		result := fmt.Sprintf("Index '%s' created on table '%s.%s'",
			c.indexName, c.schemaName, c.tableName)

		return &Row{
			Values: []types.Value{
				types.NewTextValue(result),
			},
		}, nil
	}

	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
}

// Close cleans up after CREATE INDEX
func (c *CreateIndexOperator) Close() error {
	c.executed = false
	return nil
}

// Schema returns the operator schema
func (c *CreateIndexOperator) Schema() *Schema {
	return c.schema
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
