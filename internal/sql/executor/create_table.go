package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CreateTableOperator executes CREATE TABLE statements
type CreateTableOperator struct {
	baseOperator
	tableName   string
	schemaName  string
	columns     []catalog.Column
	constraints []catalog.Constraint
	catalog     catalog.Catalog
	storage     StorageBackend
	executed    bool
}

// NewCreateTableOperator creates a new CREATE TABLE operator
func NewCreateTableOperator(
	schemaName, tableName string,
	columns []catalog.Column,
	constraints []catalog.Constraint,
	cat catalog.Catalog,
	storage StorageBackend,
) *CreateTableOperator {
	// Schema for CREATE TABLE result
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &CreateTableOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		tableName:   tableName,
		schemaName:  schemaName,
		columns:     columns,
		constraints: constraints,
		catalog:     cat,
		storage:     storage,
		executed:    false,
	}
}

// Open initializes the CREATE TABLE operation
func (c *CreateTableOperator) Open(ctx *ExecContext) error {
	c.ctx = ctx

	// Check if table already exists
	_, err := c.catalog.GetTable(c.schemaName, c.tableName)
	if err == nil {
		return fmt.Errorf("table '%s.%s' already exists", c.schemaName, c.tableName)
	}

	// Create table schema
	tableSchema := &catalog.TableSchema{
		SchemaName:  c.schemaName,
		TableName:   c.tableName,
		Columns:     make([]catalog.ColumnDef, len(c.columns)),
		Constraints: c.constraints,
	}
	
	// Convert columns to ColumnDef
	for i, col := range c.columns {
		tableSchema.Columns[i] = catalog.ColumnDef{
			Name:         col.Name,
			DataType:     col.DataType,
			IsNullable:   col.IsNullable,
			DefaultValue: col.DefaultValue,
		}
	}

	// Add table to catalog
	table, err := c.catalog.CreateTable(tableSchema)
	if err != nil {
		return fmt.Errorf("failed to create table in catalog: %w", err)
	}

	// Create storage for the table
	err = c.storage.CreateTable(table)
	if err != nil {
		// Rollback catalog change
		_ = c.catalog.DropTable(c.schemaName, c.tableName)
		return fmt.Errorf("failed to create table storage: %w", err)
	}

	c.executed = true

	// Update statistics
	if c.ctx.Stats != nil {
		c.ctx.Stats.RowsReturned = 1
	}

	return nil
}

// Next returns the result
func (c *CreateTableOperator) Next() (*Row, error) {
	if c.executed {
		result := &Row{
			Values: []types.Value{
				types.NewValue(fmt.Sprintf("Table '%s.%s' created", c.schemaName, c.tableName)),
			},
		}
		c.executed = false // Ensure we only return once
		return result, nil
	}

	return nil, nil // EOF
}

// Close cleans up resources
func (c *CreateTableOperator) Close() error {
	// Nothing to clean up
	return nil
}