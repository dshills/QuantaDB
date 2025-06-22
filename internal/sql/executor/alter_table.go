package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// AlterTableAddColumnOperator executes ALTER TABLE ADD COLUMN statements
type AlterTableAddColumnOperator struct {
	baseOperator
	tableName  string
	schemaName string
	column     *parser.ColumnDef
	catalog    catalog.Catalog
	storage    StorageBackend
	executed   bool
}

// NewAlterTableAddColumnOperator creates a new ALTER TABLE ADD COLUMN operator
func NewAlterTableAddColumnOperator(
	schemaName, tableName string,
	column *parser.ColumnDef,
	cat catalog.Catalog,
	storage StorageBackend,
) *AlterTableAddColumnOperator {
	// Schema for ALTER TABLE result
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &AlterTableAddColumnOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		tableName:  tableName,
		schemaName: schemaName,
		column:     column,
		catalog:    cat,
		storage:    storage,
		executed:   false,
	}
}

// Open initializes the ALTER TABLE ADD COLUMN operation
func (a *AlterTableAddColumnOperator) Open(ctx *ExecContext) error {
	a.ctx = ctx

	// Check if table exists
	table, err := a.catalog.GetTable(a.schemaName, a.tableName)
	if err != nil {
		return fmt.Errorf("table '%s.%s' does not exist", a.schemaName, a.tableName)
	}

	// Check if column already exists
	for _, col := range table.Columns {
		if col.Name == a.column.Name {
			return fmt.Errorf("column '%s' already exists in table '%s.%s'", a.column.Name, a.schemaName, a.tableName)
		}
	}

	// Convert parser.ColumnDef to catalog.ColumnDef
	catalogColumnDef := catalog.ColumnDef{
		Name:         a.column.Name,
		DataType:     a.column.DataType,
		IsNullable:   true,                    // Default to nullable
		DefaultValue: types.NewNullValue(),   // Default to NULL
	}

	// Process constraints to set nullability and default values
	for _, constraint := range a.column.Constraints {
		switch c := constraint.(type) {
		case parser.NotNullConstraint:
			catalogColumnDef.IsNullable = false
		case *parser.DefaultConstraint:
			// TODO: Properly evaluate default value expression
			// For now, we'll skip setting default values for simplicity
			_ = c // Avoid unused variable warning
		}
	}

	// Add column to catalog
	err = a.catalog.AddColumn(a.schemaName, a.tableName, catalogColumnDef)
	if err != nil {
		return fmt.Errorf("failed to add column to catalog: %w", err)
	}

	// TODO: Add column to storage backend (requires storage interface extension)
	// For now, we only update the catalog

	a.executed = true

	// Update statistics
	if a.ctx.Stats != nil {
		a.ctx.Stats.RowsReturned = 1
	}

	return nil
}

// Next returns the result
func (a *AlterTableAddColumnOperator) Next() (*Row, error) {
	if a.executed {
		result := &Row{
			Values: []types.Value{
				types.NewValue(fmt.Sprintf("Column '%s' added to table '%s.%s'", a.column.Name, a.schemaName, a.tableName)),
			},
		}
		a.executed = false // Ensure we only return once
		return result, nil
	}

	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
}

// Close cleans up resources
func (a *AlterTableAddColumnOperator) Close() error {
	// Nothing to clean up
	return nil
}

// AlterTableDropColumnOperator executes ALTER TABLE DROP COLUMN statements
type AlterTableDropColumnOperator struct {
	baseOperator
	tableName  string
	schemaName string
	columnName string
	catalog    catalog.Catalog
	storage    StorageBackend
	executed   bool
}

// NewAlterTableDropColumnOperator creates a new ALTER TABLE DROP COLUMN operator
func NewAlterTableDropColumnOperator(
	schemaName, tableName, columnName string,
	cat catalog.Catalog,
	storage StorageBackend,
) *AlterTableDropColumnOperator {
	// Schema for ALTER TABLE result
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &AlterTableDropColumnOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		tableName:  tableName,
		schemaName: schemaName,
		columnName: columnName,
		catalog:    cat,
		storage:    storage,
		executed:   false,
	}
}

// Open initializes the ALTER TABLE DROP COLUMN operation
func (d *AlterTableDropColumnOperator) Open(ctx *ExecContext) error {
	d.ctx = ctx

	// Check if table exists
	table, err := d.catalog.GetTable(d.schemaName, d.tableName)
	if err != nil {
		return fmt.Errorf("table '%s.%s' does not exist", d.schemaName, d.tableName)
	}

	// Check if column exists
	columnExists := false
	for _, col := range table.Columns {
		if col.Name == d.columnName {
			columnExists = true
			break
		}
	}

	if !columnExists {
		return fmt.Errorf("column '%s' does not exist in table '%s.%s'", d.columnName, d.schemaName, d.tableName)
	}

	// Check if this is the last column (not allowed in most SQL databases)
	if len(table.Columns) == 1 {
		return fmt.Errorf("cannot drop the last column from table '%s.%s'", d.schemaName, d.tableName)
	}

	// Remove column from catalog
	err = d.catalog.DropColumn(d.schemaName, d.tableName, d.columnName)
	if err != nil {
		return fmt.Errorf("failed to drop column from catalog: %w", err)
	}

	// TODO: Remove column from storage backend (requires storage interface extension)
	// For now, we only update the catalog

	d.executed = true

	// Update statistics
	if d.ctx.Stats != nil {
		d.ctx.Stats.RowsReturned = 1
	}

	return nil
}

// Next returns the result
func (d *AlterTableDropColumnOperator) Next() (*Row, error) {
	if d.executed {
		result := &Row{
			Values: []types.Value{
				types.NewValue(fmt.Sprintf("Column '%s' dropped from table '%s.%s'", d.columnName, d.schemaName, d.tableName)),
			},
		}
		d.executed = false // Ensure we only return once
		return result, nil
	}

	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
}

// Close cleans up resources
func (d *AlterTableDropColumnOperator) Close() error {
	// Nothing to clean up
	return nil
}