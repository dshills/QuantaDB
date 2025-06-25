package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// DropTableOperator executes DROP TABLE statements
type DropTableOperator struct {
	baseOperator
	tableName  string
	schemaName string
	catalog    catalog.Catalog
	storage    StorageBackend
	executed   bool
}

// NewDropTableOperator creates a new DROP TABLE operator
func NewDropTableOperator(
	schemaName, tableName string,
	cat catalog.Catalog,
	storage StorageBackend,
) *DropTableOperator {
	// Schema for DROP TABLE result
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &DropTableOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		tableName:  tableName,
		schemaName: schemaName,
		catalog:    cat,
		storage:    storage,
		executed:   false,
	}
}

// Open initializes the DROP TABLE operation
func (d *DropTableOperator) Open(ctx *ExecContext) error {
	d.ctx = ctx

	// Check if table exists
	table, err := d.catalog.GetTable(d.schemaName, d.tableName)
	if err != nil {
		return fmt.Errorf("table '%s.%s' does not exist", d.schemaName, d.tableName)
	}

	// Drop storage for the table first
	err = d.storage.DropTable(table.ID)
	if err != nil {
		return fmt.Errorf("failed to drop table storage: %w", err)
	}

	// Remove table from catalog
	err = d.catalog.DropTable(d.schemaName, d.tableName)
	if err != nil {
		// This is problematic - storage is gone but catalog still has it
		// In a production system, we'd need proper transactional DDL
		return fmt.Errorf("failed to drop table from catalog: %w", err)
	}

	d.executed = true

	// Invalidate result cache for this table
	d.ctx.InvalidateResultCacheForTable(d.schemaName, d.tableName)

	// Update statistics
	if d.ctx.Stats != nil {
		d.ctx.Stats.RowsReturned = 1
	}

	return nil
}

// Next returns the result
func (d *DropTableOperator) Next() (*Row, error) {
	if d.executed {
		result := &Row{
			Values: []types.Value{
				types.NewValue(fmt.Sprintf("Table '%s.%s' dropped", d.schemaName, d.tableName)),
			},
		}
		d.executed = false // Ensure we only return once
		return result, nil
	}

	return nil, nil // nolint:nilnil // EOF - standard iterator pattern
}

// Close cleans up resources
func (d *DropTableOperator) Close() error {
	// Nothing to clean up
	return nil
}
