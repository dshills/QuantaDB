package planner

import (
	"github.com/dshills/QuantaDB/internal/catalog"
)

// InvalidatingCatalog wraps a catalog and provides plan cache invalidation
// when schema changes occur. This follows the decorator pattern to add
// cache invalidation capabilities without modifying the core catalog.
type InvalidatingCatalog struct {
	catalog.Catalog
	planner *CachingPlanner
}

// NewInvalidatingCatalog creates a new invalidating catalog wrapper
func NewInvalidatingCatalog(baseCatalog catalog.Catalog, planner *CachingPlanner) *InvalidatingCatalog {
	return &InvalidatingCatalog{
		Catalog: baseCatalog,
		planner: planner,
	}
}

// CreateTable creates a table and invalidates the plan cache
func (ic *InvalidatingCatalog) CreateTable(schema *catalog.TableSchema) (*catalog.Table, error) {
	table, err := ic.Catalog.CreateTable(schema)
	if err != nil {
		return nil, err
	}

	// Invalidate cache after successful table creation
	if ic.planner != nil {
		ic.planner.UpdateSchemaVersion(ic.planner.schemaVersion + 1)
	}

	return table, nil
}

// DropTable drops a table and invalidates the plan cache
func (ic *InvalidatingCatalog) DropTable(schemaName, tableName string) error {
	err := ic.Catalog.DropTable(schemaName, tableName)
	if err != nil {
		return err
	}

	// Invalidate cache after successful table drop
	if ic.planner != nil {
		// For table-specific invalidation, we could be more selective
		ic.planner.InvalidateTable(tableName)
		ic.planner.UpdateSchemaVersion(ic.planner.schemaVersion + 1)
	}

	return nil
}

// AddColumn adds a column and invalidates the plan cache
func (ic *InvalidatingCatalog) AddColumn(schemaName, tableName string, column catalog.ColumnDef) error {
	err := ic.Catalog.AddColumn(schemaName, tableName, column)
	if err != nil {
		return err
	}

	// Invalidate cache after successful column addition
	if ic.planner != nil {
		ic.planner.InvalidateTable(tableName)
		ic.planner.UpdateSchemaVersion(ic.planner.schemaVersion + 1)
	}

	return nil
}

// DropColumn drops a column and invalidates the plan cache
func (ic *InvalidatingCatalog) DropColumn(schemaName, tableName, columnName string) error {
	err := ic.Catalog.DropColumn(schemaName, tableName, columnName)
	if err != nil {
		return err
	}

	// Invalidate cache after successful column drop
	if ic.planner != nil {
		ic.planner.InvalidateTable(tableName)
		ic.planner.UpdateSchemaVersion(ic.planner.schemaVersion + 1)
	}

	return nil
}

// CreateIndex creates an index and invalidates the plan cache
func (ic *InvalidatingCatalog) CreateIndex(index *catalog.IndexSchema) (*catalog.Index, error) {
	result, err := ic.Catalog.CreateIndex(index)
	if err != nil {
		return nil, err
	}

	// Invalidate cache after successful index creation
	// Index changes can affect query plans significantly
	if ic.planner != nil {
		ic.planner.UpdateSchemaVersion(ic.planner.schemaVersion + 1)
	}

	return result, nil
}

// DropIndex drops an index and invalidates the plan cache
func (ic *InvalidatingCatalog) DropIndex(schemaName, tableName, indexName string) error {
	err := ic.Catalog.DropIndex(schemaName, tableName, indexName)
	if err != nil {
		return err
	}

	// Invalidate cache after successful index drop
	// Index changes can affect query plans significantly
	if ic.planner != nil {
		ic.planner.UpdateSchemaVersion(ic.planner.schemaVersion + 1)
	}

	return nil
}

// UpdateTableStats updates table statistics and invalidates the plan cache
func (ic *InvalidatingCatalog) UpdateTableStats(schemaName, tableName string) error {
	err := ic.Catalog.UpdateTableStats(schemaName, tableName)
	if err != nil {
		return err
	}

	// Invalidate cache after successful statistics update
	// Statistics changes can affect query plans
	if ic.planner != nil {
		ic.planner.UpdateStatsVersion(ic.planner.statsVersion + 1)
	}

	return nil
}

// CreateSchema creates a schema and invalidates the plan cache
func (ic *InvalidatingCatalog) CreateSchema(name string) error {
	err := ic.Catalog.CreateSchema(name)
	if err != nil {
		return err
	}

	// Invalidate cache after successful schema creation
	if ic.planner != nil {
		ic.planner.UpdateSchemaVersion(ic.planner.schemaVersion + 1)
	}

	return nil
}

// DropSchema drops a schema and invalidates the plan cache
func (ic *InvalidatingCatalog) DropSchema(name string) error {
	err := ic.Catalog.DropSchema(name)
	if err != nil {
		return err
	}

	// Invalidate cache after successful schema drop
	if ic.planner != nil {
		ic.planner.UpdateSchemaVersion(ic.planner.schemaVersion + 1)
	}

	return nil
}

// SetPlanner updates the planner reference for cache invalidation
func (ic *InvalidatingCatalog) SetPlanner(planner *CachingPlanner) {
	ic.planner = planner
}
