package index

import (
	"fmt"
	"sync"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Manager manages all indexes for the database.
type Manager struct {
	mu      sync.RWMutex
	indexes map[string]Index // key is "schema.table.index_name"
	catalog catalog.Catalog
}

// NewManager creates a new index manager.
func NewManager(cat catalog.Catalog) *Manager {
	return &Manager{
		indexes: make(map[string]Index),
		catalog: cat,
	}
}

// CreateIndex creates a new index.
func (m *Manager) CreateIndex(schemaName, tableName, indexName string, columns []string, unique bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get table schema to validate columns
	table, err := m.catalog.GetTable(schemaName, tableName)
	if err != nil {
		return fmt.Errorf("table not found: %w", err)
	}

	// Validate columns exist
	nullable := false
	for _, colName := range columns {
		found := false
		for _, col := range table.Columns {
			if col.Name == colName {
				found = true
				if col.IsNullable {
					nullable = true
				}
				break
			}
		}
		if !found {
			return fmt.Errorf("column %s not found in table", colName)
		}
	}

	// Check if index already exists
	key := m.getIndexKey(schemaName, tableName, indexName)
	if _, exists := m.indexes[key]; exists {
		return fmt.Errorf("index %s already exists", indexName)
	}

	// For now, create a standard B+Tree index
	// The composite key encoding will be handled at insert/delete time
	idx := NewBTreeIndex(unique, nullable)
	m.indexes[key] = idx

	// TODO: If table has existing data, populate the index

	return nil
}

// DropIndex removes an index.
func (m *Manager) DropIndex(schemaName, tableName, indexName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getIndexKey(schemaName, tableName, indexName)
	if _, exists := m.indexes[key]; !exists {
		return fmt.Errorf("index %s not found", indexName)
	}

	delete(m.indexes, key)
	return nil
}

// GetIndex retrieves an index by name.
func (m *Manager) GetIndex(schemaName, tableName, indexName string) (Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.getIndexKey(schemaName, tableName, indexName)
	idx, exists := m.indexes[key]
	if !exists {
		return nil, fmt.Errorf("index %s not found", indexName)
	}

	return idx, nil
}

// GetTableIndexes returns all indexes for a table.
func (m *Manager) GetTableIndexes(schemaName, tableName string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := fmt.Sprintf("%s.%s.", schemaName, tableName)
	var indexes []string

	for key := range m.indexes {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			indexName := key[len(prefix):]
			indexes = append(indexes, indexName)
		}
	}

	return indexes
}

// encodeIndexKey encodes the index key based on the index columns
func (m *Manager) encodeIndexKey(indexMeta *catalog.Index, row map[string]types.Value) ([]byte, error) {
	encoder := KeyEncoder{}

	if len(indexMeta.Columns) == 1 {
		// Single column index
		colName := indexMeta.Columns[0].Column.Name
		val, exists := row[colName]
		if !exists {
			return nil, fmt.Errorf("column %s not found in row", colName)
		}

		return encoder.EncodeValue(val)
	}

	// Composite index - extract values in order
	values := make([]types.Value, len(indexMeta.Columns))
	for i, indexCol := range indexMeta.Columns {
		colName := indexCol.Column.Name
		val, exists := row[colName]
		if !exists {
			return nil, fmt.Errorf("column %s not found in row", colName)
		}
		values[i] = val
	}

	return encoder.EncodeMultiColumn(values)
}

// InsertIntoIndexes updates all indexes when a row is inserted.
func (m *Manager) InsertIntoIndexes(schemaName, tableName string, row map[string]types.Value, rowID []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Update each index
	for indexName := range m.getTableIndexMap(schemaName, tableName) {
		idx, err := m.GetIndex(schemaName, tableName, indexName)
		if err != nil {
			continue
		}

		// Get index metadata from catalog
		indexMeta, err := m.catalog.GetIndex(schemaName, tableName, indexName)
		if err != nil {
			continue
		}

		// Build key based on index columns
		key, err := m.encodeIndexKey(indexMeta, row)
		if err != nil {
			return err
		}

		// Insert into index
		if err := idx.Insert(key, rowID); err != nil {
			return fmt.Errorf("failed to insert into index %s: %w", indexName, err)
		}
	}

	return nil
}

// DeleteFromIndexes updates all indexes when a row is deleted.
func (m *Manager) DeleteFromIndexes(schemaName, tableName string, row map[string]types.Value) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Update each index
	for indexName := range m.getTableIndexMap(schemaName, tableName) {
		idx, err := m.GetIndex(schemaName, tableName, indexName)
		if err != nil {
			continue
		}

		// Get index metadata from catalog
		indexMeta, err := m.catalog.GetIndex(schemaName, tableName, indexName)
		if err != nil {
			continue
		}

		// Build key based on index columns
		key, err := m.encodeIndexKey(indexMeta, row)
		if err != nil {
			return err
		}

		// Delete from index
		if err := idx.Delete(key); err != nil {
			// Ignore not found errors during delete
			continue
		}
	}

	return nil
}

// getIndexKey creates a unique key for an index.
func (m *Manager) getIndexKey(schemaName, tableName, indexName string) string {
	return fmt.Sprintf("%s.%s.%s", schemaName, tableName, indexName)
}

// getTableIndexMap returns all indexes for a table as a map.
func (m *Manager) getTableIndexMap(schemaName, tableName string) map[string]bool {
	prefix := fmt.Sprintf("%s.%s.", schemaName, tableName)
	indexes := make(map[string]bool)

	for key := range m.indexes {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			indexName := key[len(prefix):]
			indexes[indexName] = true
		}
	}

	return indexes
}

// IndexMetadata stores information about an index.
type IndexMetadata struct { //nolint:revive // Established API
	Name       string
	SchemaName string
	TableName  string
	Columns    []string
	Unique     bool
	Primary    bool
	Type       IndexType
}

// CreatePrimaryKeyIndex creates an index for a primary key constraint.
func (m *Manager) CreatePrimaryKeyIndex(schemaName, tableName string, columns []string) error {
	indexName := fmt.Sprintf("%s_pkey", tableName)
	return m.CreateIndex(schemaName, tableName, indexName, columns, true)
}

// Stats returns statistics for all indexes.
func (m *Manager) Stats() map[string]IndexStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]IndexStats)
	for key, idx := range m.indexes {
		stats[key] = idx.Stats()
	}

	return stats
}
