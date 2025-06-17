package catalog

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestMemoryCatalogSchema(t *testing.T) {
	catalog := NewMemoryCatalog()

	t.Run("Default public schema exists", func(t *testing.T) {
		schemas, err := catalog.ListSchemas()
		if err != nil {
			t.Fatalf("Failed to list schemas: %v", err)
		}

		found := false
		for _, schema := range schemas {
			if schema == "public" {
				found = true
				break
			}
		}

		if !found {
			t.Error("Expected public schema to exist by default")
		}
	})

	t.Run("Create schema", func(t *testing.T) {
		err := catalog.CreateSchema("test_schema")
		if err != nil {
			t.Fatalf("Failed to create schema: %v", err)
		}

		schemas, err := catalog.ListSchemas()
		if err != nil {
			t.Fatalf("Failed to list schemas: %v", err)
		}

		found := false
		for _, schema := range schemas {
			if schema == "test_schema" {
				found = true
				break
			}
		}

		if !found {
			t.Error("Expected test_schema to exist after creation")
		}
	})

	t.Run("Create duplicate schema", func(t *testing.T) {
		err := catalog.CreateSchema("dup_schema")
		if err != nil {
			t.Fatalf("Failed to create schema: %v", err)
		}

		err = catalog.CreateSchema("dup_schema")
		if err == nil {
			t.Error("Expected error when creating duplicate schema")
		}
	})

	t.Run("Drop schema", func(t *testing.T) {
		err := catalog.CreateSchema("drop_schema")
		if err != nil {
			t.Fatalf("Failed to create schema: %v", err)
		}

		err = catalog.DropSchema("drop_schema")
		if err != nil {
			t.Fatalf("Failed to drop schema: %v", err)
		}

		schemas, err := catalog.ListSchemas()
		if err != nil {
			t.Fatalf("Failed to list schemas: %v", err)
		}

		for _, schema := range schemas {
			if schema == "drop_schema" {
				t.Error("Schema should not exist after dropping")
			}
		}
	})

	t.Run("Cannot drop public schema", func(t *testing.T) {
		err := catalog.DropSchema("public")
		if err == nil {
			t.Error("Expected error when dropping public schema")
		}
	})
}

func TestMemoryCatalogTable(t *testing.T) {
	catalog := NewMemoryCatalog()

	t.Run("Create table", func(t *testing.T) {
		schema := &TableSchema{
			SchemaName: "public",
			TableName:  "users",
			Columns: []ColumnDef{
				{
					Name:       "id",
					DataType:   types.Integer,
					IsNullable: false,
				},
				{
					Name:       "name",
					DataType:   types.Varchar(100),
					IsNullable: false,
				},
				{
					Name:       "email",
					DataType:   types.Varchar(255),
					IsNullable: true,
				},
			},
		}

		table, err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		if table.TableName != "users" {
			t.Errorf("Expected table name 'users', got %q", table.TableName)
		}

		if len(table.Columns) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(table.Columns))
		}

		// Verify columns
		expectedColumns := []struct {
			name       string
			nullable   bool
			position   int
		}{
			{"id", false, 1},
			{"name", false, 2},
			{"email", true, 3},
		}

		for i, expected := range expectedColumns {
			col := table.Columns[i]
			if col.Name != expected.name {
				t.Errorf("Column %d: expected name %q, got %q", i, expected.name, col.Name)
			}
			if col.IsNullable != expected.nullable {
				t.Errorf("Column %d: expected nullable %v, got %v", i, expected.nullable, col.IsNullable)
			}
			if col.OrdinalPosition != expected.position {
				t.Errorf("Column %d: expected position %d, got %d", i, expected.position, col.OrdinalPosition)
			}
		}
	})

	t.Run("Get table", func(t *testing.T) {
		// Create a table first
		schema := &TableSchema{
			SchemaName: "public",
			TableName:  "products",
			Columns: []ColumnDef{
				{Name: "id", DataType: types.Integer, IsNullable: false},
			},
		}

		_, err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Get the table
		table, err := catalog.GetTable("public", "products")
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}

		if table.TableName != "products" {
			t.Errorf("Expected table name 'products', got %q", table.TableName)
		}
	})

	t.Run("Get table with empty schema uses public", func(t *testing.T) {
		table, err := catalog.GetTable("", "products")
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}

		if table.SchemaName != "public" {
			t.Errorf("Expected schema 'public', got %q", table.SchemaName)
		}
	})

	t.Run("Get non-existent table", func(t *testing.T) {
		_, err := catalog.GetTable("public", "non_existent")
		if err == nil {
			t.Error("Expected error when getting non-existent table")
		}
	})

	t.Run("Create duplicate table", func(t *testing.T) {
		schema := &TableSchema{
			SchemaName: "public",
			TableName:  "dup_table",
			Columns: []ColumnDef{
				{Name: "id", DataType: types.Integer},
			},
		}

		_, err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		_, err = catalog.CreateTable(schema)
		if err == nil {
			t.Error("Expected error when creating duplicate table")
		}
	})

	t.Run("Drop table", func(t *testing.T) {
		schema := &TableSchema{
			SchemaName: "public",
			TableName:  "drop_table",
			Columns: []ColumnDef{
				{Name: "id", DataType: types.Integer},
			},
		}

		_, err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		err = catalog.DropTable("public", "drop_table")
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		_, err = catalog.GetTable("public", "drop_table")
		if err == nil {
			t.Error("Expected error when getting dropped table")
		}
	})

	t.Run("List tables", func(t *testing.T) {
		// Create a new schema for isolation
		err := catalog.CreateSchema("list_test")
		if err != nil {
			t.Fatalf("Failed to create schema: %v", err)
		}

		// Create some tables
		tables := []string{"table1", "table2", "table3"}
		for _, tableName := range tables {
			schema := &TableSchema{
				SchemaName: "list_test",
				TableName:  tableName,
				Columns: []ColumnDef{
					{Name: "id", DataType: types.Integer},
				},
			}

			_, err := catalog.CreateTable(schema)
			if err != nil {
				t.Fatalf("Failed to create table %q: %v", tableName, err)
			}
		}

		// List tables
		list, err := catalog.ListTables("list_test")
		if err != nil {
			t.Fatalf("Failed to list tables: %v", err)
		}

		if len(list) != 3 {
			t.Errorf("Expected 3 tables, got %d", len(list))
		}

		// Verify all tables are present
		for _, expectedName := range tables {
			found := false
			for _, table := range list {
				if table.TableName == expectedName {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected table %q not found in list", expectedName)
			}
		}
	})
}

func TestMemoryCatalogConstraints(t *testing.T) {
	catalog := NewMemoryCatalog()

	t.Run("Primary key constraint", func(t *testing.T) {
		schema := &TableSchema{
			SchemaName: "public",
			TableName:  "pk_table",
			Columns: []ColumnDef{
				{Name: "id", DataType: types.Integer, IsNullable: false},
				{Name: "name", DataType: types.Text, IsNullable: true},
			},
			Constraints: []Constraint{
				PrimaryKeyConstraint{Columns: []string{"id"}},
			},
		}

		table, err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Check that primary key index was created
		if len(table.Indexes) != 1 {
			t.Fatalf("Expected 1 index, got %d", len(table.Indexes))
		}

		index := table.Indexes[0]
		if !index.IsPrimary {
			t.Error("Expected index to be primary key")
		}
		if !index.IsUnique {
			t.Error("Expected primary key index to be unique")
		}
		if index.Name != "pk_table_pkey" {
			t.Errorf("Expected index name 'pk_table_pkey', got %q", index.Name)
		}
	})

	t.Run("Column constraints", func(t *testing.T) {
		schema := &TableSchema{
			SchemaName: "public",
			TableName:  "constraint_table",
			Columns: []ColumnDef{
				{
					Name:       "id",
					DataType:   types.Integer,
					IsNullable: true, // Will be overridden by NotNullConstraint
					Constraints: []ColumnConstraint{
						NotNullConstraint{},
					},
				},
				{
					Name:       "status",
					DataType:   types.Text,
					IsNullable: true,
					Constraints: []ColumnConstraint{
						DefaultConstraint{Value: types.NewValue("active")},
					},
				},
			},
		}

		table, err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Check NOT NULL constraint was applied
		idCol := table.Columns[0]
		if idCol.IsNullable {
			t.Error("Expected id column to be NOT NULL")
		}

		// Check DEFAULT constraint was applied
		statusCol := table.Columns[1]
		if statusCol.DefaultValue.IsNull() {
			t.Error("Expected status column to have default value")
		}
		if statusCol.DefaultValue.Data != "active" {
			t.Errorf("Expected default value 'active', got %v", statusCol.DefaultValue.Data)
		}
	})
}

func TestMemoryCatalogIndex(t *testing.T) {
	catalog := NewMemoryCatalog()

	// Create a table first
	tableSchema := &TableSchema{
		SchemaName: "public",
		TableName:  "index_table",
		Columns: []ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "email", DataType: types.Varchar(255), IsNullable: false},
			{Name: "created_at", DataType: types.Timestamp, IsNullable: false},
		},
	}

	_, err := catalog.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Run("Create index", func(t *testing.T) {
		indexSchema := &IndexSchema{
			SchemaName: "public",
			TableName:  "index_table",
			IndexName:  "idx_email",
			Type:       BTreeIndex,
			IsUnique:   true,
			Columns: []IndexColumnDef{
				{ColumnName: "email", SortOrder: Ascending},
			},
		}

		index, err := catalog.CreateIndex(indexSchema)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}

		if index.Name != "idx_email" {
			t.Errorf("Expected index name 'idx_email', got %q", index.Name)
		}
		if !index.IsUnique {
			t.Error("Expected index to be unique")
		}
		if index.Type != BTreeIndex {
			t.Errorf("Expected BTree index, got %v", index.Type)
		}
	})

	t.Run("Create composite index", func(t *testing.T) {
		indexSchema := &IndexSchema{
			SchemaName: "public",
			TableName:  "index_table",
			IndexName:  "idx_composite",
			Type:       BTreeIndex,
			IsUnique:   false,
			Columns: []IndexColumnDef{
				{ColumnName: "email", SortOrder: Ascending},
				{ColumnName: "created_at", SortOrder: Descending},
			},
		}

		index, err := catalog.CreateIndex(indexSchema)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}

		if len(index.Columns) != 2 {
			t.Fatalf("Expected 2 columns in index, got %d", len(index.Columns))
		}

		// Check column order
		if index.Columns[0].Column.Name != "email" {
			t.Errorf("Expected first column 'email', got %q", index.Columns[0].Column.Name)
		}
		if index.Columns[0].SortOrder != Ascending {
			t.Error("Expected first column ascending order")
		}

		if index.Columns[1].Column.Name != "created_at" {
			t.Errorf("Expected second column 'created_at', got %q", index.Columns[1].Column.Name)
		}
		if index.Columns[1].SortOrder != Descending {
			t.Error("Expected second column descending order")
		}
	})

	t.Run("Get index", func(t *testing.T) {
		index, err := catalog.GetIndex("public", "index_table", "idx_email")
		if err != nil {
			t.Fatalf("Failed to get index: %v", err)
		}

		if index.Name != "idx_email" {
			t.Errorf("Expected index name 'idx_email', got %q", index.Name)
		}
	})

	t.Run("Drop index", func(t *testing.T) {
		// Create an index to drop
		indexSchema := &IndexSchema{
			SchemaName: "public",
			TableName:  "index_table",
			IndexName:  "idx_to_drop",
			Type:       BTreeIndex,
			Columns: []IndexColumnDef{
				{ColumnName: "id", SortOrder: Ascending},
			},
		}

		_, err := catalog.CreateIndex(indexSchema)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}

		// Drop the index
		err = catalog.DropIndex("public", "index_table", "idx_to_drop")
		if err != nil {
			t.Fatalf("Failed to drop index: %v", err)
		}

		// Verify it's gone
		_, err = catalog.GetIndex("public", "index_table", "idx_to_drop")
		if err == nil {
			t.Error("Expected error when getting dropped index")
		}
	})

	t.Run("Cannot drop primary key index", func(t *testing.T) {
		// Create table with primary key
		pkSchema := &TableSchema{
			SchemaName: "public",
			TableName:  "pk_drop_test",
			Columns: []ColumnDef{
				{Name: "id", DataType: types.Integer},
			},
			Constraints: []Constraint{
				PrimaryKeyConstraint{Columns: []string{"id"}},
			},
		}

		table, err := catalog.CreateTable(pkSchema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Try to drop primary key index
		pkIndex := table.GetPrimaryKey()
		if pkIndex == nil {
			t.Fatal("Expected primary key index")
		}

		err = catalog.DropIndex("public", "pk_drop_test", pkIndex.Name)
		if err == nil {
			t.Error("Expected error when dropping primary key index")
		}
	})
}

func TestMemoryCatalogStats(t *testing.T) {
	catalog := NewMemoryCatalog()

	// Create a table
	schema := &TableSchema{
		SchemaName: "public",
		TableName:  "stats_table",
		Columns: []ColumnDef{
			{Name: "id", DataType: types.Integer},
		},
	}

	_, err := catalog.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Run("Initial stats", func(t *testing.T) {
		stats, err := catalog.GetTableStats("public", "stats_table")
		if err != nil {
			t.Fatalf("Failed to get stats: %v", err)
		}

		if stats.RowCount != 0 {
			t.Errorf("Expected row count 0, got %d", stats.RowCount)
		}
		if !stats.LastAnalyzed.IsZero() {
			t.Error("Expected LastAnalyzed to be zero time initially")
		}
	})

	t.Run("Update stats", func(t *testing.T) {
		err := catalog.UpdateTableStats("public", "stats_table")
		if err != nil {
			t.Fatalf("Failed to update stats: %v", err)
		}

		stats, err := catalog.GetTableStats("public", "stats_table")
		if err != nil {
			t.Fatalf("Failed to get stats: %v", err)
		}

		if stats.LastAnalyzed.IsZero() {
			t.Error("Expected LastAnalyzed to be updated")
		}
	})
}

func TestTableHelperMethods(t *testing.T) {
	table := &Table{
		TableName: "test",
		Columns: []*Column{
			{ID: 1, Name: "id", DataType: types.Integer},
			{ID: 2, Name: "name", DataType: types.Text},
			{ID: 3, Name: "email", DataType: types.Text},
		},
	}

	t.Run("GetColumnByName", func(t *testing.T) {
		col := table.GetColumnByName("name")
		if col == nil {
			t.Fatal("Expected to find column 'name'")
		}
		if col.ID != 2 {
			t.Errorf("Expected column ID 2, got %d", col.ID)
		}

		// Non-existent column
		col = table.GetColumnByName("non_existent")
		if col != nil {
			t.Error("Expected nil for non-existent column")
		}
	})

	t.Run("GetPrimaryKey", func(t *testing.T) {
		// No primary key
		pk := table.GetPrimaryKey()
		if pk != nil {
			t.Error("Expected nil when no primary key")
		}

		// Add primary key
		table.Indexes = []*Index{
			{ID: 1, Name: "idx1", IsPrimary: false},
			{ID: 2, Name: "test_pkey", IsPrimary: true},
		}

		pk = table.GetPrimaryKey()
		if pk == nil {
			t.Fatal("Expected to find primary key")
		}
		if pk.ID != 2 {
			t.Errorf("Expected primary key ID 2, got %d", pk.ID)
		}
	})
}