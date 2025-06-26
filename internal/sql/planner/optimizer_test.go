package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestIndexSelectionRule(t *testing.T) {
	// Create catalog with a test table and index
	cat := catalog.NewMemoryCatalog()

	// Create test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "email", DataType: types.Text, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: true},
		},
	}

	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index on email column
	indexSchema := &catalog.IndexSchema{
		SchemaName: "public",
		TableName:  "users",
		IndexName:  "idx_users_email",
		Type:       catalog.BTreeIndex,
		IsUnique:   true,
		Columns: []catalog.IndexColumnDef{
			{ColumnName: "email", SortOrder: catalog.Ascending},
		},
	}

	_, err = cat.CreateIndex(indexSchema)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Create schema for the scan
	schema := &Schema{
		Columns: []Column{
			{Name: "id", DataType: types.Integer, Nullable: false},
			{Name: "email", DataType: types.Text, Nullable: false},
			{Name: "name", DataType: types.Text, Nullable: true},
		},
	}

	tests := []struct {
		name           string
		query          string
		expectIndexUse bool
		indexName      string
	}{
		{
			name:           "Equality predicate on indexed column",
			query:          "email = 'test@example.com'",
			expectIndexUse: true,
			indexName:      "idx_users_email",
		},
		{
			name:           "Range predicate on indexed column",
			query:          "email > 'a@example.com'",
			expectIndexUse: true,
			indexName:      "idx_users_email",
		},
		{
			name:           "Predicate on non-indexed column",
			query:          "name = 'John'",
			expectIndexUse: false,
			indexName:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scan + filter plan manually
			scan := NewLogicalScan("users", "users", schema)

			// Create predicate based on test case
			var predicate Expression
			switch tt.query {
			case "email = 'test@example.com'":
				predicate = &BinaryOp{
					Left:     &ColumnRef{ColumnName: "email", ColumnType: types.Text},
					Right:    &Literal{Value: types.NewTextValue("test@example.com"), Type: types.Text},
					Operator: OpEqual,
					Type:     types.Boolean,
				}
			case "email > 'a@example.com'":
				predicate = &BinaryOp{
					Left:     &ColumnRef{ColumnName: "email", ColumnType: types.Text},
					Right:    &Literal{Value: types.NewTextValue("a@example.com"), Type: types.Text},
					Operator: OpGreater,
					Type:     types.Boolean,
				}
			case "name = 'John'":
				predicate = &BinaryOp{
					Left:     &ColumnRef{ColumnName: "name", ColumnType: types.Text},
					Right:    &Literal{Value: types.NewTextValue("John"), Type: types.Text},
					Operator: OpEqual,
					Type:     types.Boolean,
				}
			}

			filter := NewLogicalFilter(scan, predicate)

			// Apply IndexSelection rule
			indexSelection := &IndexSelection{catalog: cat}
			result, applied := indexSelection.Apply(filter)

			if tt.expectIndexUse {
				if !applied {
					t.Errorf("Expected index selection to be applied but it wasn't")
					return
				}

				// Accept either IndexScan or CompositeIndexScan
				var indexName, tableName string
				switch scan := result.(type) {
				case *IndexScan:
					indexName = scan.IndexName
					tableName = scan.TableName
				case *CompositeIndexScan:
					indexName = scan.IndexName
					tableName = scan.TableName
				case *IndexOnlyScan:
					// IndexOnlyScan is even better than IndexScan - it's a covering index
					indexName = scan.IndexName
					tableName = scan.TableName
				default:
					t.Errorf("Expected IndexScan, CompositeIndexScan, or IndexOnlyScan but got %T", result)
					return
				}

				if indexName != tt.indexName {
					t.Errorf("Expected index name %s but got %s", tt.indexName, indexName)
				}

				if tableName != "users" {
					t.Errorf("Expected table name 'users' but got %s", tableName)
				}
			} else {
				if applied {
					t.Errorf("Expected index selection not to be applied but it was")
				}
			}
		})
	}
}

func TestIndexSelectionWithComplexPlan(t *testing.T) {
	// Create catalog with test table and index
	cat := catalog.NewMemoryCatalog()

	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "products",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "price", DataType: types.Integer, IsNullable: false},
		},
	}

	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index on price column
	indexSchema := &catalog.IndexSchema{
		SchemaName: "public",
		TableName:  "products",
		IndexName:  "idx_products_price",
		Type:       catalog.BTreeIndex,
		IsUnique:   false,
		Columns: []catalog.IndexColumnDef{
			{ColumnName: "price", SortOrder: catalog.Ascending},
		},
	}

	_, err = cat.CreateIndex(indexSchema)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	schema := &Schema{
		Columns: []Column{
			{Name: "id", DataType: types.Integer, Nullable: false},
			{Name: "price", DataType: types.Integer, Nullable: false},
		},
	}

	// Create a complex plan: Project -> Filter -> Scan
	scan := NewLogicalScan("products", "products", schema)

	predicate := &BinaryOp{
		Left:     &ColumnRef{ColumnName: "price", ColumnType: types.Integer},
		Right:    &Literal{Value: types.NewValue(int64(100)), Type: types.Integer},
		Operator: OpGreater,
		Type:     types.Boolean,
	}

	filter := NewLogicalFilter(scan, predicate)

	projections := []Expression{
		&ColumnRef{ColumnName: "id", ColumnType: types.Integer},
		&ColumnRef{ColumnName: "price", ColumnType: types.Integer},
	}
	project := NewLogicalProject(filter, projections, []string{"", ""}, schema)

	// Apply IndexSelection rule
	indexSelection := &IndexSelection{catalog: cat}
	result, applied := indexSelection.Apply(project)

	if !applied {
		t.Errorf("Expected index selection to be applied to nested plan")
		return
	}

	// Verify the structure: Project -> IndexScan
	projectResult, ok := result.(*LogicalProject)
	if !ok {
		t.Errorf("Expected LogicalProject but got %T", result)
		return
	}

	if len(projectResult.Children()) != 1 {
		t.Errorf("Expected project to have 1 child but got %d", len(projectResult.Children()))
		return
	}

	// Accept IndexScan, CompositeIndexScan, or IndexOnlyScan (covering index)
	var indexName string
	switch scan := projectResult.Children()[0].(type) {
	case *IndexScan:
		indexName = scan.IndexName
	case *CompositeIndexScan:
		indexName = scan.IndexName
	case *IndexOnlyScan:
		// IndexOnlyScan is even better - it's a covering index
		indexName = scan.IndexName
	default:
		t.Errorf("Expected IndexScan, CompositeIndexScan, or IndexOnlyScan as child but got %T", projectResult.Children()[0])
		return
	}

	if indexName != "idx_products_price" {
		t.Errorf("Expected index name 'idx_products_price' but got %s", indexName)
	}
}
