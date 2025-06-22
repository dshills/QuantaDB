package planner

import (
	"fmt"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestCompositeIndexIntegration(t *testing.T) {
	// Create a mock catalog with a table and composite indexes
	cat := &MockCatalog{
		tables: map[string]*catalog.Table{
			"public.employees": {
				ID:         1,
				SchemaName: "public",
				TableName:  "employees",
				Columns: []*catalog.Column{
					{ID: 1, Name: "company_id", DataType: types.Integer, OrdinalPosition: 1},
					{ID: 2, Name: "department", DataType: types.Text, OrdinalPosition: 2},
					{ID: 3, Name: "name", DataType: types.Text, OrdinalPosition: 3},
					{ID: 4, Name: "salary", DataType: types.Integer, OrdinalPosition: 4},
				},
				Indexes: []*catalog.Index{
					{
						ID:      1,
						Name:    "idx_company_dept",
						TableID: 1,
						Type:    1, // BTreeIndex
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "company_id", DataType: types.Integer}, Position: 0},
							{Column: &catalog.Column{Name: "department", DataType: types.Text}, Position: 1},
						},
					},
					{
						ID:      2,
						Name:    "idx_company_dept_name",
						TableID: 1,
						Type:    1, // BTreeIndex
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "company_id", DataType: types.Integer}, Position: 0},
							{Column: &catalog.Column{Name: "department", DataType: types.Text}, Position: 1},
							{Column: &catalog.Column{Name: "name", DataType: types.Text}, Position: 2},
						},
					},
				},
			},
		},
	}

	// Create optimizer with catalog
	optimizer := NewOptimizerWithCatalog(cat)

	// Test case 1: Two-column predicate should use composite index
	t.Run("TwoColumnPredicate", func(t *testing.T) {
		// Create logical plan: Filter(company_id=1 AND department='Engineering') -> Scan(employees)
		scan := NewLogicalScan("employees", "", nil)

		predicate := &BinaryOp{
			Left: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "company_id", ColumnType: types.Integer},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
				Type:     types.Boolean,
			},
			Operator: OpAnd,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "department", ColumnType: types.Text},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewTextValue("Engineering"), Type: types.Text},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should have been converted to a composite index scan
		if !IsCompositeIndexScan(optimized) {
			t.Errorf("Expected CompositeIndexScan, got %T: %s", optimized, optimized.String())
		}

		if compositeIndexScan, ok := GetCompositeIndexScan(optimized); ok {
			if compositeIndexScan.IndexName != "idx_company_dept" {
				t.Errorf("Expected index 'idx_company_dept', got %s", compositeIndexScan.IndexName)
			}
			if compositeIndexScan.IndexMatch.MatchingColumns != 2 {
				t.Errorf("Expected 2 matching columns, got %d", compositeIndexScan.IndexMatch.MatchingColumns)
			}
		}
	})

	// Test case 2: Three-column predicate should use the most specific index
	t.Run("ThreeColumnPredicate", func(t *testing.T) {
		// Create logical plan: Filter(company_id=1 AND department='Engineering' AND name='Alice') -> Scan(employees)
		scan := NewLogicalScan("employees", "", nil)

		predicate := &BinaryOp{
			Left: &BinaryOp{
				Left: &BinaryOp{
					Left:     &ColumnRef{ColumnName: "company_id", ColumnType: types.Integer},
					Operator: OpEqual,
					Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
					Type:     types.Boolean,
				},
				Operator: OpAnd,
				Right: &BinaryOp{
					Left:     &ColumnRef{ColumnName: "department", ColumnType: types.Text},
					Operator: OpEqual,
					Right:    &Literal{Value: types.NewTextValue("Engineering"), Type: types.Text},
					Type:     types.Boolean,
				},
				Type: types.Boolean,
			},
			Operator: OpAnd,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "name", ColumnType: types.Text},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewTextValue("Alice"), Type: types.Text},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should have been converted to a composite index scan using the 3-column index
		if !IsCompositeIndexScan(optimized) {
			t.Errorf("Expected CompositeIndexScan, got %T: %s", optimized, optimized.String())
		}

		if compositeIndexScan, ok := GetCompositeIndexScan(optimized); ok {
			if compositeIndexScan.IndexName != "idx_company_dept_name" {
				t.Errorf("Expected index 'idx_company_dept_name', got %s", compositeIndexScan.IndexName)
			}
			if compositeIndexScan.IndexMatch.MatchingColumns != 3 {
				t.Errorf("Expected 3 matching columns, got %d", compositeIndexScan.IndexMatch.MatchingColumns)
			}
		}
	})

	// Test case 3: Single column predicate should still work
	t.Run("SingleColumnPredicate", func(t *testing.T) {
		// Create logical plan: Filter(company_id=1) -> Scan(employees)
		scan := NewLogicalScan("employees", "", nil)

		predicate := &BinaryOp{
			Left:     &ColumnRef{ColumnName: "company_id", ColumnType: types.Integer},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
			Type:     types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should have been converted to some form of index scan
		// (Could be CompositeIndexScan or regular IndexScan)
		if !IsCompositeIndexScan(optimized) && !isIndexScan(optimized) {
			t.Errorf("Expected some form of index scan, got %T: %s", optimized, optimized.String())
		}
	})

	// Test case 4: OR predicate should not use composite index
	t.Run("OrPredicate", func(t *testing.T) {
		// Create logical plan: Filter(company_id=1 OR department='Engineering') -> Scan(employees)
		scan := NewLogicalScan("employees", "", nil)

		predicate := &BinaryOp{
			Left: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "company_id", ColumnType: types.Integer},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
				Type:     types.Boolean,
			},
			Operator: OpOr,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "department", ColumnType: types.Text},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewTextValue("Engineering"), Type: types.Text},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should NOT have been converted to any index scan due to OR predicate
		if IsCompositeIndexScan(optimized) || isIndexScan(optimized) {
			t.Errorf("Expected table scan (no index), got %T: %s", optimized, optimized.String())
		}
	})
}

// MockCatalog implements catalog.Catalog for testing
type MockCatalog struct {
	tables map[string]*catalog.Table
}

func (mc *MockCatalog) CreateTable(schema *catalog.TableSchema) (*catalog.Table, error) {
	return nil, nil
}

func (mc *MockCatalog) GetTable(schemaName, tableName string) (*catalog.Table, error) {
	key := schemaName + "." + tableName
	if table, exists := mc.tables[key]; exists {
		return table, nil
	}
	return nil, fmt.Errorf("table %s.%s not found", schemaName, tableName)
}

func (mc *MockCatalog) DropTable(schemaName, tableName string) error {
	return nil
}

func (mc *MockCatalog) ListTables(schemaName string) ([]*catalog.Table, error) {
	return nil, nil
}

func (mc *MockCatalog) CreateIndex(index *catalog.IndexSchema) (*catalog.Index, error) {
	return nil, nil
}

func (mc *MockCatalog) GetIndex(schemaName, tableName, indexName string) (*catalog.Index, error) {
	return nil, nil
}

func (mc *MockCatalog) DropIndex(schemaName, tableName, indexName string) error {
	return nil
}

// Column operations (added for ALTER TABLE support)
func (mc *MockCatalog) AddColumn(schemaName, tableName string, column catalog.ColumnDef) error {
	return nil
}

func (mc *MockCatalog) DropColumn(schemaName, tableName, columnName string) error {
	return nil
}

func (mc *MockCatalog) UpdateTableStats(schemaName, tableName string) error {
	return nil
}

func (mc *MockCatalog) GetTableStats(schemaName, tableName string) (*catalog.TableStats, error) {
	return nil, nil
}

func (mc *MockCatalog) CreateSchema(name string) error {
	return nil
}

func (mc *MockCatalog) DropSchema(name string) error {
	return nil
}

func (mc *MockCatalog) ListSchemas() ([]string, error) {
	return nil, nil
}

// Helper function to check if a plan is a regular IndexScan
func isIndexScan(plan Plan) bool {
	_, ok := plan.(*IndexScan)
	return ok
}
