package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestIndexSelection(t *testing.T) {
	// Create catalog with a table and index
	cat := catalog.NewMemoryCatalog()

	// Create schema
	if err := cat.CreateSchema("test"); err != nil {
		t.Fatal(err)
	}

	// Create table
	tableSchema := &catalog.TableSchema{
		SchemaName: "test",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
			{Name: "email", DataType: types.Text, IsNullable: true},
		},
		Constraints: []catalog.Constraint{
			catalog.PrimaryKeyConstraint{Columns: []string{"id"}},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Verify primary key index was created
	if len(table.Indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(table.Indexes))
	}

	// Create secondary index on name
	indexSchema := &catalog.IndexSchema{
		SchemaName: "test",
		TableName:  "users",
		IndexName:  "idx_name",
		Type:       catalog.BTreeIndex,
		IsUnique:   false,
		Columns: []catalog.IndexColumnDef{
			{ColumnName: "name", SortOrder: catalog.Ascending},
		},
	}

	_, err = cat.CreateIndex(indexSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Create planner with catalog
	planner := NewBasicPlannerWithCatalog(cat)

	tests := []struct {
		name     string
		query    string
		wantScan string // Expected scan type in plan
	}{
		{
			name:     "Simple equality on indexed column",
			query:    "SELECT * FROM users WHERE id = 1",
			wantScan: "Scan(users)", // TODO: Should be IndexScan when implemented
		},
		{
			name:     "Range query on indexed column",
			query:    "SELECT * FROM users WHERE id > 10",
			wantScan: "Scan(users)", // TODO: Should be IndexScan when implemented
		},
		{
			name:     "Query on non-indexed column",
			query:    "SELECT * FROM users WHERE email = 'test@example.com'",
			wantScan: "Scan(users)",
		},
		{
			name:     "AND with indexed column",
			query:    "SELECT * FROM users WHERE id = 1 AND name = 'John'",
			wantScan: "Scan(users)", // TODO: Should be IndexScan when implemented
		},
		{
			name:     "OR with indexed columns",
			query:    "SELECT * FROM users WHERE id = 1 OR id = 2",
			wantScan: "Scan(users)", // OR requires multiple index scans
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse query
			p := parser.NewParser(tt.query)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			// Plan query
			plan, err := planner.Plan(stmt)
			if err != nil {
				t.Fatalf("Failed to plan query: %v", err)
			}

			// Check plan structure
			planStr := ExplainPlan(plan)
			t.Logf("Plan for %s:\n%s", tt.query, planStr)

			// TODO: Once index selection is fully implemented,
			// verify that appropriate indexes are being used
		})
	}
}

func TestIndexBoundsExtraction(t *testing.T) {
	// Create a simple index for testing
	col := &catalog.Column{
		Name:     "id",
		DataType: types.Integer,
	}

	index := &catalog.Index{
		Name: "test_index",
		Columns: []catalog.IndexColumn{
			{Column: col, SortOrder: catalog.Ascending},
		},
	}

	tests := []struct {
		name      string
		filter    Expression
		wantStart bool
		wantEnd   bool
		canUse    bool
	}{
		{
			name: "Equality condition",
			filter: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "id"},
				Right:    &Literal{Value: types.NewValue(int32(10))},
				Operator: OpEqual,
			},
			wantStart: true,
			wantEnd:   true,
			canUse:    true,
		},
		{
			name: "Greater than condition",
			filter: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "id"},
				Right:    &Literal{Value: types.NewValue(int32(10))},
				Operator: OpGreater,
			},
			wantStart: true,
			wantEnd:   false,
			canUse:    true,
		},
		{
			name: "Less than condition",
			filter: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "id"},
				Right:    &Literal{Value: types.NewValue(int32(10))},
				Operator: OpLess,
			},
			wantStart: false,
			wantEnd:   true,
			canUse:    true,
		},
		{
			name: "Non-indexed column",
			filter: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "name"},
				Right:    &Literal{Value: types.NewValue("John")},
				Operator: OpEqual,
			},
			canUse: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startKey, endKey, canUse := extractIndexBounds(index, tt.filter)

			if canUse != tt.canUse {
				t.Errorf("canUse = %v, want %v", canUse, tt.canUse)
			}

			if canUse {
				if (startKey != nil) != tt.wantStart {
					t.Errorf("startKey presence = %v, want %v", startKey != nil, tt.wantStart)
				}
				if (endKey != nil) != tt.wantEnd {
					t.Errorf("endKey presence = %v, want %v", endKey != nil, tt.wantEnd)
				}
			}
		})
	}
}
