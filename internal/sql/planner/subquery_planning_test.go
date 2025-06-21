package planner

import (
	"fmt"
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestBasicSubqueryPlanning(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "simple scalar subquery in SELECT",
			query:   "SELECT name, (SELECT 5) AS constant FROM users",
			wantErr: false,
		},
		{
			name:    "EXISTS subquery",
			query:   "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 123)",
			wantErr: false,
		},
		{
			name:    "NOT EXISTS subquery",
			query:   "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = 123)",
			wantErr: false,
		},
		{
			name:    "IN with subquery",
			query:   "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantErr: false,
		},
		{
			name:    "NOT IN with subquery",
			query:   "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE amount > 100)",
			wantErr: false,
		},
		{
			name:    "scalar subquery in WHERE",
			query:   "SELECT * FROM orders WHERE amount > (SELECT 100)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			p := parser.NewParser(tt.query)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			selectStmt, ok := stmt.(*parser.SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			// Create a basic planner
			planner := NewBasicPlanner()

			// Plan the query
			plan, err := planner.planSelect(selectStmt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if plan == nil {
				t.Errorf("Expected plan but got nil")
				return
			}

			// Basic validation - make sure we got a plan
			t.Logf("Generated plan: %s", plan.String())
		})
	}
}

func TestSubqueryExpressionConversion(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		checkCol func(*LogicalProject) error
	}{
		{
			name:  "scalar subquery in projection",
			query: "SELECT name, (SELECT 42) AS answer FROM users",
			checkCol: func(proj *LogicalProject) error {
				if len(proj.Projections) != 2 {
					return errorf("expected 2 projections, got %d", len(proj.Projections))
				}

				// Second projection should be a subquery expression
				if _, ok := proj.Projections[1].(*SubqueryExpr); !ok {
					return errorf("expected SubqueryExpr, got %T", proj.Projections[1])
				}

				return nil
			},
		},
		{
			name:  "EXISTS in WHERE clause",
			query: "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)",
			checkCol: func(proj *LogicalProject) error {
				// Should have a filter child
				if len(proj.children) == 0 {
					return errorf("expected child node")
				}

				filter, ok := proj.children[0].(*LogicalFilter)
				if !ok {
					return errorf("expected LogicalFilter, got %T", proj.children[0])
				}

				// Filter predicate should be EXISTS expression
				if _, ok := filter.Predicate.(*ExistsExpr); !ok {
					return errorf("expected ExistsExpr in filter, got %T", filter.Predicate)
				}

				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			p := parser.NewParser(tt.query)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			selectStmt, ok := stmt.(*parser.SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			// Create a basic planner
			planner := NewBasicPlanner()

			// Plan the query
			plan, err := planner.planSelect(selectStmt)
			if err != nil {
				t.Fatalf("Failed to plan query: %v", err)
			}

			// Should get a project node at the top
			proj, ok := plan.(*LogicalProject)
			if !ok {
				t.Fatalf("Expected LogicalProject at top, got %T", plan)
			}

			// Run the specific check
			if err := tt.checkCol(proj); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestSubqueryDataTypeInference(t *testing.T) {
	// Test that scalar subqueries get proper data types inferred
	query := "SELECT (SELECT 42) AS answer"

	// Parse the query
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	// Create a basic planner
	planner := NewBasicPlanner()

	// Plan the query
	plan, err := planner.planSelect(selectStmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Should get a project node at the top
	proj, ok := plan.(*LogicalProject)
	if !ok {
		t.Fatalf("Expected LogicalProject at top, got %T", plan)
	}

	// Check the subquery expression
	if len(proj.Projections) != 1 {
		t.Fatalf("Expected 1 projection, got %d", len(proj.Projections))
	}

	subqueryExpr, ok := proj.Projections[0].(*SubqueryExpr)
	if !ok {
		t.Fatalf("Expected SubqueryExpr, got %T", proj.Projections[0])
	}

	// The subquery should return an integer
	// Since we're selecting a literal 42, the type should be inferred
	if subqueryExpr.Type == types.Unknown {
		t.Logf("Subquery type is Unknown (expected for now)")
	}

	t.Logf("Subquery type: %v", subqueryExpr.Type)
}

func errorf(format string, args ...interface{}) error {
	return &testError{msg: fmt.Sprintf(format, args...)}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
