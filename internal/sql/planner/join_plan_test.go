package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/parser"
)

func TestJoinPlanning(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectJoin   bool
		expectType   JoinType
		checkColumns []string // Expected columns in result schema
	}{
		{
			name:         "simple inner join",
			sql:          "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			expectJoin:   true,
			expectType:   InnerJoin,
			checkColumns: []string{"*", "*"}, // Two placeholder columns for two tables
		},
		{
			name:         "left join with aliases",
			sql:          "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
			expectJoin:   true,
			expectType:   LeftJoin,
			checkColumns: []string{"*", "*"},
		},
		{
			name:         "cross join",
			sql:          "SELECT * FROM users CROSS JOIN orders",
			expectJoin:   true,
			expectType:   CrossJoin,
			checkColumns: []string{"*", "*"},
		},
		{
			name:         "multiple joins",
			sql:          "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.id = i.order_id",
			expectJoin:   true,
			expectType:   InnerJoin,               // The top-level join
			checkColumns: []string{"*", "*", "*"}, // Three tables
		},
		{
			name:         "simple table (no join)",
			sql:          "SELECT * FROM users",
			expectJoin:   false,
			checkColumns: []string{"*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			selectStmt, ok := stmt.(*parser.SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			// Plan the query
			planner := NewBasicPlanner()
			plan, err := planner.planSelect(selectStmt)
			if err != nil {
				t.Fatalf("Failed to plan query: %v", err)
			}

			// Check if we got a join at the appropriate level
			if tt.expectJoin {
				// For single joins, the top-level plan after WHERE should be a join
				// For multiple joins, we need to traverse the tree
				foundJoin := false
				var joinPlan *LogicalJoin

				// Walk the plan tree to find joins
				walkPlan(plan, func(p LogicalPlan) {
					if j, ok := p.(*LogicalJoin); ok {
						foundJoin = true
						if joinPlan == nil {
							joinPlan = j // Keep the first (top-level) join
						}
					}
				})

				if !foundJoin {
					t.Error("Expected join in plan but found none")
				} else if joinPlan != nil && joinPlan.JoinType != tt.expectType {
					t.Errorf("Expected join type %v, got %v", tt.expectType, joinPlan.JoinType)
				}
			} else {
				// Should not have a join
				walkPlan(plan, func(p LogicalPlan) {
					if _, ok := p.(*LogicalJoin); ok {
						t.Error("Expected no join in plan but found one")
					}
				})
			}

			// Check schema has expected number of columns
			if plan != nil {
				schema := plan.Schema()
				if schema != nil && len(schema.Columns) != len(tt.checkColumns) {
					t.Errorf("Expected %d columns, got %d", len(tt.checkColumns), len(schema.Columns))
				}
			}
		})
	}
}

// walkPlan walks the plan tree and calls fn for each node
func walkPlan(plan LogicalPlan, fn func(LogicalPlan)) {
	fn(plan)
	for _, child := range plan.Children() {
		if logicalChild, ok := child.(LogicalPlan); ok {
			walkPlan(logicalChild, fn)
		}
	}
}

func TestJoinConditionConversion(t *testing.T) {
	// Test that join conditions are properly converted
	sql := "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND o.status = 'active'"

	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt := stmt.(*parser.SelectStmt)
	planner := NewBasicPlanner()
	plan, err := planner.planSelect(selectStmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Find the join in the plan
	var joinPlan *LogicalJoin
	walkPlan(plan, func(p LogicalPlan) {
		if j, ok := p.(*LogicalJoin); ok {
			joinPlan = j
		}
	})

	if joinPlan == nil {
		t.Fatal("Expected join in plan but found none")
	}

	if joinPlan.Condition == nil {
		t.Fatal("Expected join condition but found none")
	}

	// The condition should be a binary expression
	// Just verify it's not nil for now
	condStr := joinPlan.Condition.String()
	if condStr == "" {
		t.Error("Expected non-empty join condition string")
	}
}

func TestQualifiedColumnReferences(t *testing.T) {
	// Test that qualified column references (table.column) are preserved
	sql := "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id"

	p := parser.NewParser(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt := stmt.(*parser.SelectStmt)
	planner := NewBasicPlanner()
	plan, err := planner.planSelect(selectStmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Just verify the plan was created successfully
	if plan == nil {
		t.Fatal("Expected plan but got nil")
	}

	// Verify we have a join somewhere in the plan
	foundJoin := false
	walkPlan(plan, func(p LogicalPlan) {
		if _, ok := p.(*LogicalJoin); ok {
			foundJoin = true
		}
	})

	if !foundJoin {
		t.Error("Expected join in plan for query with JOIN clause")
	}
}
