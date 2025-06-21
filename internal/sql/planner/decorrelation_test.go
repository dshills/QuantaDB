package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestSubqueryDecorrelationExists(t *testing.T) {
	tests := []struct {
		name        string
		setupPlan   func() LogicalPlan
		expectJoin  bool
		expectAnti  bool
		description string
	}{
		{
			name: "EXISTS subquery transformation",
			setupPlan: func() LogicalPlan {
				// Create a subquery plan (simple scan)
				subquerySchema := &Schema{
					Columns: []Column{
						{Name: "order_id", DataType: types.Integer, Nullable: false},
					},
				}
				subqueryScan := NewLogicalScan("orders", "", subquerySchema)

				// Create the EXISTS expression
				existsExpr := &ExistsExpr{
					Subquery: &SubqueryExpr{
						Subplan: subqueryScan,
						Type:    types.Boolean,
					},
					Not: false,
				}

				// Create main table scan
				mainSchema := &Schema{
					Columns: []Column{
						{Name: "user_id", DataType: types.Integer, Nullable: false},
						{Name: "name", DataType: types.Text, Nullable: false},
					},
				}
				mainScan := NewLogicalScan("users", "", mainSchema)

				// Create filter with EXISTS
				return NewLogicalFilter(mainScan, existsExpr)
			},
			expectJoin:  true,
			expectAnti:  false,
			description: "Should transform EXISTS to SEMI join",
		},
		{
			name: "NOT EXISTS subquery transformation",
			setupPlan: func() LogicalPlan {
				// Create a subquery plan (simple scan)
				subquerySchema := &Schema{
					Columns: []Column{
						{Name: "order_id", DataType: types.Integer, Nullable: false},
					},
				}
				subqueryScan := NewLogicalScan("orders", "", subquerySchema)

				// Create the NOT EXISTS expression
				existsExpr := &ExistsExpr{
					Subquery: &SubqueryExpr{
						Subplan: subqueryScan,
						Type:    types.Boolean,
					},
					Not: true,
				}

				// Create main table scan
				mainSchema := &Schema{
					Columns: []Column{
						{Name: "user_id", DataType: types.Integer, Nullable: false},
						{Name: "name", DataType: types.Text, Nullable: false},
					},
				}
				mainScan := NewLogicalScan("users", "", mainSchema)

				// Create filter with NOT EXISTS
				return NewLogicalFilter(mainScan, existsExpr)
			},
			expectJoin:  true,
			expectAnti:  true,
			description: "Should transform NOT EXISTS to ANTI join",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup the plan
			plan := tt.setupPlan()

			// Apply decorrelation
			decorrelation := &SubqueryDecorrelation{}
			result, applied := decorrelation.Apply(plan)

			if !applied {
				t.Errorf("Expected decorrelation to be applied")
				return
			}

			if tt.expectJoin {
				// Should result in a join
				join, ok := result.(*LogicalJoin)
				if !ok {
					t.Errorf("Expected LogicalJoin, got %T", result)
					return
				}

				// Check join type
				if tt.expectAnti {
					if join.JoinType != AntiJoin {
						t.Errorf("Expected AntiJoin, got %v", join.JoinType)
					}
				} else {
					if join.JoinType != SemiJoin {
						t.Errorf("Expected SemiJoin, got %v", join.JoinType)
					}
				}

				// Check that we have two children (left and right of join)
				if len(join.Children()) != 2 {
					t.Errorf("Expected 2 children, got %d", len(join.Children()))
				}

				t.Logf("Successfully transformed to %s join", join.JoinType.String())
			}
		})
	}
}

func TestSubqueryDecorrelationInSubquery(t *testing.T) {
	tests := []struct {
		name        string
		setupPlan   func() LogicalPlan
		expectJoin  bool
		expectAnti  bool
		description string
	}{
		{
			name: "IN subquery transformation",
			setupPlan: func() LogicalPlan {
				// Create a subquery plan (simple scan)
				subquerySchema := &Schema{
					Columns: []Column{
						{Name: "user_id", DataType: types.Integer, Nullable: false},
					},
				}
				subqueryScan := NewLogicalScan("orders", "", subquerySchema)

				// Create the IN expression with subquery
				inExpr := &InExpr{
					Expr: &ColumnRef{ColumnName: "user_id", TableAlias: ""},
					Subquery: &SubqueryExpr{
						Subplan: subqueryScan,
						Type:    types.Integer,
					},
					Not: false,
				}

				// Create main table scan
				mainSchema := &Schema{
					Columns: []Column{
						{Name: "user_id", DataType: types.Integer, Nullable: false},
						{Name: "name", DataType: types.Text, Nullable: false},
					},
				}
				mainScan := NewLogicalScan("users", "", mainSchema)

				// Create filter with IN subquery
				return NewLogicalFilter(mainScan, inExpr)
			},
			expectJoin:  true,
			expectAnti:  false,
			description: "Should transform IN (subquery) to SEMI join",
		},
		{
			name: "NOT IN subquery transformation",
			setupPlan: func() LogicalPlan {
				// Create a subquery plan (simple scan)
				subquerySchema := &Schema{
					Columns: []Column{
						{Name: "user_id", DataType: types.Integer, Nullable: false},
					},
				}
				subqueryScan := NewLogicalScan("orders", "", subquerySchema)

				// Create the NOT IN expression with subquery
				inExpr := &InExpr{
					Expr: &ColumnRef{ColumnName: "user_id", TableAlias: ""},
					Subquery: &SubqueryExpr{
						Subplan: subqueryScan,
						Type:    types.Integer,
					},
					Not: true,
				}

				// Create main table scan
				mainSchema := &Schema{
					Columns: []Column{
						{Name: "user_id", DataType: types.Integer, Nullable: false},
						{Name: "name", DataType: types.Text, Nullable: false},
					},
				}
				mainScan := NewLogicalScan("users", "", mainSchema)

				// Create filter with NOT IN subquery
				return NewLogicalFilter(mainScan, inExpr)
			},
			expectJoin:  true,
			expectAnti:  true,
			description: "Should transform NOT IN (subquery) to ANTI join",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup the plan
			plan := tt.setupPlan()

			// Apply decorrelation
			decorrelation := &SubqueryDecorrelation{}
			result, applied := decorrelation.Apply(plan)

			if !applied {
				t.Errorf("Expected decorrelation to be applied")
				return
			}

			if tt.expectJoin {
				// Should result in a join
				join, ok := result.(*LogicalJoin)
				if !ok {
					t.Errorf("Expected LogicalJoin, got %T", result)
					return
				}

				// Check join type
				if tt.expectAnti {
					if join.JoinType != AntiJoin {
						t.Errorf("Expected AntiJoin, got %v", join.JoinType)
					}
				} else {
					if join.JoinType != SemiJoin {
						t.Errorf("Expected SemiJoin, got %v", join.JoinType)
					}
				}

				// Check that we have two children (left and right of join)
				if len(join.Children()) != 2 {
					t.Errorf("Expected 2 children, got %d", len(join.Children()))
				}

				t.Logf("Successfully transformed to %s join", join.JoinType.String())
			}
		})
	}
}

func TestSubqueryDecorrelationComplexPredicates(t *testing.T) {
	// Test AND combinations with multiple subqueries
	subquerySchema := &Schema{
		Columns: []Column{
			{Name: "order_id", DataType: types.Integer, Nullable: false},
		},
	}
	subqueryScan1 := NewLogicalScan("orders", "", subquerySchema)
	subqueryScan2 := NewLogicalScan("payments", "", subquerySchema)

	// Create first EXISTS expression
	exists1 := &ExistsExpr{
		Subquery: &SubqueryExpr{
			Subplan: subqueryScan1,
			Type:    types.Boolean,
		},
		Not: false,
	}

	// Create second EXISTS expression
	exists2 := &ExistsExpr{
		Subquery: &SubqueryExpr{
			Subplan: subqueryScan2,
			Type:    types.Boolean,
		},
		Not: false,
	}

	// Create AND combination
	andExpr := &BinaryOp{
		Left:     exists1,
		Right:    exists2,
		Operator: OpAnd,
		Type:     types.Boolean,
	}

	// Create main table scan
	mainSchema := &Schema{
		Columns: []Column{
			{Name: "user_id", DataType: types.Integer, Nullable: false},
			{Name: "name", DataType: types.Text, Nullable: false},
		},
	}
	mainScan := NewLogicalScan("users", "", mainSchema)

	// Create filter with AND of EXISTS
	plan := NewLogicalFilter(mainScan, andExpr)

	// Apply decorrelation
	decorrelation := &SubqueryDecorrelation{}
	result, applied := decorrelation.Apply(plan)

	if !applied {
		t.Errorf("Expected decorrelation to be applied")
		return
	}

	// The result should be a join (either the final join or a filter over joins)
	// The exact structure depends on how the AND is processed
	t.Logf("Result plan: %s", result.String())

	// Basic validation - should have converted the subqueries
	if result == plan {
		t.Errorf("Expected plan to be transformed")
	}
}

func TestSubqueryDecorrelationNoTransformation(t *testing.T) {
	// Test cases where decorrelation should not apply
	tests := []struct {
		name      string
		setupPlan func() LogicalPlan
	}{
		{
			name: "filter without subqueries",
			setupPlan: func() LogicalPlan {
				mainSchema := &Schema{
					Columns: []Column{
						{Name: "user_id", DataType: types.Integer, Nullable: false},
						{Name: "name", DataType: types.Text, Nullable: false},
					},
				}
				mainScan := NewLogicalScan("users", "", mainSchema)

				// Simple equality predicate
				simpleFilter := &BinaryOp{
					Left:     &ColumnRef{ColumnName: "user_id", TableAlias: ""},
					Right:    &Literal{Value: types.NewValue(123), Type: types.Integer},
					Operator: OpEqual,
					Type:     types.Boolean,
				}

				return NewLogicalFilter(mainScan, simpleFilter)
			},
		},
		{
			name: "IN with value list (not subquery)",
			setupPlan: func() LogicalPlan {
				mainSchema := &Schema{
					Columns: []Column{
						{Name: "user_id", DataType: types.Integer, Nullable: false},
						{Name: "name", DataType: types.Text, Nullable: false},
					},
				}
				mainScan := NewLogicalScan("users", "", mainSchema)

				// IN with value list
				inExpr := &InExpr{
					Expr: &ColumnRef{ColumnName: "user_id", TableAlias: ""},
					Values: []Expression{
						&Literal{Value: types.NewValue(1), Type: types.Integer},
						&Literal{Value: types.NewValue(2), Type: types.Integer},
					},
					Not: false,
				}

				return NewLogicalFilter(mainScan, inExpr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := tt.setupPlan()

			// Apply decorrelation
			decorrelation := &SubqueryDecorrelation{}
			result, applied := decorrelation.Apply(plan)

			if applied {
				t.Errorf("Expected decorrelation NOT to be applied for %s", tt.name)
			}

			if result != plan {
				t.Errorf("Expected plan to remain unchanged")
			}
		})
	}
}
