package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjectionPushdown(t *testing.T) {
	// For now, we'll test without a catalog since it's not essential
	// for projection pushdown logic

	tests := []struct {
		name       string
		inputPlan  LogicalPlan
		expectPush bool
		validateFn func(t *testing.T, outputPlan LogicalPlan)
	}{
		{
			name: "simple projection through filter",
			inputPlan: buildPlan(
				// SELECT a, b FROM t1 WHERE c > 10
				NewLogicalProject(
					NewLogicalFilter(
						NewLogicalScan("t1", "t1", &Schema{
							Columns: []Column{
								{Name: "a", DataType: types.Integer},
								{Name: "b", DataType: types.Text},
								{Name: "c", DataType: types.Integer},
								{Name: "d", DataType: types.Text},
							},
						}),
						&BinaryOp{
							Left:     &ColumnRef{ColumnName: "c"},
							Right:    &Literal{Value: types.NewValue(int32(10))},
							Operator: OpGreater,
						},
					),
					[]Expression{
						&ColumnRef{ColumnName: "a"},
						&ColumnRef{ColumnName: "b"},
					},
					[]string{"a", "b"},
					&Schema{
						Columns: []Column{
							{Name: "a", DataType: types.Integer},
							{Name: "b", DataType: types.Text},
						},
					},
				),
			),
			expectPush: true,
			validateFn: func(t *testing.T, plan LogicalPlan) {
				// Should have projection pushed below filter
				proj, ok := plan.(*LogicalProject)
				require.True(t, ok, "root should be project")
				assert.Len(t, proj.Projections, 2)

				filter, ok := proj.Children()[0].(*LogicalFilter)
				require.True(t, ok, "should have filter under project")

				// Should have new projection above scan
				innerProj, ok := filter.Children()[0].(*LogicalProject)
				require.True(t, ok, "should have projection above scan")
				// Should project a, b, c (c needed for filter)
				assert.Len(t, innerProj.Projections, 3)
			},
		},
		{
			name: "projection through join",
			inputPlan: buildPlan(
				// SELECT t1.a, t2.x FROM t1 JOIN t2 ON t1.id = t2.id
				NewLogicalProject(
					NewLogicalJoin(
						NewLogicalScan("t1", "t1", &Schema{
							Columns: []Column{
								{Name: "id", DataType: types.Integer},
								{Name: "a", DataType: types.Text},
								{Name: "b", DataType: types.Text},
								{Name: "c", DataType: types.Text},
							},
						}),
						NewLogicalScan("t2", "t2", &Schema{
							Columns: []Column{
								{Name: "id", DataType: types.Integer},
								{Name: "x", DataType: types.Text},
								{Name: "y", DataType: types.Text},
								{Name: "z", DataType: types.Text},
							},
						}),
						InnerJoin,
						&BinaryOp{
							Left:     &ColumnRef{TableAlias: "t1", ColumnName: "id"},
							Right:    &ColumnRef{TableAlias: "t2", ColumnName: "id"},
							Operator: OpEqual,
						},
						nil, // schema will be set by join
					),
					[]Expression{
						&ColumnRef{TableAlias: "t1", ColumnName: "a"},
						&ColumnRef{TableAlias: "t2", ColumnName: "x"},
					},
					[]string{"a", "x"},
					&Schema{
						Columns: []Column{
							{Name: "a", DataType: types.Text},
							{Name: "x", DataType: types.Text},
						},
					},
				),
			),
			expectPush: true,
			validateFn: func(t *testing.T, plan LogicalPlan) {
				// Should push projections to both sides of join
				proj, ok := plan.(*LogicalProject)
				require.True(t, ok, "root should be project")

				join, ok := proj.Children()[0].(*LogicalJoin)
				require.True(t, ok, "should have join under project")

				// Left side should project id, a
				leftProj, ok := join.Children()[0].(*LogicalProject)
				require.True(t, ok, "left side should have projection")
				assert.Len(t, leftProj.Projections, 2) // id, a

				// Right side should project id, x
				rightProj, ok := join.Children()[1].(*LogicalProject)
				require.True(t, ok, "right side should have projection")
				assert.Len(t, rightProj.Projections, 2) // id, x
			},
		},
		{
			name: "no push when all columns needed",
			inputPlan: buildPlan(
				// SELECT * FROM t1
				NewLogicalProject(
					NewLogicalScan("t1", "t1", &Schema{
						Columns: []Column{
							{Name: "a", DataType: types.Integer},
							{Name: "b", DataType: types.Text},
						},
					}),
					[]Expression{&Star{}},
					nil,
					&Schema{
						Columns: []Column{
							{Name: "a", DataType: types.Integer},
							{Name: "b", DataType: types.Text},
						},
					},
				),
			),
			expectPush: false,
			validateFn: func(t *testing.T, plan LogicalPlan) {
				// Should not add any projections
				proj, ok := plan.(*LogicalProject)
				require.True(t, ok, "root should be project")

				_, ok = proj.Children()[0].(*LogicalScan)
				require.True(t, ok, "should have scan directly under project")
			},
		},
		{
			name: "projection through sort",
			inputPlan: buildPlan(
				// SELECT a FROM t1 ORDER BY b
				NewLogicalProject(
					NewLogicalSort(
						NewLogicalScan("t1", "t1", &Schema{
							Columns: []Column{
								{Name: "a", DataType: types.Integer},
								{Name: "b", DataType: types.Text},
								{Name: "c", DataType: types.Integer},
								{Name: "d", DataType: types.Text},
							},
						}),
						[]OrderByExpr{
							{Expr: &ColumnRef{ColumnName: "b"}, Order: Ascending},
						},
					),
					[]Expression{
						&ColumnRef{ColumnName: "a"},
					},
					[]string{"a"},
					&Schema{
						Columns: []Column{
							{Name: "a", DataType: types.Integer},
						},
					},
				),
			),
			expectPush: true,
			validateFn: func(t *testing.T, plan LogicalPlan) {
				// Should keep b until after sort, then project it away
				proj, ok := plan.(*LogicalProject)
				require.True(t, ok, "root should be project")

				sort, ok := proj.Children()[0].(*LogicalSort)
				require.True(t, ok, "should have sort under project")

				// Should have projection above scan with a, b
				innerProj, ok := sort.Children()[0].(*LogicalProject)
				require.True(t, ok, "should have projection above scan")
				assert.Len(t, innerProj.Projections, 2) // a, b (b needed for sort)
			},
		},
		{
			name: "aggregate with group by",
			inputPlan: buildPlan(
				// SELECT category, SUM(price) FROM products GROUP BY category
				NewLogicalAggregate(
					NewLogicalScan("products", "products", &Schema{
						Columns: []Column{
							{Name: "id", DataType: types.Integer},
							{Name: "name", DataType: types.Text},
							{Name: "category", DataType: types.Text},
							{Name: "price", DataType: types.Decimal(10, 2)},
							{Name: "description", DataType: types.Text},
						},
					}),
					[]Expression{
						&ColumnRef{ColumnName: "category"},
					},
					[]AggregateExpr{
						{
							Function: AggSum,
							Args:     []Expression{&ColumnRef{ColumnName: "price"}},
							Type:     types.Decimal(10, 2),
						},
					},
					&Schema{
						Columns: []Column{
							{Name: "category", DataType: types.Text},
							{Name: "sum", DataType: types.Decimal(10, 2)},
						},
					},
				),
			),
			expectPush: true,
			validateFn: func(t *testing.T, plan LogicalPlan) {
				// Should push projection to only read category and price
				agg, ok := plan.(*LogicalAggregate)
				require.True(t, ok, "root should be aggregate")

				proj, ok := agg.Children()[0].(*LogicalProject)
				require.True(t, ok, "should have projection under aggregate")
				assert.Len(t, proj.Projections, 2) // category, price
			},
		},
	}

	rule := &ProjectionPushdown{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputPlan, modified := rule.Apply(tt.inputPlan)

			assert.Equal(t, tt.expectPush, modified, "modification flag mismatch")

			if tt.validateFn != nil {
				tt.validateFn(t, outputPlan)
			}

			// Verify plan is still valid
			assert.NotNil(t, outputPlan.Schema(), "output plan should have schema")
		})
	}
}

func TestColumnTracking(t *testing.T) {
	tests := []struct {
		name     string
		expr     Expression
		expected []string
	}{
		{
			name:     "simple column ref",
			expr:     &ColumnRef{ColumnName: "a"},
			expected: []string{"a"},
		},
		{
			name: "binary operation",
			expr: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "a"},
				Right:    &ColumnRef{ColumnName: "b"},
				Operator: OpAdd,
			},
			expected: []string{"a", "b"},
		},
		{
			name: "function call",
			expr: &FunctionCall{
				Name: "upper",
				Args: []Expression{
					&ColumnRef{ColumnName: "name"},
				},
			},
			expected: []string{"name"},
		},
		{
			name:     "star expression",
			expr:     &Star{},
			expected: []string{}, // But hasStar should be true
		},
		{
			name: "complex expression",
			expr: &BinaryOp{
				Left: &FunctionCall{
					Name: "length",
					Args: []Expression{
						&ColumnRef{ColumnName: "description"},
					},
				},
				Right: &BinaryOp{
					Left:     &ColumnRef{ColumnName: "price"},
					Right:    &Literal{Value: types.NewValue(1.1)},
					Operator: OpMultiply,
				},
				Operator: OpGreater,
			},
			expected: []string{"description", "price"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols := NewColumnSet()
			extractColumns(tt.expr, cols)

			if _, ok := tt.expr.(*Star); ok {
				assert.True(t, cols.HasStar(), "star expression should set hasStar")
			} else {
				colSlice := cols.ToSlice()
				colNames := make([]string, len(colSlice))
				for i, col := range colSlice {
					colNames[i] = col.ColumnName
				}
				assert.ElementsMatch(t, tt.expected, colNames, "extracted columns mismatch")
			}
		})
	}
}

func TestColumnSetOperations(t *testing.T) {
	// Test basic operations
	cs1 := NewColumnSet()
	cs1.Add(ColumnRef{ColumnName: "a"})
	cs1.Add(ColumnRef{ColumnName: "b"})

	cs2 := NewColumnSet()
	cs2.Add(ColumnRef{ColumnName: "b"})
	cs2.Add(ColumnRef{ColumnName: "c"})

	// Test AddAll
	cs3 := cs1.Clone()
	cs3.AddAll(cs2)
	assert.Equal(t, 3, cs3.Size(), "should have 3 unique columns")
	assert.True(t, cs3.Contains(ColumnRef{ColumnName: "a"}))
	assert.True(t, cs3.Contains(ColumnRef{ColumnName: "b"}))
	assert.True(t, cs3.Contains(ColumnRef{ColumnName: "c"}))

	// Test star propagation
	cs4 := NewColumnSet()
	cs4.SetStar()
	cs5 := NewColumnSet()
	cs5.Add(ColumnRef{ColumnName: "x"})
	cs5.AddAll(cs4)
	assert.True(t, cs5.HasStar(), "star should propagate through AddAll")
}

// Helper functions

func buildPlan(root LogicalPlan) LogicalPlan {
	// This would normally be done by the planner, but for tests we build manually
	return root
}
