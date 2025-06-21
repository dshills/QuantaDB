package planner

import (
	"fmt"
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// BenchmarkProjectionPushdown measures the impact of projection pushdown
// by comparing the amount of data that would flow through operators
func BenchmarkProjectionPushdown(b *testing.B) {
	// Create a wide table schema (20 columns)
	columns := make([]Column, 20)
	for i := 0; i < 20; i++ {
		columns[i] = Column{
			Name:     fmt.Sprintf("col%d", i),
			DataType: types.Text,
			Nullable: true,
		}
	}
	wideSchema := &Schema{Columns: columns}

	// Create a plan that only needs 2 columns but filters on a third
	// SELECT col0, col1 FROM wide_table WHERE col2 > 100
	scan := NewLogicalScan("wide_table", "wide_table", wideSchema)

	filter := NewLogicalFilter(scan, &BinaryOp{
		Left:     &ColumnRef{ColumnName: "col2"},
		Right:    &Literal{Value: types.NewValue(int32(100))},
		Operator: OpGreater,
	})

	project := NewLogicalProject(
		filter,
		[]Expression{
			&ColumnRef{ColumnName: "col0"},
			&ColumnRef{ColumnName: "col1"},
		},
		[]string{"col0", "col1"},
		&Schema{
			Columns: []Column{
				{Name: "col0", DataType: types.Text},
				{Name: "col1", DataType: types.Text},
			},
		},
	)

	b.Run("WithoutProjectionPushdown", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Simulate data flow without projection pushdown
			// Scan returns all 20 columns
			// Filter processes all 20 columns
			// Project reduces to 2 columns
			dataFlow := calculateDataFlow(project)
			_ = dataFlow
		}
	})

	b.Run("WithProjectionPushdown", func(b *testing.B) {
		rule := &ProjectionPushdown{}
		optimized, _ := rule.Apply(project)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Simulate data flow with projection pushdown
			// Scan returns all 20 columns
			// Projection reduces to 3 columns (col0, col1, col2)
			// Filter processes only 3 columns
			// Final project already has the right columns
			dataFlow := calculateDataFlow(optimized)
			_ = dataFlow
		}
	})
}

// calculateDataFlow simulates the amount of data flowing through operators
func calculateDataFlow(plan Plan) int64 {
	// Simplified calculation: columns * assumed row count at each level
	const rowCount = 10000

	var totalDataFlow int64
	var calculate func(p Plan, rowsIn int64) int64

	calculate = func(p Plan, rowsIn int64) int64 {
		schema := p.Schema()
		if schema == nil {
			return 0
		}

		colCount := len(schema.Columns)
		dataFlow := rowsIn * int64(colCount)
		totalDataFlow += dataFlow

		// Recursively process children
		for _, child := range p.Children() {
			// Filters reduce rows by assumed selectivity
			if _, ok := p.(*LogicalFilter); ok {
				calculate(child, rowsIn/2) // 50% selectivity
			} else {
				calculate(child, rowsIn)
			}
		}

		return dataFlow
	}

	calculate(plan, rowCount)
	return totalDataFlow
}

// TestProjectionPushdownOptimization verifies the optimization reduces data flow
func TestProjectionPushdownOptimization(t *testing.T) {
	// Create the same wide table scenario
	columns := make([]Column, 20)
	for i := 0; i < 20; i++ {
		columns[i] = Column{
			Name:     fmt.Sprintf("col%d", i),
			DataType: types.Text,
			Nullable: true,
		}
	}
	wideSchema := &Schema{Columns: columns}

	scan := NewLogicalScan("wide_table", "wide_table", wideSchema)
	filter := NewLogicalFilter(scan, &BinaryOp{
		Left:     &ColumnRef{ColumnName: "col2"},
		Right:    &Literal{Value: types.NewValue(int32(100))},
		Operator: OpGreater,
	})
	project := NewLogicalProject(
		filter,
		[]Expression{
			&ColumnRef{ColumnName: "col0"},
			&ColumnRef{ColumnName: "col1"},
		},
		[]string{"col0", "col1"},
		&Schema{
			Columns: []Column{
				{Name: "col0", DataType: types.Text},
				{Name: "col1", DataType: types.Text},
			},
		},
	)

	// Apply optimization
	rule := &ProjectionPushdown{}
	optimized, modified := rule.Apply(project)

	if !modified {
		t.Fatal("Expected projection pushdown to be applied")
	}

	// Verify structure: Project -> Filter -> Project -> Scan
	proj1, ok := optimized.(*LogicalProject)
	if !ok {
		t.Fatal("Top level should be project")
	}

	filter2, ok := proj1.Children()[0].(*LogicalFilter)
	if !ok {
		t.Fatal("Second level should be filter")
	}

	proj2, ok := filter2.Children()[0].(*LogicalProject)
	if !ok {
		t.Fatal("Third level should be project (pushed down)")
	}

	// The pushed projection should have 3 columns (col0, col1, col2)
	if len(proj2.Projections) != 3 {
		t.Errorf("Pushed projection should have 3 columns, got %d", len(proj2.Projections))
	}

	// Calculate data flow reduction
	unoptimizedFlow := calculateDataFlow(project)
	optimizedFlow := calculateDataFlow(optimized)

	reduction := float64(unoptimizedFlow-optimizedFlow) / float64(unoptimizedFlow) * 100
	t.Logf("Data flow reduction: %.1f%% (from %d to %d column-rows)",
		reduction, unoptimizedFlow, optimizedFlow)

	// Should see significant reduction
	if reduction < 45 {
		t.Errorf("Expected at least 45%% data flow reduction, got %.1f%%", reduction)
	}
}
