package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestPhysicalPlannerIntegration(t *testing.T) {
	catalog := &mockCatalogProvider{
		tables: map[string]*TableStats{
			"public.users": {
				RowCount:   10000,
				BlockCount: 100,
				AvgRowSize: 200,
			},
			"public.orders": {
				RowCount:   50000,
				BlockCount: 500,
				AvgRowSize: 150,
			},
		},
	}
	stats := &mockStatsProvider{}
	physicalPlanner := NewPhysicalPlanner(catalog, stats)

	t.Run("scan optimization", func(t *testing.T) {
		// Create a logical scan manually
		schema := &planner.Schema{
			Columns: []planner.Column{
				{Name: "id", DataType: types.Integer},
				{Name: "name", DataType: types.Text},
			},
		}

		logicalScan := planner.NewLogicalScan("users", "u", schema)

		// Optimize it
		physical, err := physicalPlanner.OptimizePlan(logicalScan)
		if err != nil {
			t.Fatalf("Failed to optimize scan: %v", err)
		}

		// Should be a PhysicalScan
		physicalScan, ok := physical.(*PhysicalScan)
		if !ok {
			t.Fatalf("Expected PhysicalScan, got %T", physical)
		}

		if physicalScan.TableName != "users" {
			t.Errorf("Expected table name 'users', got %s", physicalScan.TableName)
		}

		if physicalScan.Cardinality() != 10000 {
			t.Errorf("Expected cardinality 10000, got %d", physicalScan.Cardinality())
		}

		if physicalScan.Cost() <= 0 {
			t.Errorf("Expected positive cost, got %f", physicalScan.Cost())
		}
	})

	t.Run("filter pushdown", func(t *testing.T) {
		// Create logical scan + filter
		schema := &planner.Schema{
			Columns: []planner.Column{
				{Name: "id", DataType: types.Integer},
			},
		}
		scan := planner.NewLogicalScan("users", "", schema)

		// Create a simple predicate (this is simplified)
		predicate := &planner.Literal{
			Value: types.NewValue(true),
			Type:  types.Boolean,
		}
		filter := planner.NewLogicalFilter(scan, predicate)

		// Optimize it
		physical, err := physicalPlanner.OptimizePlan(filter)
		if err != nil {
			t.Fatalf("Failed to optimize filter: %v", err)
		}

		// Should result in a scan with pushed predicate
		physicalScan, ok := physical.(*PhysicalScan)
		if !ok {
			t.Fatalf("Expected PhysicalScan with pushed filter, got %T", physical)
		}

		if physicalScan.Predicate == nil {
			t.Error("Expected predicate to be pushed down to scan")
		}

		// Cardinality should be reduced due to filter selectivity
		if physicalScan.Cardinality() >= 10000 {
			t.Errorf("Expected reduced cardinality due to filter, got %d", physicalScan.Cardinality())
		}
	})

	t.Run("cost estimation", func(t *testing.T) {
		// Test that costs are reasonable for different operations
		schema := &planner.Schema{
			Columns: []planner.Column{
				{Name: "name", DataType: types.Text},
			},
		}
		scan := planner.NewLogicalScan("users", "", schema)

		// Sort should be more expensive than scan
		orderBy := []planner.OrderByExpr{
			{
				Expr: &planner.ColumnRef{
					ColumnName: "name",
					ColumnType: types.Text,
				},
				Order: planner.Ascending,
			},
		}
		sort := planner.NewLogicalSort(scan, orderBy)

		physicalSort, err := physicalPlanner.OptimizePlan(sort)
		if err != nil {
			t.Fatalf("Failed to optimize sort: %v", err)
		}

		// Should be more expensive than just the scan
		if len(physicalSort.Children()) == 0 {
			t.Fatal("Sort should have children")
		}

		childCost := physicalSort.Children()[0].(PhysicalPlan).Cost()
		sortCost := physicalSort.Cost()
		if sortCost <= childCost {
			t.Errorf("Expected sort cost (%f) > child cost (%f)", sortCost, childCost)
		}
	})
}
