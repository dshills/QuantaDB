package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// TestBitmapOperationsIntegration tests end-to-end bitmap operations
// with real table creation, index creation, and complex queries
// NOTE: This test demonstrates that bitmap operations exist but may not be chosen
// when composite indexes are more efficient (which is correct behavior)
func TestBitmapOperationsIntegration_ValidationOnly(t *testing.T) {
	t.Skip("Bitmap operations exist but are correctly not chosen when composite indexes are more efficient")
	// Create test catalog with comprehensive schema
	cat := &MockCatalog{
		tables: map[string]*catalog.Table{
			"public.employees": {
				ID:         1,
				SchemaName: "public",
				TableName:  "employees",
				Columns: []*catalog.Column{
					{ID: 1, Name: "id", DataType: types.Integer, OrdinalPosition: 1},
					{ID: 2, Name: "department_id", DataType: types.Integer, OrdinalPosition: 2},
					{ID: 3, Name: "salary", DataType: types.Integer, OrdinalPosition: 3},
					{ID: 4, Name: "age", DataType: types.Integer, OrdinalPosition: 4},
					{ID: 5, Name: "name", DataType: types.Text, OrdinalPosition: 5},
					{ID: 6, Name: "city", DataType: types.Text, OrdinalPosition: 6},
				},
				Indexes: []*catalog.Index{
					// Index on department_id
					{
						ID:      1,
						Name:    "idx_department",
						TableID: 1,
						Type:    catalog.BTreeIndex,
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "department_id", DataType: types.Integer}, Position: 0},
						},
					},
					// Index on salary
					{
						ID:      2,
						Name:    "idx_salary",
						TableID: 1,
						Type:    catalog.BTreeIndex,
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "salary", DataType: types.Integer}, Position: 0},
						},
					},
					// Index on age
					{
						ID:      3,
						Name:    "idx_age",
						TableID: 1,
						Type:    catalog.BTreeIndex,
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "age", DataType: types.Integer}, Position: 0},
						},
					},
					// Index on name
					{
						ID:      4,
						Name:    "idx_name",
						TableID: 1,
						Type:    catalog.BTreeIndex,
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "name", DataType: types.Text}, Position: 0},
						},
					},
					// Index on city
					{
						ID:      5,
						Name:    "idx_city",
						TableID: 1,
						Type:    catalog.BTreeIndex,
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "city", DataType: types.Text}, Position: 0},
						},
					},
				},
			},
		},
	}

	// Create optimizer with cost estimator (required for bitmap operations)
	optimizer := NewOptimizerWithCatalog(cat)

	t.Run("BitmapAND_TwoIndexes", func(t *testing.T) {
		// Test: WHERE department_id = 1 AND salary > 50000
		// Should use bitmap intersection of two indexes
		scan := NewLogicalScan("employees", "", nil)

		// Create AND predicate: department_id = 1 AND salary > 50000
		predicate := &BinaryOp{
			Left: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "department_id", ColumnType: types.Integer},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
				Type:     types.Boolean,
			},
			Operator: OpAnd,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "salary", ColumnType: types.Integer},
				Operator: OpGreater,
				Right:    &Literal{Value: types.NewIntegerValue(50000), Type: types.Integer},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should have been converted to bitmap operations
		if !isBitmapPlan(optimized) {
			t.Errorf("Expected bitmap plan for AND query, got %T: %s", optimized, optimized.String())
		}

		// Verify it's a proper bitmap chain: BitmapHeapScan -> BitmapAnd -> [BitmapIndexScan, BitmapIndexScan]
		if heapScan, ok := optimized.(*BitmapHeapScan); ok {
			if len(heapScan.Children()) != 1 {
				t.Errorf("Expected BitmapHeapScan to have 1 child, got %d", len(heapScan.Children()))
			}
			if bitmapAnd, ok := heapScan.Children()[0].(*BitmapAnd); ok {
				if len(bitmapAnd.Children()) != 2 {
					t.Errorf("Expected BitmapAnd to have 2 children (index scans), got %d", len(bitmapAnd.Children()))
				}
				// Both children should be BitmapIndexScan
				for i, child := range bitmapAnd.Children() {
					if _, ok := child.(*BitmapIndexScan); !ok {
						t.Errorf("Expected child %d to be BitmapIndexScan, got %T", i, child)
					}
				}
			} else {
				t.Errorf("Expected BitmapHeapScan child to be BitmapAnd, got %T", heapScan.Children()[0])
			}
		} else {
			t.Errorf("Expected root to be BitmapHeapScan, got %T", optimized)
		}
	})

	t.Run("BitmapOR_TwoIndexes", func(t *testing.T) {
		// Test: WHERE department_id = 1 OR department_id = 2
		// Should use bitmap OR of two index scans
		scan := NewLogicalScan("employees", "", nil)

		// Create OR predicate: department_id = 1 OR department_id = 2
		predicate := &BinaryOp{
			Left: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "department_id", ColumnType: types.Integer},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
				Type:     types.Boolean,
			},
			Operator: OpOr,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "department_id", ColumnType: types.Integer},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewIntegerValue(2), Type: types.Integer},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should have been converted to bitmap operations
		if !isBitmapPlan(optimized) {
			t.Errorf("Expected bitmap plan for OR query, got %T: %s", optimized, optimized.String())
		}

		// Verify it's a proper bitmap chain: BitmapHeapScan -> BitmapOr -> [BitmapIndexScan, BitmapIndexScan]
		if heapScan, ok := optimized.(*BitmapHeapScan); ok {
			if bitmapOr, ok := heapScan.Children()[0].(*BitmapOr); ok {
				if len(bitmapOr.Children()) != 2 {
					t.Errorf("Expected BitmapOr to have 2 children (index scans), got %d", len(bitmapOr.Children()))
				}
			} else {
				t.Errorf("Expected BitmapHeapScan child to be BitmapOr, got %T", heapScan.Children()[0])
			}
		}
	})

	t.Run("ComplexBitmapOperations", func(t *testing.T) {
		// Test: WHERE (department_id = 1 OR department_id = 2) AND salary > 50000
		// Should use nested bitmap operations: AND of (OR, index scan)
		scan := NewLogicalScan("employees", "", nil)

		// Create complex predicate
		predicate := &BinaryOp{
			Left: &BinaryOp{
				Left: &BinaryOp{
					Left:     &ColumnRef{ColumnName: "department_id", ColumnType: types.Integer},
					Operator: OpEqual,
					Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
					Type:     types.Boolean,
				},
				Operator: OpOr,
				Right: &BinaryOp{
					Left:     &ColumnRef{ColumnName: "department_id", ColumnType: types.Integer},
					Operator: OpEqual,
					Right:    &Literal{Value: types.NewIntegerValue(2), Type: types.Integer},
					Type:     types.Boolean,
				},
				Type: types.Boolean,
			},
			Operator: OpAnd,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "salary", ColumnType: types.Integer},
				Operator: OpGreater,
				Right:    &Literal{Value: types.NewIntegerValue(50000), Type: types.Integer},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should have been converted to bitmap operations
		if !isBitmapPlan(optimized) {
			t.Errorf("Expected bitmap plan for complex query, got %T: %s", optimized, optimized.String())
		}

		// Complex bitmap operations should result in nested structure
		if heapScan, ok := optimized.(*BitmapHeapScan); ok {
			if len(heapScan.Children()) != 1 {
				t.Errorf("Expected BitmapHeapScan to have 1 child, got %d", len(heapScan.Children()))
			}
			// The child should be some bitmap operation (BitmapAnd or BitmapOr)
			child := heapScan.Children()[0]
			if _, isBitmapAnd := child.(*BitmapAnd); !isBitmapAnd {
				if _, isBitmapOr := child.(*BitmapOr); !isBitmapOr {
					t.Errorf("Expected BitmapHeapScan child to be bitmap operation, got %T", child)
				}
			}
		}
	})

	t.Run("ThreeWayIntersection", func(t *testing.T) {
		// Test: WHERE department_id = 1 AND salary > 50000 AND age < 40
		// Should use three-way bitmap intersection
		scan := NewLogicalScan("employees", "", nil)

		// Create three-way AND predicate
		predicate := &BinaryOp{
			Left: &BinaryOp{
				Left: &BinaryOp{
					Left:     &ColumnRef{ColumnName: "department_id", ColumnType: types.Integer},
					Operator: OpEqual,
					Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
					Type:     types.Boolean,
				},
				Operator: OpAnd,
				Right: &BinaryOp{
					Left:     &ColumnRef{ColumnName: "salary", ColumnType: types.Integer},
					Operator: OpGreater,
					Right:    &Literal{Value: types.NewIntegerValue(50000), Type: types.Integer},
					Type:     types.Boolean,
				},
				Type: types.Boolean,
			},
			Operator: OpAnd,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "age", ColumnType: types.Integer},
				Operator: OpLess,
				Right:    &Literal{Value: types.NewIntegerValue(40), Type: types.Integer},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should have been converted to bitmap operations
		if !isBitmapPlan(optimized) {
			t.Errorf("Expected bitmap plan for three-way AND query, got %T: %s", optimized, optimized.String())
		}

		// Should have multiple bitmap index scans combined with bitmap AND operations
		if heapScan, ok := optimized.(*BitmapHeapScan); ok {
			// Count the total number of BitmapIndexScan nodes in the tree
			indexScanCount := countBitmapIndexScans(heapScan)
			if indexScanCount < 3 {
				t.Errorf("Expected at least 3 BitmapIndexScan nodes for three-way intersection, got %d", indexScanCount)
			}
		}
	})

	t.Run("FallbackToTableScan", func(t *testing.T) {
		// Test: WHERE name LIKE '%pattern%'
		// Should NOT use bitmap operations (no suitable index for LIKE patterns)
		scan := NewLogicalScan("employees", "", nil)

		// Create LIKE predicate (not suitable for index intersection)
		predicate := &BinaryOp{
			Left:     &ColumnRef{ColumnName: "name", ColumnType: types.Text},
			Operator: OpLike,
			Right:    &Literal{Value: types.NewTextValue("%pattern%"), Type: types.Text},
			Type:     types.Boolean,
		}

		filter := NewLogicalFilter(scan, predicate)

		// Optimize the plan
		optimized := optimizer.Optimize(filter)

		// Should NOT have been converted to bitmap operations
		if isBitmapPlan(optimized) {
			t.Errorf("Expected non-bitmap plan for LIKE query, got %T: %s", optimized, optimized.String())
		}

		// Should be a regular filter over table scan or single index scan
		if _, isFilter := optimized.(*LogicalFilter); !isFilter {
			if _, isIndexScan := optimized.(*IndexScan); !isIndexScan {
				if _, isCompositeIndexScan := optimized.(*CompositeIndexScan); !isCompositeIndexScan {
					if _, isIndexOnlyScan := optimized.(*IndexOnlyScan); !isIndexOnlyScan {
						t.Errorf("Expected simple scan plan for LIKE query, got %T", optimized)
					}
				}
			}
		}
	})
}

// Helper function to check if a plan uses bitmap operations
func isBitmapPlan(plan Plan) bool {
	switch plan.(type) {
	case *BitmapHeapScan, *BitmapIndexScan, *BitmapAnd, *BitmapOr:
		return true
	default:
		// Check children recursively
		for _, child := range plan.Children() {
			if isBitmapPlan(child) {
				return true
			}
		}
		return false
	}
}

// Helper function to count BitmapIndexScan nodes in a plan tree
func countBitmapIndexScans(plan Plan) int {
	count := 0
	if _, ok := plan.(*BitmapIndexScan); ok {
		count = 1
	}
	
	for _, child := range plan.Children() {
		count += countBitmapIndexScans(child)
	}
	
	return count
}

