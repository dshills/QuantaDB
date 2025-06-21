package planner

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestCompositeIndexMatcher_Basic(t *testing.T) {
	matcher := NewCompositeIndexMatcher()

	// Create test table with composite index
	table := &catalog.Table{
		ID:        1,
		TableName: "users",
		Columns: []*catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer},
			{ID: 2, Name: "company_id", DataType: types.Integer},
			{ID: 3, Name: "department", DataType: types.Text},
			{ID: 4, Name: "name", DataType: types.Text},
		},
		Indexes: []*catalog.Index{
			{
				ID:      1,
				Name:    "idx_company_dept_name",
				TableID: 1,
				Columns: []catalog.IndexColumn{
					{Column: &catalog.Column{Name: "company_id", DataType: types.Integer}, Position: 0},
					{Column: &catalog.Column{Name: "department", DataType: types.Text}, Position: 1},
					{Column: &catalog.Column{Name: "name", DataType: types.Text}, Position: 2},
				},
			},
		},
	}

	// Test simple equality predicate on first column
	predicate1 := &BinaryOp{
		Left:     &ColumnRef{ColumnName: "company_id", ColumnType: types.Integer},
		Operator: OpEqual,
		Right:    &Literal{Value: types.NewIntegerValue(100), Type: types.Integer},
		Type:     types.Boolean,
	}

	match1 := matcher.FindBestIndexMatch(table, predicate1)
	if match1 == nil {
		t.Fatal("Expected to find index match for single column predicate")
	}
	if match1.MatchingColumns != 1 {
		t.Errorf("Expected 1 matching column, got %d", match1.MatchingColumns)
	}

	// Test two-column AND predicate
	predicate2 := &BinaryOp{
		Left: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "company_id", ColumnType: types.Integer},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewIntegerValue(100), Type: types.Integer},
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

	match2 := matcher.FindBestIndexMatch(table, predicate2)
	if match2 == nil {
		t.Fatal("Expected to find index match for two-column predicate")
	}
	if match2.MatchingColumns != 2 {
		t.Errorf("Expected 2 matching columns, got %d", match2.MatchingColumns)
	}

	// Test three-column AND predicate (exact match)
	predicate3 := &BinaryOp{
		Left: &BinaryOp{
			Left: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "company_id", ColumnType: types.Integer},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewIntegerValue(100), Type: types.Integer},
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

	match3 := matcher.FindBestIndexMatch(table, predicate3)
	if match3 == nil {
		t.Fatal("Expected to find index match for three-column predicate")
	}
	if match3.MatchingColumns != 3 {
		t.Errorf("Expected 3 matching columns, got %d", match3.MatchingColumns)
	}
	if !match3.IsExactMatch() {
		t.Error("Expected exact match for all index columns")
	}
}

func TestCompositeIndexMatcher_LeftmostPrefixRule(t *testing.T) {
	matcher := NewCompositeIndexMatcher()

	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "products",
		Columns: []*catalog.Column{
			{ID: 1, Name: "category_id", DataType: types.Integer},
			{ID: 2, Name: "subcategory", DataType: types.Text},
			{ID: 3, Name: "price", DataType: types.Integer},
		},
		Indexes: []*catalog.Index{
			{
				ID:      1,
				Name:    "idx_category_sub_price",
				TableID: 1,
				Columns: []catalog.IndexColumn{
					{Column: &catalog.Column{Name: "category_id", DataType: types.Integer}, Position: 0},
					{Column: &catalog.Column{Name: "subcategory", DataType: types.Text}, Position: 1},
					{Column: &catalog.Column{Name: "price", DataType: types.Integer}, Position: 2},
				},
			},
		},
	}

	// Test skipping first column - should not match
	predicateSkipFirst := &BinaryOp{
		Left: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "subcategory", ColumnType: types.Text},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewTextValue("Books"), Type: types.Text},
			Type:     types.Boolean,
		},
		Operator: OpAnd,
		Right: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "price", ColumnType: types.Integer},
			Operator: OpLess,
			Right:    &Literal{Value: types.NewIntegerValue(50), Type: types.Integer},
			Type:     types.Boolean,
		},
		Type: types.Boolean,
	}

	match := matcher.FindBestIndexMatch(table, predicateSkipFirst)
	if match != nil && match.MatchingColumns > 0 {
		t.Error("Should not match index when skipping first column")
	}

	// Test gap in middle - should stop at gap
	predicateGap := &BinaryOp{
		Left: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "category_id", ColumnType: types.Integer},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
			Type:     types.Boolean,
		},
		Operator: OpAnd,
		Right: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "price", ColumnType: types.Integer}, // Skip subcategory
			Operator: OpLess,
			Right:    &Literal{Value: types.NewIntegerValue(100), Type: types.Integer},
			Type:     types.Boolean,
		},
		Type: types.Boolean,
	}

	matchGap := matcher.FindBestIndexMatch(table, predicateGap)
	if matchGap == nil {
		t.Fatal("Expected to find partial match for first column")
	}
	if matchGap.MatchingColumns != 1 {
		t.Errorf("Expected 1 matching column due to gap, got %d", matchGap.MatchingColumns)
	}
}

func TestCompositeIndexMatcher_RangePredicates(t *testing.T) {
	matcher := NewCompositeIndexMatcher()

	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "orders",
		Columns: []*catalog.Column{
			{ID: 1, Name: "customer_id", DataType: types.Integer},
			{ID: 2, Name: "order_date", DataType: types.Timestamp},
			{ID: 3, Name: "amount", DataType: types.Integer},
		},
		Indexes: []*catalog.Index{
			{
				ID:      1,
				Name:    "idx_customer_date_amount",
				TableID: 1,
				Columns: []catalog.IndexColumn{
					{Column: &catalog.Column{Name: "customer_id", DataType: types.Integer}, Position: 0},
					{Column: &catalog.Column{Name: "order_date", DataType: types.Timestamp}, Position: 1},
					{Column: &catalog.Column{Name: "amount", DataType: types.Integer}, Position: 2},
				},
			},
		},
	}

	// Test range predicate on second column
	predicateRange := &BinaryOp{
		Left: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "customer_id", ColumnType: types.Integer},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewIntegerValue(123), Type: types.Integer},
			Type:     types.Boolean,
		},
		Operator: OpAnd,
		Right: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "order_date", ColumnType: types.Timestamp},
			Operator: OpGreaterEqual,
			Right:    &Literal{Value: types.NewTimestampValue(time.Now()), Type: types.Timestamp},
			Type:     types.Boolean,
		},
		Type: types.Boolean,
	}

	match := matcher.FindBestIndexMatch(table, predicateRange)
	if match == nil {
		t.Fatal("Expected to find index match for range predicate")
	}
	if match.MatchingColumns != 2 {
		t.Errorf("Expected 2 matching columns for range predicate, got %d", match.MatchingColumns)
	}
	if !match.HasRangePredicates {
		t.Error("Expected HasRangePredicates to be true")
	}

	// Test range predicate stopping subsequent column usage
	predicateRangeStop := &BinaryOp{
		Left: &BinaryOp{
			Left: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "customer_id", ColumnType: types.Integer},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewIntegerValue(123), Type: types.Integer},
				Type:     types.Boolean,
			},
			Operator: OpAnd,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "order_date", ColumnType: types.Timestamp},
				Operator: OpGreaterEqual,
				Right:    &Literal{Value: types.NewTimestampValue(time.Now()), Type: types.Timestamp},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		},
		Operator: OpAnd,
		Right: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "amount", ColumnType: types.Integer},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewIntegerValue(1000), Type: types.Integer},
			Type:     types.Boolean,
		},
		Type: types.Boolean,
	}

	matchStop := matcher.FindBestIndexMatch(table, predicateRangeStop)
	if matchStop == nil {
		t.Fatal("Expected to find index match")
	}
	// Should stop at range predicate, not use the third column
	if matchStop.MatchingColumns != 2 {
		t.Errorf("Expected 2 matching columns (should stop at range), got %d", matchStop.MatchingColumns)
	}
}

func TestCompositeIndexMatcher_IndexSelection(t *testing.T) {
	matcher := NewCompositeIndexMatcher()

	// Create test table with multiple indexes
	table := &catalog.Table{
		ID:        1,
		TableName: "employees",
		Columns: []*catalog.Column{
			{ID: 1, Name: "company_id", DataType: types.Integer},
			{ID: 2, Name: "department", DataType: types.Text},
			{ID: 3, Name: "name", DataType: types.Text},
			{ID: 4, Name: "salary", DataType: types.Integer},
		},
		Indexes: []*catalog.Index{
			{
				ID:      1,
				Name:    "idx_company",
				TableID: 1,
				Columns: []catalog.IndexColumn{
					{Column: &catalog.Column{Name: "company_id", DataType: types.Integer}, Position: 0},
				},
			},
			{
				ID:      2,
				Name:    "idx_company_dept",
				TableID: 1,
				Columns: []catalog.IndexColumn{
					{Column: &catalog.Column{Name: "company_id", DataType: types.Integer}, Position: 0},
					{Column: &catalog.Column{Name: "department", DataType: types.Text}, Position: 1},
				},
			},
			{
				ID:      3,
				Name:    "idx_company_dept_name",
				TableID: 1,
				Columns: []catalog.IndexColumn{
					{Column: &catalog.Column{Name: "company_id", DataType: types.Integer}, Position: 0},
					{Column: &catalog.Column{Name: "department", DataType: types.Text}, Position: 1},
					{Column: &catalog.Column{Name: "name", DataType: types.Text}, Position: 2},
				},
			},
		},
	}

	// Test predicate that matches all three indexes but best matches the composite one
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

	match := matcher.FindBestIndexMatch(table, predicate)
	if match == nil {
		t.Fatal("Expected to find best index match")
	}

	// Should choose the 2-column index as it matches both predicates exactly
	if match.Index.Name != "idx_company_dept" {
		t.Errorf("Expected to choose idx_company_dept, got %s", match.Index.Name)
	}
	if match.MatchingColumns != 2 {
		t.Errorf("Expected 2 matching columns, got %d", match.MatchingColumns)
	}
}

func TestCompositeIndexMatcher_ExtractColumnPredicates(t *testing.T) {
	matcher := NewCompositeIndexMatcher()

	// Test complex nested AND expression
	predicate := &BinaryOp{
		Left: &BinaryOp{
			Left: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "a", ColumnType: types.Integer},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
				Type:     types.Boolean,
			},
			Operator: OpAnd,
			Right: &BinaryOp{
				Left:     &ColumnRef{ColumnName: "b", ColumnType: types.Text},
				Operator: OpEqual,
				Right:    &Literal{Value: types.NewTextValue("test"), Type: types.Text},
				Type:     types.Boolean,
			},
			Type: types.Boolean,
		},
		Operator: OpAnd,
		Right: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "c", ColumnType: types.Integer},
			Operator: OpLess,
			Right:    &Literal{Value: types.NewIntegerValue(100), Type: types.Integer},
			Type:     types.Boolean,
		},
		Type: types.Boolean,
	}

	predicates := matcher.extractColumnPredicates(predicate)

	// Should extract all three column predicates
	if len(predicates) != 3 {
		t.Errorf("Expected 3 column predicates, got %d", len(predicates))
	}

	if pred, exists := predicates["a"]; !exists || pred.Operator != OpEqual {
		t.Error("Expected column 'a' with OpEqual")
	}

	if pred, exists := predicates["b"]; !exists || pred.Operator != OpEqual {
		t.Error("Expected column 'b' with OpEqual")
	}

	if pred, exists := predicates["c"]; !exists || pred.Operator != OpLess || !pred.IsRange {
		t.Error("Expected column 'c' with OpLess and IsRange=true")
	}
}

func TestCompositeIndexMatcher_UnsupportedPredicates(t *testing.T) {
	matcher := NewCompositeIndexMatcher()

	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test",
		Columns: []*catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer},
			{ID: 2, Name: "name", DataType: types.Text},
		},
		Indexes: []*catalog.Index{
			{
				ID:      1,
				Name:    "idx_id_name",
				TableID: 1,
				Columns: []catalog.IndexColumn{
					{Column: &catalog.Column{Name: "id", DataType: types.Integer}, Position: 0},
					{Column: &catalog.Column{Name: "name", DataType: types.Text}, Position: 1},
				},
			},
		},
	}

	// Test OR predicate (not supported)
	orPredicate := &BinaryOp{
		Left: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "id", ColumnType: types.Integer},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
			Type:     types.Boolean,
		},
		Operator: OpOr,
		Right: &BinaryOp{
			Left:     &ColumnRef{ColumnName: "name", ColumnType: types.Text},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewTextValue("test"), Type: types.Text},
			Type:     types.Boolean,
		},
		Type: types.Boolean,
	}

	match := matcher.FindBestIndexMatch(table, orPredicate)
	if match != nil && match.MatchingColumns > 0 {
		t.Error("Should not match index for OR predicate")
	}

	// Test NOT EQUAL predicate (not efficiently supported)
	notEqualPredicate := &BinaryOp{
		Left:     &ColumnRef{ColumnName: "id", ColumnType: types.Integer},
		Operator: OpNotEqual,
		Right:    &Literal{Value: types.NewIntegerValue(1), Type: types.Integer},
		Type:     types.Boolean,
	}

	matchNotEqual := matcher.FindBestIndexMatch(table, notEqualPredicate)
	if matchNotEqual != nil && matchNotEqual.MatchingColumns > 0 {
		t.Error("Should not match index for NOT EQUAL predicate")
	}
}
