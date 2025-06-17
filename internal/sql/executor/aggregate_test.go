package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestAggregateOperator(t *testing.T) {
	// Create test data
	rows := []*Row{
		{Values: []types.Value{types.NewValue("Sales"), types.NewValue(int64(100))}},
		{Values: []types.Value{types.NewValue("Sales"), types.NewValue(int64(200))}},
		{Values: []types.Value{types.NewValue("Engineering"), types.NewValue(int64(300))}},
		{Values: []types.Value{types.NewValue("Engineering"), types.NewValue(int64(400))}},
		{Values: []types.Value{types.NewValue("Engineering"), types.NewValue(int64(500))}},
		{Values: []types.Value{types.NewValue("HR"), types.NewValue(int64(150))}},
		{Values: []types.Value{types.NewValue("HR"), types.NewNullValue()}}, // NULL value
	}

	schema := &Schema{
		Columns: []Column{
			{Name: "department", Type: types.Text},
			{Name: "amount", Type: types.Integer},
		},
	}

	t.Run("GROUP BY with COUNT", func(t *testing.T) {
		mock := newMockOperator(rows, schema)

		// GROUP BY department
		groupBy := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 0, resolved: true}, // department
		}

		// COUNT(*)
		aggregates := []AggregateExpr{
			{
				Function: &CountFunction{CountStar: true},
				Expr:     &literalEvaluator{value: types.NewValue(int64(1))},
				Alias:    "count",
			},
		}

		aggOp := NewAggregateOperator(mock, groupBy, aggregates)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := aggOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open aggregate operator: %v", err)
		}
		defer aggOp.Close()

		// Expected results (order may vary)
		expected := map[string]int64{
			"Sales":       2,
			"Engineering": 3,
			"HR":          2,
		}

		results := make(map[string]int64)
		for {
			row, err := aggOp.Next()
			if err != nil {
				t.Fatalf("Error getting row: %v", err)
			}
			if row == nil {
				break
			}

			dept := row.Values[0].Data.(string)
			count := row.Values[1].Data.(int64)
			results[dept] = count
		}

		if len(results) != len(expected) {
			t.Errorf("Expected %d groups, got %d", len(expected), len(results))
		}

		for dept, count := range expected {
			if results[dept] != count {
				t.Errorf("Department %s: expected count %d, got %d", dept, count, results[dept])
			}
		}
	})

	t.Run("GROUP BY with SUM", func(t *testing.T) {
		mock := newMockOperator(rows, schema)

		// GROUP BY department
		groupBy := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 0, resolved: true}, // department
		}

		// SUM(amount)
		aggregates := []AggregateExpr{
			{
				Function: &SumFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 1, resolved: true}, // amount
				Alias:    "total",
			},
		}

		aggOp := NewAggregateOperator(mock, groupBy, aggregates)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := aggOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open aggregate operator: %v", err)
		}
		defer aggOp.Close()

		// Expected results
		expected := map[string]float64{
			"Sales":       300.0,  // 100 + 200
			"Engineering": 1200.0, // 300 + 400 + 500
			"HR":          150.0,  // 150 (NULL ignored)
		}

		results := make(map[string]float64)
		for {
			row, err := aggOp.Next()
			if err != nil {
				t.Fatalf("Error getting row: %v", err)
			}
			if row == nil {
				break
			}

			dept := row.Values[0].Data.(string)
			total := row.Values[1].Data.(float64)
			results[dept] = total
		}

		for dept, total := range expected {
			if results[dept] != total {
				t.Errorf("Department %s: expected sum %.2f, got %.2f", dept, total, results[dept])
			}
		}
	})

	t.Run("GROUP BY with AVG", func(t *testing.T) {
		mock := newMockOperator(rows, schema)

		// GROUP BY department
		groupBy := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 0, resolved: true}, // department
		}

		// AVG(amount)
		aggregates := []AggregateExpr{
			{
				Function: &AvgFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 1, resolved: true}, // amount
				Alias:    "average",
			},
		}

		aggOp := NewAggregateOperator(mock, groupBy, aggregates)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := aggOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open aggregate operator: %v", err)
		}
		defer aggOp.Close()

		// Expected results
		expected := map[string]float64{
			"Sales":       150.0, // (100 + 200) / 2
			"Engineering": 400.0, // (300 + 400 + 500) / 3
			"HR":          150.0, // 150 / 1 (NULL ignored)
		}

		results := make(map[string]float64)
		for {
			row, err := aggOp.Next()
			if err != nil {
				t.Fatalf("Error getting row: %v", err)
			}
			if row == nil {
				break
			}

			dept := row.Values[0].Data.(string)
			avg := row.Values[1].Data.(float64)
			results[dept] = avg
		}

		for dept, avg := range expected {
			if results[dept] != avg {
				t.Errorf("Department %s: expected avg %.2f, got %.2f", dept, avg, results[dept])
			}
		}
	})

	t.Run("Multiple aggregates", func(t *testing.T) {
		mock := newMockOperator(rows, schema)

		// GROUP BY department
		groupBy := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 0, resolved: true}, // department
		}

		// COUNT(*), SUM(amount), AVG(amount)
		aggregates := []AggregateExpr{
			{
				Function: &CountFunction{CountStar: true},
				Expr:     &literalEvaluator{value: types.NewValue(int64(1))},
				Alias:    "count",
			},
			{
				Function: &SumFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 1, resolved: true},
				Alias:    "total",
			},
			{
				Function: &AvgFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 1, resolved: true},
				Alias:    "average",
			},
		}

		aggOp := NewAggregateOperator(mock, groupBy, aggregates)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := aggOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open aggregate operator: %v", err)
		}
		defer aggOp.Close()

		rowCount := 0
		for {
			row, err := aggOp.Next()
			if err != nil {
				t.Fatalf("Error getting row: %v", err)
			}
			if row == nil {
				break
			}

			// Verify we have 4 columns: department, count, sum, avg
			if len(row.Values) != 4 {
				t.Errorf("Expected 4 columns, got %d", len(row.Values))
			}
			rowCount++
		}

		if rowCount != 3 {
			t.Errorf("Expected 3 groups, got %d", rowCount)
		}
	})

	t.Run("No GROUP BY (global aggregates)", func(t *testing.T) {
		mock := newMockOperator(rows, schema)

		// No GROUP BY
		groupBy := []ExprEvaluator{}

		// COUNT(*), SUM(amount)
		aggregates := []AggregateExpr{
			{
				Function: &CountFunction{CountStar: true},
				Expr:     &literalEvaluator{value: types.NewValue(int64(1))},
				Alias:    "total_count",
			},
			{
				Function: &SumFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 1, resolved: true},
				Alias:    "grand_total",
			},
		}

		aggOp := NewAggregateOperator(mock, groupBy, aggregates)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := aggOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open aggregate operator: %v", err)
		}
		defer aggOp.Close()

		// Should return exactly one row
		row, err := aggOp.Next()
		if err != nil {
			t.Fatalf("Error getting row: %v", err)
		}
		if row == nil {
			t.Fatal("Expected one row, got nil")
		}

		count := row.Values[0].Data.(int64)
		total := row.Values[1].Data.(float64)

		if count != 7 {
			t.Errorf("Expected count 7, got %d", count)
		}

		if total != 1650.0 {
			t.Errorf("Expected total 1650, got %.2f", total)
		}

		// Check EOF
		row, err = aggOp.Next()
		if err != nil {
			t.Fatalf("Error checking EOF: %v", err)
		}
		if row != nil {
			t.Error("Expected EOF after one row")
		}
	})
}

func TestMinMaxAggregate(t *testing.T) {
	// Test MIN/MAX with different data types
	t.Run("MIN/MAX with integers", func(t *testing.T) {
		rows := []*Row{
			{Values: []types.Value{types.NewValue(int64(10))}},
			{Values: []types.Value{types.NewValue(int64(5))}},
			{Values: []types.Value{types.NewValue(int64(15))}},
			{Values: []types.Value{types.NewNullValue()}}, // NULL should be ignored
			{Values: []types.Value{types.NewValue(int64(3))}},
		}

		schema := &Schema{
			Columns: []Column{{Name: "value", Type: types.Integer}},
		}

		mock := newMockOperator(rows, schema)

		aggregates := []AggregateExpr{
			{
				Function: &MinFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 0, resolved: true},
				Alias:    "min_val",
			},
			{
				Function: &MaxFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 0, resolved: true},
				Alias:    "max_val",
			},
		}

		aggOp := NewAggregateOperator(mock, []ExprEvaluator{}, aggregates)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := aggOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open aggregate operator: %v", err)
		}
		defer aggOp.Close()

		row, err := aggOp.Next()
		if err != nil {
			t.Fatalf("Error getting row: %v", err)
		}
		if row == nil {
			t.Fatal("Expected one row, got nil")
		}

		minVal := row.Values[0].Data.(int64)
		maxVal := row.Values[1].Data.(int64)

		if minVal != 3 {
			t.Errorf("Expected min 3, got %d", minVal)
		}

		if maxVal != 15 {
			t.Errorf("Expected max 15, got %d", maxVal)
		}
	})

	t.Run("MIN/MAX with strings", func(t *testing.T) {
		rows := []*Row{
			{Values: []types.Value{types.NewValue("banana")}},
			{Values: []types.Value{types.NewValue("apple")}},
			{Values: []types.Value{types.NewValue("cherry")}},
			{Values: []types.Value{types.NewValue("date")}},
		}

		schema := &Schema{
			Columns: []Column{{Name: "fruit", Type: types.Text}},
		}

		mock := newMockOperator(rows, schema)

		aggregates := []AggregateExpr{
			{
				Function: &MinFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 0, resolved: true},
				Alias:    "first",
			},
			{
				Function: &MaxFunction{},
				Expr:     &columnRefEvaluator{columnIdx: 0, resolved: true},
				Alias:    "last",
			},
		}

		aggOp := NewAggregateOperator(mock, []ExprEvaluator{}, aggregates)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := aggOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open aggregate operator: %v", err)
		}
		defer aggOp.Close()

		row, err := aggOp.Next()
		if err != nil {
			t.Fatalf("Error getting row: %v", err)
		}

		minVal := row.Values[0].Data.(string)
		maxVal := row.Values[1].Data.(string)

		if minVal != "apple" {
			t.Errorf("Expected min 'apple', got '%s'", minVal)
		}

		if maxVal != "date" {
			t.Errorf("Expected max 'date', got '%s'", maxVal)
		}
	})
}
