package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/require"
)

func TestCaseExpressionExecution(t *testing.T) {
	// Create a test schema
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "status", Type: types.Text},
			{Name: "price", Type: types.Decimal(10, 2)},
			{Name: "quantity", Type: types.Integer},
		},
	}

	// Create test rows
	rows := []*Row{
		{Values: []types.Value{
			types.NewValue(int64(1)),
			types.NewValue("active"),
			types.NewValue(float64(150.0)),
			types.NewValue(int64(5)),
		}},
		{Values: []types.Value{
			types.NewValue(int64(2)),
			types.NewValue("inactive"),
			types.NewValue(float64(75.0)),
			types.NewValue(int64(15)),
		}},
		{Values: []types.Value{
			types.NewValue(int64(3)),
			types.NewValue("pending"),
			types.NewValue(float64(25.0)),
			types.NewValue(int64(8)),
		}},
	}

	tests := []struct {
		name     string
		expr     string
		expected []interface{} // Expected values for each row
	}{
		{
			name: "simple case expression",
			expr: "CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END",
			expected: []interface{}{
				int64(1),  // active -> 1
				int64(0),  // inactive -> 0
				int64(-1), // pending -> -1 (else)
			},
		},
		{
			name: "searched case expression",
			expr: "CASE WHEN quantity > 10 THEN 'many' WHEN quantity > 5 THEN 'some' ELSE 'few' END",
			expected: []interface{}{
				"few",  // 5 not > 5
				"many", // 15 > 10
				"some", // 8 > 5
			},
		},
		{
			name: "case with arithmetic in result",
			expr: "CASE WHEN quantity > 10 THEN quantity * 2 ELSE quantity END",
			expected: []interface{}{
				int64(5),  // quantity=5, not > 10, return quantity
				int64(30), // quantity=15 > 10, return quantity * 2
				int64(8),  // quantity=8, not > 10, return quantity
			},
		},
		{
			name: "case without else (returns null)",
			expr: "CASE WHEN status = 'active' THEN 'YES' WHEN status = 'inactive' THEN 'NO' END",
			expected: []interface{}{
				"YES", // active
				"NO",  // inactive
				nil,   // pending -> NULL (no else)
			},
		},
		{
			name: "nested case expression",
			expr: "CASE WHEN quantity < 6 THEN 'low' ELSE CASE WHEN quantity < 10 THEN 'mid' ELSE 'high' END END",
			expected: []interface{}{
				"low",  // 5 < 6
				"high", // 15 >= 10
				"mid",  // 8 < 10
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the expression using a SELECT wrapper to ensure proper parsing
			query := "SELECT " + tt.expr
			p := parser.NewParser(query)
			stmt, err := p.Parse()
			require.NoError(t, err)
			
			selectStmt, ok := stmt.(*parser.SelectStmt)
			require.True(t, ok)
			require.Len(t, selectStmt.Columns, 1)
			
			expr := selectStmt.Columns[0].Expr

			// Build evaluator with catalog columns
			catalogColumns := make([]catalog.Column, len(schema.Columns))
			for i, col := range schema.Columns {
				catalogColumns[i] = catalog.Column{
					Name:     col.Name,
					DataType: col.Type,
				}
			}
			
			ctx := &evalContext{
				columns: catalogColumns,
			}

			// Evaluate for each row
			for i, row := range rows {
				ctx.row = row
				result, err := evaluateExpression(expr, ctx)
				require.NoError(t, err)

				// Check expected result
				if tt.expected[i] == nil {
					require.True(t, result.IsNull(), "Expected NULL for row %d", i)
				} else {
					require.False(t, result.IsNull(), "Expected non-NULL for row %d", i)
					
					// Handle different numeric types
					switch expected := tt.expected[i].(type) {
					case float64:
						// For decimal results, compare as floats
						if f, ok := result.Data.(float64); ok {
							require.InDelta(t, expected, f, 0.01, "Row %d", i)
						} else {
							t.Errorf("Expected float64, got %T for row %d", result.Data, i)
						}
					default:
						require.Equal(t, expected, result.Data, "Row %d", i)
					}
				}
			}
		})
	}
}


// Benchmark CASE expression evaluation
func BenchmarkCaseExpression(b *testing.B) {
	// Create test data
	schema := &Schema{
		Columns: []Column{
			{Name: "value", Type: types.Integer},
		},
	}

	row := &Row{
		Values: []types.Value{types.NewValue(int64(42))},
	}

	// Parse a CASE expression
	expr := "CASE WHEN value > 100 THEN 'high' WHEN value > 50 THEN 'medium' WHEN value > 20 THEN 'low' ELSE 'very low' END"
	p := parser.NewParser(expr)
	parsedExpr, err := p.ParseExpression()
	require.NoError(b, err)

	catalogColumns := make([]catalog.Column, len(schema.Columns))
	for i, col := range schema.Columns {
		catalogColumns[i] = catalog.Column{
			Name:     col.Name,
			DataType: col.Type,
		}
	}
	
	ctx := &evalContext{
		row:     row,
		columns: catalogColumns,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := evaluateExpression(parsedExpr, ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}