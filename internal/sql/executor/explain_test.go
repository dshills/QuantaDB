package executor

import (
	"strings"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestExplainOperator(t *testing.T) {
	// Create a simple mock operator for testing
	mockOp := &explainMockOperator{
		baseOperator: baseOperator{
			schema: &Schema{
				Columns: []Column{
					{Name: "id", Type: types.Integer},
					{Name: "name", Type: types.Text},
				},
			},
		},
		rows: []*Row{
			{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("Alice")}},
			{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("Bob")}},
		},
	}

	tests := []struct {
		name     string
		analyze  bool
		verbose  bool
		format   string
		checkFor []string // Strings that should appear in output
	}{
		{
			name:     "Basic EXPLAIN",
			analyze:  false,
			verbose:  false,
			format:   "text",
			checkFor: []string{"explainMock"},
		},
		{
			name:     "EXPLAIN ANALYZE",
			analyze:  true,
			verbose:  false,
			format:   "text",
			checkFor: []string{"explainMock", "actual time=", "rows=", "Planning Time:", "Execution Time:"},
		},
		{
			name:     "EXPLAIN ANALYZE VERBOSE",
			analyze:  true,
			verbose:  true,
			format:   "text",
			checkFor: []string{"explainMock", "actual time=", "rows=", "Planning Time:", "Execution Time:"},
		},
		{
			name:     "EXPLAIN JSON format",
			analyze:  false,
			verbose:  false,
			format:   "json",
			checkFor: []string{`"Node Type"`, `"Plan"`},
		},
		{
			name:     "EXPLAIN ANALYZE JSON format",
			analyze:  true,
			verbose:  false,
			format:   "json",
			checkFor: []string{`"Node Type"`, `"Planning Time"`, `"Execution Time"`, `"Actual Rows"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			explainOp := NewExplainOperator(mockOp, tt.analyze, tt.verbose, tt.format)

			ctx := &ExecContext{
				PlanningTime: 5 * time.Millisecond,
			}

			// Open the explain operator
			if err := explainOp.Open(ctx); err != nil {
				t.Fatalf("Failed to open EXPLAIN operator: %v", err)
			}

			// Get the explain output
			row, err := explainOp.Next()
			if err != nil {
				t.Fatalf("Failed to get EXPLAIN output: %v", err)
			}
			if row == nil {
				t.Fatal("Expected EXPLAIN output row, got nil")
			}

			// Check we got one value (the query plan)
			if len(row.Values) != 1 {
				t.Fatalf("Expected 1 value in EXPLAIN output, got %d", len(row.Values))
			}

			// Get the plan output
			output := row.Values[0].Data.(string)

			// Check for expected strings
			for _, check := range tt.checkFor {
				if !strings.Contains(output, check) {
					t.Errorf("Expected output to contain %q, but it didn't. Output:\n%s", check, output)
				}
			}

			// Should return EOF on next call
			row2, err := explainOp.Next()
			if err != nil {
				t.Fatalf("Unexpected error on second Next(): %v", err)
			}
			if row2 != nil {
				t.Error("Expected EOF (nil) on second Next(), got row")
			}

			// Close
			if err := explainOp.Close(); err != nil {
				t.Fatalf("Failed to close EXPLAIN operator: %v", err)
			}
		})
	}
}

// explainMockOperator is a simple operator for testing
type explainMockOperator struct {
	baseOperator
	rows    []*Row
	current int
}

func (m *explainMockOperator) Open(ctx *ExecContext) error {
	m.ctx = ctx
	m.current = 0
	m.initStats(int64(len(m.rows)))
	return nil
}

func (m *explainMockOperator) Next() (*Row, error) {
	if m.current >= len(m.rows) {
		m.finishStats()
		// Report stats to collector
		if m.ctx != nil && m.ctx.StatsCollector != nil && m.stats != nil {
			m.ctx.StatsCollector(m, m.stats)
		}
		return nil, nil
	}
	row := m.rows[m.current]
	m.current++
	m.recordRow()
	return row, nil
}

func (m *explainMockOperator) Close() error {
	m.finishStats()
	// Report stats to collector if not already done
	if m.ctx != nil && m.ctx.StatsCollector != nil && m.stats != nil {
		m.ctx.StatsCollector(m, m.stats)
	}
	return nil
}
