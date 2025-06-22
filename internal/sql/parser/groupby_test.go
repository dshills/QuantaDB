package parser

import (
	"testing"
)

func TestParseGroupBy(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:  "simple GROUP BY",
			input: "SELECT city, COUNT(*) FROM orders GROUP BY city",
		},
		{
			name:  "GROUP BY with HAVING",
			input: "SELECT city, SUM(amount) FROM orders GROUP BY city HAVING SUM(amount) > 100",
		},
		{
			name:  "multiple GROUP BY columns",
			input: "SELECT city, state, COUNT(*) FROM orders GROUP BY city, state",
		},
		{
			name:  "aggregate without GROUP BY",
			input: "SELECT COUNT(*) FROM orders",
		},
		{
			name:  "COUNT with DISTINCT",
			input: "SELECT COUNT(DISTINCT user_id) FROM orders",
		},
		{
			name:  "arithmetic in aggregate",
			input: "SELECT SUM(price * quantity) FROM orders",
		},
		{
			name:  "GROUP BY with ORDER BY",
			input: "SELECT city, COUNT(*) as cnt FROM orders GROUP BY city ORDER BY cnt DESC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Errorf("expected SelectStmt, got %T", stmt)
				return
			}

			// Basic validation that parsing succeeded
			if len(selectStmt.Columns) == 0 {
				t.Error("expected at least one column")
			}

			// Check for function calls in columns
			for _, col := range selectStmt.Columns {
				if fc, ok := col.Expr.(*FunctionCall); ok {
					if fc.Name == "" {
						t.Error("function call has empty name")
					}
				}
			}
		})
	}
}

func TestParseFunctionCalls(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantFunction string
		wantDistinct bool
		wantArgCount int
	}{
		{
			name:         "COUNT(*)",
			input:        "SELECT COUNT(*) FROM users",
			wantFunction: "COUNT",
			wantDistinct: false,
			wantArgCount: 1,
		},
		{
			name:         "COUNT(DISTINCT id)",
			input:        "SELECT COUNT(DISTINCT id) FROM users",
			wantFunction: "COUNT",
			wantDistinct: true,
			wantArgCount: 1,
		},
		{
			name:         "SUM(amount)",
			input:        "SELECT SUM(amount) FROM orders",
			wantFunction: "SUM",
			wantDistinct: false,
			wantArgCount: 1,
		},
		{
			name:         "nested function",
			input:        "SELECT SUM(price * (1 - discount)) FROM orders",
			wantFunction: "SUM",
			wantDistinct: false,
			wantArgCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			if len(selectStmt.Columns) != 1 {
				t.Fatalf("expected 1 column, got %d", len(selectStmt.Columns))
			}

			fc, ok := selectStmt.Columns[0].Expr.(*FunctionCall)
			if !ok {
				t.Fatalf("expected FunctionCall, got %T", selectStmt.Columns[0].Expr)
			}

			if fc.Name != tt.wantFunction {
				t.Errorf("function name = %q, want %q", fc.Name, tt.wantFunction)
			}

			if fc.Distinct != tt.wantDistinct {
				t.Errorf("distinct = %v, want %v", fc.Distinct, tt.wantDistinct)
			}

			if len(fc.Args) != tt.wantArgCount {
				t.Errorf("arg count = %d, want %d", len(fc.Args), tt.wantArgCount)
			}
		})
	}
}
