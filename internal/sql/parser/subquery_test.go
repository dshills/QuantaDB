package parser

import (
	"fmt"
	"testing"
)

func TestSubqueryParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// EXISTS subqueries
		{
			name:    "simple EXISTS",
			input:   "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 123)",
			wantErr: false,
		},
		{
			name:    "NOT EXISTS",
			input:   "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = 123)",
			wantErr: false,
		},
		{
			name:    "EXISTS with complex subquery",
			input:   "SELECT * FROM users WHERE EXISTS (SELECT * FROM orders WHERE user_id = 123 AND amount > 100)",
			wantErr: false,
		},

		// IN subqueries
		{
			name:    "IN with subquery",
			input:   "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantErr: false,
		},
		{
			name:    "NOT IN with subquery",
			input:   "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE amount > 100)",
			wantErr: false,
		},
		{
			name:    "IN with value list",
			input:   "SELECT * FROM users WHERE id IN (1, 2, 3)",
			wantErr: false,
		},

		// Scalar subqueries
		{
			name:    "scalar subquery in SELECT",
			input:   "SELECT name, (SELECT 5 FROM orders WHERE user_id = 123) AS order_count FROM users",
			wantErr: false,
		},
		{
			name:    "scalar subquery in WHERE",
			input:   "SELECT * FROM orders WHERE amount > (SELECT 100 FROM orders)",
			wantErr: false,
		},

		// Multiple subqueries
		{
			name:    "multiple subqueries",
			input:   "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 123) AND id NOT IN (SELECT user_id FROM banned_users)",
			wantErr: false,
		},

		// Correlated subqueries (simplified without aliases for now)
		{
			name:    "correlated subquery",
			input:   "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if stmt == nil {
				t.Errorf("expected statement but got nil")
				return
			}

			// Verify it's a SELECT statement
			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Errorf("expected SelectStmt but got %T", stmt)
				return
			}

			// For debugging, print the parsed statement
			t.Logf("Parsed: %s", selectStmt.String())
		})
	}
}

func TestSubqueryExpressionTypes(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		checkExpr func(*SelectStmt) error
	}{
		{
			name:  "EXISTS expression type",
			input: "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)",
			checkExpr: func(stmt *SelectStmt) error {
				if stmt.Where == nil {
					return errorf("expected WHERE clause")
				}
				if _, ok := stmt.Where.(*ExistsExpr); !ok {
					return errorf("expected ExistsExpr but got %T", stmt.Where)
				}
				return nil
			},
		},
		{
			name:  "NOT EXISTS expression type",
			input: "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders)",
			checkExpr: func(stmt *SelectStmt) error {
				if stmt.Where == nil {
					return errorf("expected WHERE clause")
				}
				existsExpr, ok := stmt.Where.(*ExistsExpr)
				if !ok {
					return errorf("expected ExistsExpr but got %T", stmt.Where)
				}
				if !existsExpr.Not {
					return errorf("expected Not to be true")
				}
				return nil
			},
		},
		{
			name:  "IN subquery expression type",
			input: "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			checkExpr: func(stmt *SelectStmt) error {
				if stmt.Where == nil {
					return errorf("expected WHERE clause")
				}
				inExpr, ok := stmt.Where.(*InExpr)
				if !ok {
					return errorf("expected InExpr but got %T", stmt.Where)
				}
				if inExpr.Subquery == nil {
					return errorf("expected subquery in InExpr")
				}
				if len(inExpr.Values) > 0 {
					return errorf("expected no values when subquery is present")
				}
				return nil
			},
		},
		{
			name:  "IN value list expression type",
			input: "SELECT * FROM users WHERE id IN (1, 2, 3)",
			checkExpr: func(stmt *SelectStmt) error {
				if stmt.Where == nil {
					return errorf("expected WHERE clause")
				}
				inExpr, ok := stmt.Where.(*InExpr)
				if !ok {
					return errorf("expected InExpr but got %T", stmt.Where)
				}
				if inExpr.Subquery != nil {
					return errorf("expected no subquery for value list")
				}
				if len(inExpr.Values) != 3 {
					return errorf("expected 3 values but got %d", len(inExpr.Values))
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("expected SelectStmt but got %T", stmt)
			}

			if err := tt.checkExpr(selectStmt); err != nil {
				t.Error(err)
			}
		})
	}
}

func errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
