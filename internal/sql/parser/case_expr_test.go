package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCaseExpression(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "simple case expression",
			input:    "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END",
			expected: "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE - 1 END",
		},
		{
			name:     "searched case expression",
			input:    "SELECT CASE WHEN price > 100 THEN 'expensive' WHEN price > 50 THEN 'moderate' ELSE 'cheap' END",
			expected: "SELECT CASE WHEN price > 100 THEN 'expensive' WHEN price > 50 THEN 'moderate' ELSE 'cheap' END",
		},
		{
			name:     "case without else",
			input:    "SELECT CASE WHEN active = true THEN 'yes' WHEN active = false THEN 'no' END",
			expected: "SELECT CASE WHEN active = TRUE THEN 'yes' WHEN active = FALSE THEN 'no' END",
		},
		{
			name:     "nested case expression",
			input:    "SELECT CASE WHEN age < 18 THEN 'minor' ELSE CASE WHEN age < 65 THEN 'adult' ELSE 'senior' END END",
			expected: "SELECT CASE WHEN age < 18 THEN 'minor' ELSE CASE WHEN age < 65 THEN 'adult' ELSE 'senior' END END",
		},
		{
			name:     "case in where clause",
			input:    "SELECT * FROM users WHERE CASE WHEN status = 'active' THEN created_at > '2023-01-01' ELSE true END",
			expected: "SELECT * FROM users WHERE CASE WHEN status = 'active' THEN created_at > '2023-01-01' ELSE TRUE END",
		},
		{
			name:     "simple case with multiple values",
			input:    "SELECT CASE grade WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 WHEN 'D' THEN 1 ELSE 0 END",
			expected: "SELECT CASE grade WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 WHEN 'D' THEN 1 ELSE 0 END",
		},
		{
			name:     "TPC-H style case expression",
			input:    "SELECT sum(case when nation = 'BRAZIL' then volume else 0 end)",
			expected: "SELECT SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END)",
		},
		{
			name:     "case with arithmetic",
			input:    "SELECT CASE WHEN quantity > 10 THEN price * 0.9 ELSE price END",
			expected: "SELECT CASE WHEN quantity > 10 THEN (price * 8106479329266893/9007199254740992) ELSE price END",
		},
		{
			name:     "error - missing END",
			input:    "SELECT CASE WHEN x = 1 THEN 'one'",
			wantErr:  true,
		},
		{
			name:     "error - missing THEN",
			input:    "SELECT CASE WHEN x = 1 'one' END",
			wantErr:  true,
		},
		{
			name:     "error - empty CASE",
			input:    "SELECT CASE END",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)

			// Check that the statement string representation matches expected
			actual := stmt.String()
			require.Equal(t, tt.expected, actual)
		})
	}
}

// Test that CASE expressions work in various contexts
func TestCaseExpressionContexts(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "case in select with alias",
			input: "SELECT CASE WHEN active THEN 'Y' ELSE 'N' END AS is_active FROM users",
		},
		{
			name:  "case in group by",
			input: "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END, COUNT(*) FROM users GROUP BY CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END",
		},
		{
			name:  "case in order by",
			input: "SELECT * FROM products ORDER BY CASE WHEN category = 'featured' THEN 0 ELSE 1 END, price",
		},
		// TODO: Add EXISTS support in parseComparison
		// {
		// 	name:  "case with subquery condition",
		// 	input: "SELECT CASE WHEN EXISTS (SELECT 1 FROM orders WHERE user_id = users.id) THEN 'customer' ELSE 'prospect' END FROM users",
		// },
		{
			name:  "case in aggregate function",
			input: "SELECT COUNT(CASE WHEN status = 'active' THEN 1 END) AS active_count FROM users",
		},
		{
			name:  "case with IN clause",
			input: "SELECT CASE WHEN status IN ('new', 'pending') THEN 'processing' ELSE 'done' END FROM orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			require.NoError(t, err)
			require.NotNil(t, stmt)
		})
	}
}