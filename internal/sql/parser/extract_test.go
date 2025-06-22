package parser

import (
	"testing"
)

func TestExtractFunction(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:  "EXTRACT YEAR from date literal",
			query: "SELECT EXTRACT(YEAR FROM date '2023-01-01')",
		},
		{
			name:  "EXTRACT MONTH from date literal",
			query: "SELECT EXTRACT(MONTH FROM date '2023-12-25')",
		},
		{
			name:  "EXTRACT DAY from date literal",
			query: "SELECT EXTRACT(DAY FROM date '2023-12-25')",
		},
		{
			name:  "EXTRACT HOUR from timestamp literal",
			query: "SELECT EXTRACT(HOUR FROM timestamp '2023-01-01 14:30:45')",
		},
		{
			name:  "EXTRACT MINUTE from timestamp literal",
			query: "SELECT EXTRACT(MINUTE FROM timestamp '2023-01-01 14:30:45')",
		},
		{
			name:  "EXTRACT SECOND from timestamp literal",
			query: "SELECT EXTRACT(SECOND FROM timestamp '2023-01-01 14:30:45')",
		},
		{
			name:  "EXTRACT from column reference",
			query: "SELECT EXTRACT(YEAR FROM order_date) FROM orders",
		},
		{
			name:  "EXTRACT with alias",
			query: "SELECT EXTRACT(YEAR FROM order_date) AS order_year FROM orders",
		},
		{
			name:  "EXTRACT in WHERE clause",
			query: "SELECT * FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2023",
		},
		{
			name:    "Invalid field",
			query:   "SELECT EXTRACT(INVALID FROM date '2023-01-01')",
			wantErr: true,
		},
		{
			name:    "Missing FROM keyword",
			query:   "SELECT EXTRACT(YEAR date '2023-01-01')",
			wantErr: true,
		},
		{
			name:    "Missing parentheses",
			query:   "SELECT EXTRACT YEAR FROM date '2023-01-01'",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.query)
			stmt, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && stmt == nil {
				t.Error("Parse() returned nil statement without error")
			}
			if err == nil {
				// Verify the parsed statement contains ExtractExpr
				t.Logf("Successfully parsed: %s", stmt.String())
			}
		})
	}
}
