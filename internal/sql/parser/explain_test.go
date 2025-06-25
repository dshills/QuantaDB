package parser

import (
	"testing"
)

func TestParseExplain(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		analyze bool
		verbose bool
		format  string
	}{
		{
			name:    "Basic EXPLAIN",
			sql:     "EXPLAIN SELECT * FROM users",
			wantErr: false,
			analyze: false,
			verbose: false,
			format:  "text",
		},
		{
			name:    "EXPLAIN ANALYZE",
			sql:     "EXPLAIN ANALYZE SELECT * FROM users",
			wantErr: false,
			analyze: true,
			verbose: false,
			format:  "text",
		},
		{
			name:    "EXPLAIN with options",
			sql:     "EXPLAIN (ANALYZE, VERBOSE) SELECT * FROM users",
			wantErr: false,
			analyze: true,
			verbose: true,
			format:  "text",
		},
		{
			name:    "EXPLAIN with format",
			sql:     "EXPLAIN (FORMAT json) SELECT * FROM users",
			wantErr: false,
			analyze: false,
			verbose: false,
			format:  "json",
		},
		{
			name:    "EXPLAIN ANALYZE VERBOSE",
			sql:     "EXPLAIN ANALYZE VERBOSE SELECT * FROM users",
			wantErr: false,
			analyze: true,
			verbose: true,
			format:  "text",
		},
		{
			name:    "EXPLAIN with all options",
			sql:     "EXPLAIN (ANALYZE, VERBOSE, FORMAT json) SELECT * FROM users WHERE id = 1",
			wantErr: false,
			analyze: true,
			verbose: true,
			format:  "json",
		},
		{
			name:    "EXPLAIN DELETE",
			sql:     "EXPLAIN DELETE FROM users WHERE id = 1",
			wantErr: false,
			analyze: false,
			verbose: false,
			format:  "text",
		},
		{
			name:    "EXPLAIN UPDATE",
			sql:     "EXPLAIN UPDATE users SET name = 'test' WHERE id = 1",
			wantErr: false,
			analyze: false,
			verbose: false,
			format:  "text",
		},
		{
			name:    "EXPLAIN INSERT",
			sql:     "EXPLAIN INSERT INTO users (name) VALUES ('test')",
			wantErr: false,
			analyze: false,
			verbose: false,
			format:  "text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			stmt, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			explainStmt, ok := stmt.(*ExplainStmt)
			if !ok {
				t.Errorf("Parse() returned %T, want *ExplainStmt", stmt)
				return
			}

			if explainStmt.Analyze != tt.analyze {
				t.Errorf("ExplainStmt.Analyze = %v, want %v", explainStmt.Analyze, tt.analyze)
			}
			if explainStmt.Verbose != tt.verbose {
				t.Errorf("ExplainStmt.Verbose = %v, want %v", explainStmt.Verbose, tt.verbose)
			}
			if explainStmt.Format != tt.format {
				t.Errorf("ExplainStmt.Format = %v, want %v", explainStmt.Format, tt.format)
			}
			if explainStmt.Statement == nil {
				t.Error("ExplainStmt.Statement is nil")
			}
		})
	}
}
