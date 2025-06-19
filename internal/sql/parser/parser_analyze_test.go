package parser

import (
	"testing"
)

func TestParseAnalyze(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *AnalyzeStmt
		wantErr bool
	}{
		{
			name: "simple analyze",
			sql:  "ANALYZE users",
			want: &AnalyzeStmt{
				TableName: "users",
				Columns:   []string{},
			},
		},
		{
			name: "analyze with TABLE keyword",
			sql:  "ANALYZE TABLE users",
			want: &AnalyzeStmt{
				TableName: "users",
				Columns:   []string{},
			},
		},
		{
			name: "analyze specific columns",
			sql:  "ANALYZE users (id, email, created_at)",
			want: &AnalyzeStmt{
				TableName: "users",
				Columns:   []string{"id", "email", "created_at"},
			},
		},
		{
			name: "analyze single column",
			sql:  "ANALYZE users (email)",
			want: &AnalyzeStmt{
				TableName: "users",
				Columns:   []string{"email"},
			},
		},
		{
			name: "analyze with semicolon",
			sql:  "ANALYZE users;",
			want: &AnalyzeStmt{
				TableName: "users",
				Columns:   []string{},
			},
		},
		{
			name:    "missing table name",
			sql:     "ANALYZE",
			wantErr: true,
		},
		{
			name:    "missing closing paren",
			sql:     "ANALYZE users (id, email",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			stmt, err := p.Parse()

			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err != nil {
				return
			}

			analyzeStmt, ok := stmt.(*AnalyzeStmt)
			if !ok {
				t.Fatalf("expected *AnalyzeStmt, got %T", stmt)
			}

			// Check table name
			if analyzeStmt.TableName != tt.want.TableName {
				t.Errorf("TableName = %v, want %v", analyzeStmt.TableName, tt.want.TableName)
			}

			// Check columns
			if len(analyzeStmt.Columns) != len(tt.want.Columns) {
				t.Errorf("Columns len = %v, want %v", len(analyzeStmt.Columns), len(tt.want.Columns))
			} else {
				for i, col := range analyzeStmt.Columns {
					if col != tt.want.Columns[i] {
						t.Errorf("Columns[%d] = %v, want %v", i, col, tt.want.Columns[i])
					}
				}
			}
		})
	}
}
