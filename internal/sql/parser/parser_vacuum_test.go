package parser

import (
	"testing"
)

func TestParseVacuum(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *VacuumStmt
		wantErr  bool
	}{
		{
			name:  "vacuum all tables",
			input: "VACUUM",
			expected: &VacuumStmt{
				TableName: "",
				Analyze:   false,
			},
		},
		{
			name:  "vacuum specific table",
			input: "VACUUM users",
			expected: &VacuumStmt{
				TableName: "users",
				Analyze:   false,
			},
		},
		{
			name:  "vacuum with TABLE keyword",
			input: "VACUUM TABLE users",
			expected: &VacuumStmt{
				TableName: "users",
				Analyze:   false,
			},
		},
		{
			name:  "vacuum analyze all tables",
			input: "VACUUM ANALYZE",
			expected: &VacuumStmt{
				TableName: "",
				Analyze:   true,
			},
		},
		{
			name:  "vacuum analyze specific table",
			input: "VACUUM ANALYZE users",
			expected: &VacuumStmt{
				TableName: "users",
				Analyze:   true,
			},
		},
		{
			name:  "vacuum analyze with TABLE keyword",
			input: "VACUUM ANALYZE TABLE users",
			expected: &VacuumStmt{
				TableName: "users",
				Analyze:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()

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

			vacuumStmt, ok := stmt.(*VacuumStmt)
			if !ok {
				t.Fatalf("expected VacuumStmt, got %T", stmt)
			}

			if vacuumStmt.TableName != tt.expected.TableName {
				t.Errorf("TableName: expected %q, got %q", tt.expected.TableName, vacuumStmt.TableName)
			}

			if vacuumStmt.Analyze != tt.expected.Analyze {
				t.Errorf("Analyze: expected %v, got %v", tt.expected.Analyze, vacuumStmt.Analyze)
			}
		})
	}
}

