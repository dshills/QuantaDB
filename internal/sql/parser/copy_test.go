package parser

import (
	"testing"
)

func TestParseCopy(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *CopyStmt
		wantErr  bool
	}{
		{
			name:  "COPY FROM STDIN",
			input: "COPY users FROM STDIN",
			expected: &CopyStmt{
				TableName: "users",
				Direction: CopyFrom,
				Source:    "STDIN",
				Options:   map[string]string{},
			},
		},
		{
			name:  "COPY TO STDOUT",
			input: "COPY users TO STDOUT",
			expected: &CopyStmt{
				TableName: "users",
				Direction: CopyTo,
				Source:    "STDOUT",
				Options:   map[string]string{},
			},
		},
		{
			name:  "COPY with column list",
			input: "COPY users (id, name, email) FROM STDIN",
			expected: &CopyStmt{
				TableName: "users",
				Columns:   []string{"id", "name", "email"},
				Direction: CopyFrom,
				Source:    "STDIN",
				Options:   map[string]string{},
			},
		},
		{
			name:  "COPY with CSV format",
			input: "COPY users FROM STDIN WITH (FORMAT CSV)",
			expected: &CopyStmt{
				TableName: "users",
				Direction: CopyFrom,
				Source:    "STDIN",
				Options: map[string]string{
					"FORMAT": "CSV",
				},
			},
		},
		{
			name:  "COPY with delimiter",
			input: "COPY users FROM STDIN WITH (DELIMITER '|')",
			expected: &CopyStmt{
				TableName: "users",
				Direction: CopyFrom,
				Source:    "STDIN",
				Options: map[string]string{
					"DELIMITER": "|",
				},
			},
		},
		{
			name:  "COPY with multiple options",
			input: "COPY users FROM STDIN WITH (FORMAT CSV, DELIMITER ',', HEADER)",
			expected: &CopyStmt{
				TableName: "users",
				Direction: CopyFrom,
				Source:    "STDIN",
				Options: map[string]string{
					"FORMAT":    "CSV",
					"DELIMITER": ",",
					"HEADER":    "",
				},
			},
		},
		{
			name:  "COPY from file",
			input: "COPY users FROM '/tmp/users.csv'",
			expected: &CopyStmt{
				TableName: "users",
				Direction: CopyFrom,
				Source:    "/tmp/users.csv",
				Options:   map[string]string{},
			},
		},
		{
			name:  "COPY to file",
			input: "COPY users TO '/tmp/users_backup.csv'",
			expected: &CopyStmt{
				TableName: "users",
				Direction: CopyTo,
				Source:    "/tmp/users_backup.csv",
				Options:   map[string]string{},
			},
		},
		{
			name:    "COPY missing direction",
			input:   "COPY users STDIN",
			wantErr: true,
		},
		{
			name:    "COPY missing source",
			input:   "COPY users FROM",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			
			if stmt == nil {
				t.Fatal("expected statement, got nil")
			}
			
			copyStmt, ok := stmt.(*CopyStmt)
			if !ok {
				t.Fatalf("expected *CopyStmt, got %T", stmt)
			}
			
			// Compare fields
			if copyStmt.TableName != tt.expected.TableName {
				t.Errorf("TableName: expected %s, got %s", tt.expected.TableName, copyStmt.TableName)
			}
			
			if copyStmt.Direction != tt.expected.Direction {
				t.Errorf("Direction: expected %v, got %v", tt.expected.Direction, copyStmt.Direction)
			}
			
			if copyStmt.Source != tt.expected.Source {
				t.Errorf("Source: expected %s, got %s", tt.expected.Source, copyStmt.Source)
			}
			
			// Compare columns
			if len(copyStmt.Columns) != len(tt.expected.Columns) {
				t.Errorf("Columns length: expected %d, got %d", len(tt.expected.Columns), len(copyStmt.Columns))
			} else {
				for i, col := range copyStmt.Columns {
					if col != tt.expected.Columns[i] {
						t.Errorf("Column[%d]: expected %s, got %s", i, tt.expected.Columns[i], col)
					}
				}
			}
			
			// Compare options
			if len(copyStmt.Options) != len(tt.expected.Options) {
				t.Errorf("Options length: expected %d, got %d", len(tt.expected.Options), len(copyStmt.Options))
			} else {
				for k, v := range tt.expected.Options {
					if got, ok := copyStmt.Options[k]; !ok {
						t.Errorf("Option %s not found", k)
					} else if got != v {
						t.Errorf("Option %s: expected %s, got %s", k, v, got)
					}
				}
			}
		})
	}
}

func TestCopyStmtString(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *CopyStmt
		expected string
	}{
		{
			name: "basic COPY FROM",
			stmt: &CopyStmt{
				TableName: "users",
				Direction: CopyFrom,
				Source:    "STDIN",
				Options:   map[string]string{},
			},
			expected: "COPY users FROM STDIN",
		},
		{
			name: "COPY with columns",
			stmt: &CopyStmt{
				TableName: "users",
				Columns:   []string{"id", "name"},
				Direction: CopyTo,
				Source:    "STDOUT",
				Options:   map[string]string{},
			},
			expected: "COPY users (id, name) TO STDOUT",
		},
		{
			name: "COPY with options",
			stmt: &CopyStmt{
				TableName: "users",
				Direction: CopyFrom,
				Source:    "STDIN",
				Options: map[string]string{
					"FORMAT":    "CSV",
					"DELIMITER": ",",
				},
			},
			expected: "COPY users FROM STDIN WITH",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.stmt.String()
			// For options test, just check prefix since map order is non-deterministic
			if tt.name == "COPY with options" {
				if len(got) < len(tt.expected) || got[:len(tt.expected)] != tt.expected {
					t.Errorf("expected string to start with %q, got %q", tt.expected, got)
				}
			} else if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}