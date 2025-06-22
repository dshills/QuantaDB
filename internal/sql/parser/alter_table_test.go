package parser

import (
	"testing"
)

func TestParseAlterTable(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "ALTER TABLE ADD COLUMN",
			input:    "ALTER TABLE users ADD COLUMN email VARCHAR(100)",
			expected: "ALTER TABLE users ADD COLUMN email VARCHAR(100)",
		},
		{
			name:     "ALTER TABLE ADD COLUMN without COLUMN keyword",
			input:    "ALTER TABLE users ADD email VARCHAR(100)",
			expected: "ALTER TABLE users ADD COLUMN email VARCHAR(100)",
		},
		{
			name:     "ALTER TABLE DROP COLUMN",
			input:    "ALTER TABLE users DROP COLUMN email",
			expected: "ALTER TABLE users DROP COLUMN email",
		},
		{
			name:     "ALTER TABLE ADD COLUMN with constraints",
			input:    "ALTER TABLE users ADD COLUMN age INTEGER NOT NULL DEFAULT 0",
			expected: "ALTER TABLE users ADD COLUMN age INTEGER NOT NULL DEFAULT 0",
		},
		{
			name:    "ALTER TABLE missing table name",
			input:   "ALTER TABLE",
			wantErr: true,
		},
		{
			name:    "ALTER TABLE missing action",
			input:   "ALTER TABLE users",
			wantErr: true,
		},
		{
			name:    "ALTER TABLE DROP missing COLUMN keyword",
			input:   "ALTER TABLE users DROP email",
			wantErr: true,
		},
		{
			name:    "ALTER TABLE ADD missing column definition",
			input:   "ALTER TABLE users ADD COLUMN",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			alterStmt, ok := stmt.(*AlterTableStmt)
			if !ok {
				t.Errorf("Expected AlterTableStmt, got %T", stmt)
				return
			}

			// Normalize whitespace for comparison
			got := normalizeWhitespace(alterStmt.String())
			expected := normalizeWhitespace(tt.expected)

			if got != expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestAlterTableStmtTypes(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		action     AlterTableAction
		tableName  string
		columnName string // For DROP COLUMN
	}{
		{
			name:      "ADD COLUMN",
			input:     "ALTER TABLE users ADD COLUMN email VARCHAR(50)",
			action:    AlterTableActionAddColumn,
			tableName: "users",
		},
		{
			name:       "DROP COLUMN",
			input:      "ALTER TABLE users DROP COLUMN email",
			action:     AlterTableActionDropColumn,
			tableName:  "users",
			columnName: "email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			alterStmt, ok := stmt.(*AlterTableStmt)
			if !ok {
				t.Fatalf("Expected AlterTableStmt, got %T", stmt)
			}

			if alterStmt.Action != tt.action {
				t.Errorf("Expected action %d, got %d", tt.action, alterStmt.Action)
			}

			if alterStmt.TableName != tt.tableName {
				t.Errorf("Expected table name '%s', got '%s'", tt.tableName, alterStmt.TableName)
			}

			if tt.action == AlterTableActionAddColumn {
				if alterStmt.Column == nil {
					t.Error("Expected column definition for ADD COLUMN")
				}
			}

			if tt.action == AlterTableActionDropColumn {
				if alterStmt.ColumnName != tt.columnName {
					t.Errorf("Expected column name '%s', got '%s'", tt.columnName, alterStmt.ColumnName)
				}
			}
		})
	}
}

func TestAlterTableAddColumnDetails(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN email VARCHAR(100) NOT NULL DEFAULT 'unknown'"
	parser := NewParser(input)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected AlterTableStmt, got %T", stmt)
	}

	if alterStmt.Action != AlterTableActionAddColumn {
		t.Errorf("Expected ADD COLUMN action")
	}

	if alterStmt.Column == nil {
		t.Fatal("Expected column definition")
	}

	col := alterStmt.Column
	if col.Name != "email" {
		t.Errorf("Expected column name 'email', got '%s'", col.Name)
	}

	if col.DataType.Name() != "VARCHAR(100)" {
		t.Errorf("Expected VARCHAR(100), got %s", col.DataType.Name())
	}

	// Check that constraints were parsed (simplified check)
	if len(col.Constraints) == 0 {
		t.Error("Expected constraints to be parsed")
	}
}