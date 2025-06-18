package parser

import (
	"testing"
)

func TestParseCreateIndex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *CreateIndexStmt
		wantErr  bool
	}{
		{
			name:  "simple index",
			input: "CREATE INDEX idx_users_email ON users (email)",
			expected: &CreateIndexStmt{
				IndexName: "idx_users_email",
				TableName: "users",
				Columns:   []string{"email"},
				Unique:    false,
				IndexType: "BTREE",
			},
		},
		{
			name:  "unique index",
			input: "CREATE UNIQUE INDEX idx_users_email ON users (email)",
			expected: &CreateIndexStmt{
				IndexName: "idx_users_email",
				TableName: "users",
				Columns:   []string{"email"},
				Unique:    true,
				IndexType: "BTREE",
			},
		},
		{
			name:  "multi-column index",
			input: "CREATE INDEX idx_users_name ON users (first_name, last_name)",
			expected: &CreateIndexStmt{
				IndexName: "idx_users_name",
				TableName: "users",
				Columns:   []string{"first_name", "last_name"},
				Unique:    false,
				IndexType: "BTREE",
			},
		},
		{
			name:  "index with type",
			input: "CREATE INDEX idx_users_email ON users (email) USING HASH",
			expected: &CreateIndexStmt{
				IndexName: "idx_users_email",
				TableName: "users",
				Columns:   []string{"email"},
				Unique:    false,
				IndexType: "HASH",
			},
		},
		{
			name:  "index with ASC/DESC",
			input: "CREATE INDEX idx_users_created ON users (created_at DESC)",
			expected: &CreateIndexStmt{
				IndexName: "idx_users_created",
				TableName: "users",
				Columns:   []string{"created_at"},
				Unique:    false,
				IndexType: "BTREE",
			},
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

			createIdx, ok := stmt.(*CreateIndexStmt)
			if !ok {
				t.Errorf("Expected CreateIndexStmt, got %T", stmt)
				return
			}

			if createIdx.IndexName != tt.expected.IndexName {
				t.Errorf("IndexName = %v, want %v", createIdx.IndexName, tt.expected.IndexName)
			}
			if createIdx.TableName != tt.expected.TableName {
				t.Errorf("TableName = %v, want %v", createIdx.TableName, tt.expected.TableName)
			}
			if len(createIdx.Columns) != len(tt.expected.Columns) {
				t.Errorf("Columns count = %v, want %v", len(createIdx.Columns), len(tt.expected.Columns))
			} else {
				for i, col := range createIdx.Columns {
					if col != tt.expected.Columns[i] {
						t.Errorf("Column[%d] = %v, want %v", i, col, tt.expected.Columns[i])
					}
				}
			}
			if createIdx.Unique != tt.expected.Unique {
				t.Errorf("Unique = %v, want %v", createIdx.Unique, tt.expected.Unique)
			}
			if createIdx.IndexType != tt.expected.IndexType {
				t.Errorf("IndexType = %v, want %v", createIdx.IndexType, tt.expected.IndexType)
			}
		})
	}
}

func TestParseDropIndex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *DropIndexStmt
		wantErr  bool
	}{
		{
			name:  "drop index simple",
			input: "DROP INDEX idx_users_email",
			expected: &DropIndexStmt{
				IndexName: "idx_users_email",
				TableName: "",
			},
		},
		{
			name:  "drop index with table",
			input: "DROP INDEX idx_users_email ON users",
			expected: &DropIndexStmt{
				IndexName: "idx_users_email",
				TableName: "users",
			},
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

			dropIdx, ok := stmt.(*DropIndexStmt)
			if !ok {
				t.Errorf("Expected DropIndexStmt, got %T", stmt)
				return
			}

			if dropIdx.IndexName != tt.expected.IndexName {
				t.Errorf("IndexName = %v, want %v", dropIdx.IndexName, tt.expected.IndexName)
			}
			if dropIdx.TableName != tt.expected.TableName {
				t.Errorf("TableName = %v, want %v", dropIdx.TableName, tt.expected.TableName)
			}
		})
	}
}