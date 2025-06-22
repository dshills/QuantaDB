package parser

import (
	"testing"
)

func TestParsePrepare(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		validate func(*testing.T, *PrepareStmt)
	}{
		{
			name:  "simple PREPARE",
			input: "PREPARE stmt1 AS SELECT * FROM users WHERE id = $1",
			validate: func(t *testing.T, stmt *PrepareStmt) {
				if stmt.Name != "stmt1" {
					t.Errorf("expected name 'stmt1', got %s", stmt.Name)
				}
				if len(stmt.ParamTypes) != 0 {
					t.Errorf("expected no param types, got %d", len(stmt.ParamTypes))
				}
				if stmt.Query == nil {
					t.Error("expected query to be non-nil")
				}
			},
		},
		{
			name:  "PREPARE with parameter types",
			input: "PREPARE stmt2 (INTEGER, VARCHAR) AS SELECT * FROM users WHERE id = $1 AND name = $2",
			validate: func(t *testing.T, stmt *PrepareStmt) {
				if stmt.Name != "stmt2" {
					t.Errorf("expected name 'stmt2', got %s", stmt.Name)
				}
				if len(stmt.ParamTypes) != 2 {
					t.Fatalf("expected 2 param types, got %d", len(stmt.ParamTypes))
				}
				if stmt.ParamTypes[0].Name() != "INTEGER" {
					t.Errorf("expected first param type INTEGER, got %s", stmt.ParamTypes[0].Name())
				}
				if stmt.ParamTypes[1].Name() != "VARCHAR(0)" {
					t.Errorf("expected second param type VARCHAR(0), got %s", stmt.ParamTypes[1].Name())
				}
			},
		},
		{
			name:  "PREPARE with INSERT",
			input: "PREPARE insert_user AS INSERT INTO users (name, email) VALUES ($1, $2)",
			validate: func(t *testing.T, stmt *PrepareStmt) {
				if stmt.Name != "insert_user" {
					t.Errorf("expected name 'insert_user', got %s", stmt.Name)
				}
				if _, ok := stmt.Query.(*InsertStmt); !ok {
					t.Errorf("expected InsertStmt, got %T", stmt.Query)
				}
			},
		},
		{
			name:  "PREPARE with UPDATE",
			input: "PREPARE update_user AS UPDATE users SET name = $1 WHERE id = $2",
			validate: func(t *testing.T, stmt *PrepareStmt) {
				if stmt.Name != "update_user" {
					t.Errorf("expected name 'update_user', got %s", stmt.Name)
				}
				if _, ok := stmt.Query.(*UpdateStmt); !ok {
					t.Errorf("expected UpdateStmt, got %T", stmt.Query)
				}
			},
		},
		{
			name:  "PREPARE with DELETE",
			input: "PREPARE delete_user AS DELETE FROM users WHERE id = $1",
			validate: func(t *testing.T, stmt *PrepareStmt) {
				if stmt.Name != "delete_user" {
					t.Errorf("expected name 'delete_user', got %s", stmt.Name)
				}
				if _, ok := stmt.Query.(*DeleteStmt); !ok {
					t.Errorf("expected DeleteStmt, got %T", stmt.Query)
				}
			},
		},
		{
			name:    "missing statement name",
			input:   "PREPARE AS SELECT * FROM users",
			wantErr: true,
		},
		{
			name:    "missing AS keyword",
			input:   "PREPARE stmt1 SELECT * FROM users",
			wantErr: true,
		},
		{
			name:    "missing query",
			input:   "PREPARE stmt1 AS",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			
			prepStmt, ok := stmt.(*PrepareStmt)
			if !ok {
				t.Fatalf("expected *PrepareStmt, got %T", stmt)
			}
			
			if tt.validate != nil {
				tt.validate(t, prepStmt)
			}
		})
	}
}

func TestParseExecute(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		validate func(*testing.T, *ExecuteStmt)
	}{
		{
			name:  "EXECUTE without parameters",
			input: "EXECUTE stmt1",
			validate: func(t *testing.T, stmt *ExecuteStmt) {
				if stmt.Name != "stmt1" {
					t.Errorf("expected name 'stmt1', got %s", stmt.Name)
				}
				if len(stmt.Params) != 0 {
					t.Errorf("expected no params, got %d", len(stmt.Params))
				}
			},
		},
		{
			name:  "EXECUTE with parameters",
			input: "EXECUTE stmt1 (123, 'test')",
			validate: func(t *testing.T, stmt *ExecuteStmt) {
				if stmt.Name != "stmt1" {
					t.Errorf("expected name 'stmt1', got %s", stmt.Name)
				}
				if len(stmt.Params) != 2 {
					t.Fatalf("expected 2 params, got %d", len(stmt.Params))
				}
			},
		},
		{
			name:  "EXECUTE with expressions",
			input: "EXECUTE calc_stmt (1 + 2, user_id * 10)",
			validate: func(t *testing.T, stmt *ExecuteStmt) {
				if stmt.Name != "calc_stmt" {
					t.Errorf("expected name 'calc_stmt', got %s", stmt.Name)
				}
				if len(stmt.Params) != 2 {
					t.Fatalf("expected 2 params, got %d", len(stmt.Params))
				}
			},
		},
		{
			name:    "missing statement name",
			input:   "EXECUTE",
			wantErr: true,
		},
		{
			name:    "missing closing paren",
			input:   "EXECUTE stmt1 (123, 'test'",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			
			execStmt, ok := stmt.(*ExecuteStmt)
			if !ok {
				t.Fatalf("expected *ExecuteStmt, got %T", stmt)
			}
			
			if tt.validate != nil {
				tt.validate(t, execStmt)
			}
		})
	}
}

func TestParseDeallocate(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		validate func(*testing.T, *DeallocateStmt)
	}{
		{
			name:  "DEALLOCATE simple",
			input: "DEALLOCATE stmt1",
			validate: func(t *testing.T, stmt *DeallocateStmt) {
				if stmt.Name != "stmt1" {
					t.Errorf("expected name 'stmt1', got %s", stmt.Name)
				}
			},
		},
		{
			name:  "DEALLOCATE PREPARE",
			input: "DEALLOCATE PREPARE stmt2",
			validate: func(t *testing.T, stmt *DeallocateStmt) {
				if stmt.Name != "stmt2" {
					t.Errorf("expected name 'stmt2', got %s", stmt.Name)
				}
			},
		},
		{
			name:    "missing statement name",
			input:   "DEALLOCATE",
			wantErr: true,
		},
		{
			name:    "missing statement name after PREPARE",
			input:   "DEALLOCATE PREPARE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			
			deallocStmt, ok := stmt.(*DeallocateStmt)
			if !ok {
				t.Fatalf("expected *DeallocateStmt, got %T", stmt)
			}
			
			if tt.validate != nil {
				tt.validate(t, deallocStmt)
			}
		})
	}
}

func TestPreparedStatementString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "PREPARE statement",
			input:    "PREPARE stmt1 AS SELECT * FROM users",
			expected: "PREPARE stmt1 AS SELECT * FROM users",
		},
		{
			name:     "PREPARE with types",
			input:    "PREPARE stmt2 (INTEGER, VARCHAR) AS SELECT * FROM users WHERE id = $1 AND name = $2",
			expected: "PREPARE stmt2 (INTEGER, VARCHAR(0)) AS SELECT * FROM users WHERE (id = $1 AND name = $2)",
		},
		{
			name:     "EXECUTE statement",
			input:    "EXECUTE stmt1",
			expected: "EXECUTE stmt1",
		},
		{
			name:     "EXECUTE with params",
			input:    "EXECUTE stmt1 (123, 'test')",
			expected: "EXECUTE stmt1 (123, 'test')",
		},
		{
			name:     "DEALLOCATE statement",
			input:    "DEALLOCATE stmt1",
			expected: "DEALLOCATE stmt1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			
			got := stmt.String()
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}