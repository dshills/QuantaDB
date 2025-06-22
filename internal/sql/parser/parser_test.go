package parser

import (
	"strings"
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestParseCreateTable(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "Simple CREATE TABLE",
			input:    "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))",
			expected: "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))",
		},
		{
			name:     "CREATE TABLE with multiple columns",
			input:    "CREATE TABLE products (id BIGINT, name TEXT NOT NULL, price DECIMAL(10, 2))",
			expected: "CREATE TABLE products (id BIGINT, name TEXT NOT NULL, price DECIMAL(10,2))",
		},
		{
			name:     "CREATE TABLE with table constraint",
			input:    "CREATE TABLE orders (id INTEGER, user_id INTEGER, PRIMARY KEY (id))",
			expected: "CREATE TABLE orders (id INTEGER, user_id INTEGER, PRIMARY KEY (id))",
		},
		{
			name:     "CREATE TABLE with all column constraints",
			input:    "CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE DEFAULT 'unknown')",
			expected: "CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE DEFAULT 'unknown')",
		},
		{
			name:    "CREATE TABLE missing table name",
			input:   "CREATE TABLE",
			wantErr: true,
		},
		{
			name:    "CREATE TABLE missing columns",
			input:   "CREATE TABLE users",
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

			createStmt, ok := stmt.(*CreateTableStmt)
			if !ok {
				t.Errorf("Expected CreateTableStmt, got %T", stmt)
				return
			}

			// Normalize whitespace for comparison
			got := normalizeWhitespace(createStmt.String())
			expected := normalizeWhitespace(tt.expected)

			if got != expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestParseInsert(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "Simple INSERT",
			input:    "INSERT INTO users VALUES (1, 'John')",
			expected: "INSERT INTO users VALUES (1, 'John')",
		},
		{
			name:     "INSERT with column list",
			input:    "INSERT INTO users (id, name) VALUES (1, 'John')",
			expected: "INSERT INTO users (id, name) VALUES (1, 'John')",
		},
		{
			name:     "INSERT multiple rows",
			input:    "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')",
			expected: "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')",
		},
		{
			name:     "INSERT with NULL",
			input:    "INSERT INTO users VALUES (1, NULL)",
			expected: "INSERT INTO users VALUES (1, NULL)",
		},
		{
			name:     "INSERT with boolean",
			input:    "INSERT INTO users VALUES (1, TRUE, FALSE)",
			expected: "INSERT INTO users VALUES (1, TRUE, FALSE)",
		},
		{
			name:    "INSERT missing table",
			input:   "INSERT INTO",
			wantErr: true,
		},
		{
			name:    "INSERT missing VALUES",
			input:   "INSERT INTO users",
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

			insertStmt, ok := stmt.(*InsertStmt)
			if !ok {
				t.Errorf("Expected InsertStmt, got %T", stmt)
				return
			}

			got := normalizeWhitespace(insertStmt.String())
			expected := normalizeWhitespace(tt.expected)

			if got != expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestParseSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "Simple SELECT *",
			input:    "SELECT * FROM users",
			expected: "SELECT * FROM users",
		},
		{
			name:     "SELECT with columns",
			input:    "SELECT id, name FROM users",
			expected: "SELECT id, name FROM users",
		},
		{
			name:     "SELECT with WHERE",
			input:    "SELECT * FROM users WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
		},
		{
			name:     "SELECT with complex WHERE",
			input:    "SELECT * FROM users WHERE id > 10 AND name LIKE 'John%'",
			expected: "SELECT * FROM users WHERE (id > 10 AND name LIKE 'John%')",
		},
		{
			name:     "SELECT with ORDER BY",
			input:    "SELECT * FROM users ORDER BY name ASC, id DESC",
			expected: "SELECT * FROM users ORDER BY name ASC, id DESC",
		},
		{
			name:     "SELECT with LIMIT and OFFSET",
			input:    "SELECT * FROM users LIMIT 10 OFFSET 20",
			expected: "SELECT * FROM users LIMIT 10 OFFSET 20",
		},
		{
			name:     "SELECT with alias",
			input:    "SELECT id AS user_id, name AS user_name FROM users",
			expected: "SELECT id AS user_id, name AS user_name FROM users",
		},
		{
			name:     "SELECT with expressions",
			input:    "SELECT id + 1, name FROM users",
			expected: "SELECT (id + 1), name FROM users",
		},
		{
			name:     "SELECT without FROM",
			input:    "SELECT 1",
			expected: "SELECT 1",
		},
		{
			name:     "SELECT * without FROM",
			input:    "SELECT *",
			expected: "SELECT *",
		},
		{
			name:    "SELECT missing table",
			input:   "SELECT * FROM",
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

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Errorf("Expected SelectStmt, got %T", stmt)
				return
			}

			got := normalizeWhitespace(selectStmt.String())
			expected := normalizeWhitespace(tt.expected)

			if got != expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestParseUpdate(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "Simple UPDATE",
			input:    "UPDATE users SET name = 'John'",
			expected: "UPDATE users SET name = 'John'",
		},
		{
			name:     "UPDATE multiple columns",
			input:    "UPDATE users SET name = 'John', age = 30",
			expected: "UPDATE users SET name = 'John', age = 30",
		},
		{
			name:     "UPDATE with WHERE",
			input:    "UPDATE users SET name = 'John' WHERE id = 1",
			expected: "UPDATE users SET name = 'John' WHERE id = 1",
		},
		{
			name:     "UPDATE with expression",
			input:    "UPDATE users SET age = age + 1 WHERE age < 100",
			expected: "UPDATE users SET age = (age + 1) WHERE age < 100",
		},
		{
			name:    "UPDATE missing SET",
			input:   "UPDATE users",
			wantErr: true,
		},
		{
			name:    "UPDATE missing table",
			input:   "UPDATE",
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

			updateStmt, ok := stmt.(*UpdateStmt)
			if !ok {
				t.Errorf("Expected UpdateStmt, got %T", stmt)
				return
			}

			got := normalizeWhitespace(updateStmt.String())
			expected := normalizeWhitespace(tt.expected)

			if got != expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestParseDelete(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "Simple DELETE",
			input:    "DELETE FROM users",
			expected: "DELETE FROM users",
		},
		{
			name:     "DELETE with WHERE",
			input:    "DELETE FROM users WHERE id = 1",
			expected: "DELETE FROM users WHERE id = 1",
		},
		{
			name:     "DELETE with complex WHERE",
			input:    "DELETE FROM users WHERE age > 18 AND status = 'inactive'",
			expected: "DELETE FROM users WHERE (age > 18 AND status = 'inactive')",
		},
		{
			name:    "DELETE missing FROM",
			input:   "DELETE users",
			wantErr: true,
		},
		{
			name:    "DELETE missing table",
			input:   "DELETE FROM",
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

			deleteStmt, ok := stmt.(*DeleteStmt)
			if !ok {
				t.Errorf("Expected DeleteStmt, got %T", stmt)
				return
			}

			got := normalizeWhitespace(deleteStmt.String())
			expected := normalizeWhitespace(tt.expected)

			if got != expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestParseDrop(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "Simple DROP TABLE",
			input:    "DROP TABLE users",
			expected: "DROP TABLE users",
		},
		{
			name:    "DROP missing TABLE keyword",
			input:   "DROP users",
			wantErr: true,
		},
		{
			name:    "DROP missing table name",
			input:   "DROP TABLE",
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

			dropStmt, ok := stmt.(*DropTableStmt)
			if !ok {
				t.Errorf("Expected DropTableStmt, got %T", stmt)
				return
			}

			got := normalizeWhitespace(dropStmt.String())
			expected := normalizeWhitespace(tt.expected)

			if got != expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestParseExpressions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Arithmetic
		{
			name:     "Addition",
			input:    "SELECT a + b FROM t",
			expected: "SELECT (a + b) FROM t",
		},
		{
			name:     "Complex arithmetic",
			input:    "SELECT a + b * c FROM t",
			expected: "SELECT (a + (b * c)) FROM t",
		},
		{
			name:     "Parentheses",
			input:    "SELECT (a + b) * c FROM t",
			expected: "SELECT (((a + b)) * c) FROM t",
		},
		// Comparisons
		{
			name:     "Equal",
			input:    "SELECT * FROM t WHERE a = 1",
			expected: "SELECT * FROM t WHERE a = 1",
		},
		{
			name:     "Not equal",
			input:    "SELECT * FROM t WHERE a != 1",
			expected: "SELECT * FROM t WHERE a != 1",
		},
		{
			name:     "Greater than",
			input:    "SELECT * FROM t WHERE a > 1",
			expected: "SELECT * FROM t WHERE a > 1",
		},
		// Logical
		{
			name:     "AND",
			input:    "SELECT * FROM t WHERE a = 1 AND b = 2",
			expected: "SELECT * FROM t WHERE (a = 1 AND b = 2)",
		},
		{
			name:     "OR",
			input:    "SELECT * FROM t WHERE a = 1 OR b = 2",
			expected: "SELECT * FROM t WHERE (a = 1 OR b = 2)",
		},
		{
			name:     "NOT",
			input:    "SELECT * FROM t WHERE NOT a = 1",
			expected: "SELECT * FROM t WHERE NOT a = 1",
		},
		// Special operators
		{
			name:     "LIKE",
			input:    "SELECT * FROM t WHERE name LIKE 'John%'",
			expected: "SELECT * FROM t WHERE name LIKE 'John%'",
		},
		{
			name:     "IN",
			input:    "SELECT * FROM t WHERE id IN (1, 2, 3)",
			expected: "SELECT * FROM t WHERE id IN (1, 2, 3)",
		},
		{
			name:     "BETWEEN",
			input:    "SELECT * FROM t WHERE age BETWEEN 18 AND 65",
			expected: "SELECT * FROM t WHERE age BETWEEN 18 AND 65",
		},
		{
			name:     "IS NULL",
			input:    "SELECT * FROM t WHERE name IS NULL",
			expected: "SELECT * FROM t WHERE name IS NULL",
		},
		{
			name:     "IS NOT NULL",
			input:    "SELECT * FROM t WHERE name IS NOT NULL",
			expected: "SELECT * FROM t WHERE name IS NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			got := normalizeWhitespace(stmt.String())
			expected := normalizeWhitespace(tt.expected)

			if got != expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestParseMultiple(t *testing.T) {
	input := `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));
		INSERT INTO users VALUES (1, 'John');
		SELECT * FROM users WHERE id = 1;
	`

	parser := NewParser(input)
	statements, err := parser.ParseMultiple()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(statements) != 3 {
		t.Fatalf("Expected 3 statements, got %d", len(statements))
	}

	// Check statement types
	if _, ok := statements[0].(*CreateTableStmt); !ok {
		t.Errorf("Expected CreateTableStmt, got %T", statements[0])
	}
	if _, ok := statements[1].(*InsertStmt); !ok {
		t.Errorf("Expected InsertStmt, got %T", statements[1])
	}
	if _, ok := statements[2].(*SelectStmt); !ok {
		t.Errorf("Expected SelectStmt, got %T", statements[2])
	}
}

func TestParseDataTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		checkCol func(*ColumnDef) bool
	}{
		{
			name:  "INTEGER",
			input: "CREATE TABLE t (c INTEGER)",
			checkCol: func(col *ColumnDef) bool {
				return col.DataType == types.Integer
			},
		},
		{
			name:  "BIGINT",
			input: "CREATE TABLE t (c BIGINT)",
			checkCol: func(col *ColumnDef) bool {
				return col.DataType == types.BigInt
			},
		},
		{
			name:  "VARCHAR with length",
			input: "CREATE TABLE t (c VARCHAR(100))",
			checkCol: func(col *ColumnDef) bool {
				// Check if it's a VARCHAR(100) by checking the name
				return col.DataType.Name() == "VARCHAR(100)"
			},
		},
		{
			name:  "DECIMAL with precision and scale",
			input: "CREATE TABLE t (c DECIMAL(10, 2))",
			checkCol: func(col *ColumnDef) bool {
				// Check if it's a DECIMAL(10,2) by checking the name
				return col.DataType.Name() == "DECIMAL(10,2)"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			createStmt, ok := stmt.(*CreateTableStmt)
			if !ok {
				t.Errorf("Expected CreateTableStmt, got %T", stmt)
				return
			}

			if len(createStmt.Columns) != 1 {
				t.Errorf("Expected 1 column, got %d", len(createStmt.Columns))
				return
			}

			if !tt.checkCol(&createStmt.Columns[0]) {
				t.Errorf("Column check failed for %s", tt.name)
			}
		})
	}
}

func TestParseLiterals(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		checkValue func(*Literal) bool
	}{
		{
			name:  "Integer literal",
			input: "INSERT INTO t VALUES (42)",
			checkValue: func(lit *Literal) bool {
				val, ok := lit.Value.Data.(int32)
				return ok && val == 42
			},
		},
		{
			name:  "String literal",
			input: "INSERT INTO t VALUES ('hello')",
			checkValue: func(lit *Literal) bool {
				val, ok := lit.Value.Data.(string)
				return ok && val == "hello"
			},
		},
		{
			name:  "Boolean true",
			input: "INSERT INTO t VALUES (TRUE)",
			checkValue: func(lit *Literal) bool {
				val, ok := lit.Value.Data.(bool)
				return ok && val == true
			},
		},
		{
			name:  "Boolean false",
			input: "INSERT INTO t VALUES (FALSE)",
			checkValue: func(lit *Literal) bool {
				val, ok := lit.Value.Data.(bool)
				return ok && val == false
			},
		},
		{
			name:  "NULL",
			input: "INSERT INTO t VALUES (NULL)",
			checkValue: func(lit *Literal) bool {
				return lit.Value.IsNull()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			insertStmt, ok := stmt.(*InsertStmt)
			if !ok {
				t.Errorf("Expected InsertStmt, got %T", stmt)
				return
			}

			if len(insertStmt.Values) != 1 || len(insertStmt.Values[0]) != 1 {
				t.Errorf("Expected 1 value, got %d", len(insertStmt.Values))
				return
			}

			lit, ok := insertStmt.Values[0][0].(*Literal)
			if !ok {
				t.Errorf("Expected Literal, got %T", insertStmt.Values[0][0])
				return
			}

			if !tt.checkValue(lit) {
				t.Errorf("Value check failed for %s", tt.name)
			}
		})
	}
}

// Helper function to normalize whitespace for string comparison.
func normalizeWhitespace(s string) string {
	// Replace multiple spaces with single space
	s = strings.Join(strings.Fields(s), " ")
	// Trim leading/trailing whitespace
	return strings.TrimSpace(s)
}
