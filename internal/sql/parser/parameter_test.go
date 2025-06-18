package parser

import (
	"testing"
)

func TestLexerParameters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "Simple parameter",
			input:    "$1",
			expected: []TokenType{TokenParam, TokenEOF},
			values:   []string{"$1", ""},
		},
		{
			name:     "Multiple parameters",
			input:    "$1, $2, $10",
			expected: []TokenType{TokenParam, TokenComma, TokenParam, TokenComma, TokenParam, TokenEOF},
			values:   []string{"$1", ",", "$2", ",", "$10", ""},
		},
		{
			name:     "Parameter in expression",
			input:    "id = $1",
			expected: []TokenType{TokenIdentifier, TokenEqual, TokenParam, TokenEOF},
			values:   []string{"id", "=", "$1", ""},
		},
		{
			name:     "Invalid parameter (no number)",
			input:    "$",
			expected: []TokenType{TokenError, TokenEOF},
			values:   []string{"invalid parameter placeholder", ""},
		},
		{
			name:     "Invalid parameter (letter after $)",
			input:    "$abc",
			expected: []TokenType{TokenError, TokenIdentifier, TokenEOF},
			values:   []string{"invalid parameter placeholder", "abc", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			
			for i, expectedType := range tt.expected {
				token := lexer.NextToken()
				if token.Type != expectedType {
					t.Errorf("Expected token type %v, got %v", expectedType, token.Type)
				}
				if tt.values != nil && i < len(tt.values) && token.Value != tt.values[i] {
					t.Errorf("Expected token value %q, got %q", tt.values[i], token.Value)
				}
			}
		})
	}
}

func TestParseParameters(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
		validate  func(t *testing.T, stmt Statement)
	}{
		{
			name: "SELECT with single parameter",
			sql:  "SELECT * FROM users WHERE id = $1",
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStmt)
				where := selectStmt.Where.(*ComparisonExpr)
				param := where.Right.(*ParameterRef)
				if param.Index != 1 {
					t.Errorf("Expected parameter index 1, got %d", param.Index)
				}
			},
		},
		{
			name: "SELECT with multiple parameters",
			sql:  "SELECT * FROM users WHERE id = $1 AND name = $2",
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStmt)
				and := selectStmt.Where.(*BinaryExpr)
				
				// Check left side (id = $1)
				left := and.Left.(*ComparisonExpr)
				param1 := left.Right.(*ParameterRef)
				if param1.Index != 1 {
					t.Errorf("Expected parameter index 1, got %d", param1.Index)
				}
				
				// Check right side (name = $2)
				right := and.Right.(*ComparisonExpr)
				param2 := right.Right.(*ParameterRef)
				if param2.Index != 2 {
					t.Errorf("Expected parameter index 2, got %d", param2.Index)
				}
			},
		},
		{
			name: "INSERT with parameters",
			sql:  "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
			validate: func(t *testing.T, stmt Statement) {
				insertStmt := stmt.(*InsertStmt)
				values := insertStmt.Values[0]
				
				// Check all three parameters
				for i, val := range values {
					param := val.(*ParameterRef)
					if param.Index != i+1 {
						t.Errorf("Expected parameter index %d, got %d", i+1, param.Index)
					}
				}
			},
		},
		{
			name: "UPDATE with parameters",
			sql:  "UPDATE users SET name = $1, email = $2 WHERE id = $3",
			validate: func(t *testing.T, stmt Statement) {
				updateStmt := stmt.(*UpdateStmt)
				
				// Check SET clause parameters
				if updateStmt.Assignments[0].Value.(*ParameterRef).Index != 1 {
					t.Error("Expected parameter $1 in first assignment")
				}
				if updateStmt.Assignments[1].Value.(*ParameterRef).Index != 2 {
					t.Error("Expected parameter $2 in second assignment")
				}
				
				// Check WHERE clause parameter
				where := updateStmt.Where.(*ComparisonExpr)
				if where.Right.(*ParameterRef).Index != 3 {
					t.Error("Expected parameter $3 in WHERE clause")
				}
			},
		},
		{
			name: "Parameter with large index",
			sql:  "SELECT * FROM users WHERE id = $999",
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStmt)
				where := selectStmt.Where.(*ComparisonExpr)
				param := where.Right.(*ParameterRef)
				if param.Index != 999 {
					t.Errorf("Expected parameter index 999, got %d", param.Index)
				}
			},
		},
		{
			name:      "Invalid parameter index 0",
			sql:       "SELECT * FROM users WHERE id = $0",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmt, err := parser.Parse()
			
			if tt.wantError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			if tt.validate != nil {
				tt.validate(t, stmt)
			}
		})
	}
}