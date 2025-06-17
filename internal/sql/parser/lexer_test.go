package parser

import (
	"testing"
)

func TestLexerBasicTokens(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "Simple SELECT",
			input: "SELECT * FROM users",
			expected: []Token{
				{Type: TokenSelect, Value: "SELECT"},
				{Type: TokenStar, Value: "*"},
				{Type: TokenFrom, Value: "FROM"},
				{Type: TokenIdentifier, Value: "users"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "SELECT with columns",
			input: "SELECT id, name FROM users",
			expected: []Token{
				{Type: TokenSelect, Value: "SELECT"},
				{Type: TokenIdentifier, Value: "id"},
				{Type: TokenComma, Value: ","},
				{Type: TokenIdentifier, Value: "name"},
				{Type: TokenFrom, Value: "FROM"},
				{Type: TokenIdentifier, Value: "users"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "CREATE TABLE",
			input: "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))",
			expected: []Token{
				{Type: TokenCreate, Value: "CREATE"},
				{Type: TokenTable, Value: "TABLE"},
				{Type: TokenIdentifier, Value: "users"},
				{Type: TokenLeftParen, Value: "("},
				{Type: TokenIdentifier, Value: "id"},
				{Type: TokenInteger, Value: "INTEGER"},
				{Type: TokenPrimary, Value: "PRIMARY"},
				{Type: TokenKey, Value: "KEY"},
				{Type: TokenComma, Value: ","},
				{Type: TokenIdentifier, Value: "name"},
				{Type: TokenVarchar, Value: "VARCHAR"},
				{Type: TokenLeftParen, Value: "("},
				{Type: TokenNumber, Value: "100"},
				{Type: TokenRightParen, Value: ")"},
				{Type: TokenRightParen, Value: ")"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "INSERT statement",
			input: "INSERT INTO users (id, name) VALUES (1, 'John')",
			expected: []Token{
				{Type: TokenInsert, Value: "INSERT"},
				{Type: TokenInto, Value: "INTO"},
				{Type: TokenIdentifier, Value: "users"},
				{Type: TokenLeftParen, Value: "("},
				{Type: TokenIdentifier, Value: "id"},
				{Type: TokenComma, Value: ","},
				{Type: TokenIdentifier, Value: "name"},
				{Type: TokenRightParen, Value: ")"},
				{Type: TokenValues, Value: "VALUES"},
				{Type: TokenLeftParen, Value: "("},
				{Type: TokenNumber, Value: "1"},
				{Type: TokenComma, Value: ","},
				{Type: TokenString, Value: "John"},
				{Type: TokenRightParen, Value: ")"},
				{Type: TokenEOF, Value: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			for i, expected := range tt.expected {
				token := lexer.NextToken()
				if token.Type != expected.Type {
					t.Errorf("Token %d: expected type %v, got %v", i, expected.Type, token.Type)
				}
				if token.Value != expected.Value {
					t.Errorf("Token %d: expected value %q, got %q", i, expected.Value, token.Value)
				}
			}
		})
	}
}

func TestLexerOperators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "Comparison operators",
			input: "= != < <= > >= <>",
			expected: []Token{
				{Type: TokenEqual, Value: "="},
				{Type: TokenNotEqual, Value: "!="},
				{Type: TokenLess, Value: "<"},
				{Type: TokenLessEqual, Value: "<="},
				{Type: TokenGreater, Value: ">"},
				{Type: TokenGreaterEqual, Value: ">="},
				{Type: TokenNotEqual, Value: "<>"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "Arithmetic operators",
			input: "+ - * / %",
			expected: []Token{
				{Type: TokenPlus, Value: "+"},
				{Type: TokenMinus, Value: "-"},
				{Type: TokenStar, Value: "*"},
				{Type: TokenSlash, Value: "/"},
				{Type: TokenPercent, Value: "%"},
				{Type: TokenEOF, Value: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			for i, expected := range tt.expected {
				token := lexer.NextToken()
				if token.Type != expected.Type {
					t.Errorf("Token %d: expected type %v, got %v", i, expected.Type, token.Type)
				}
				if token.Value != expected.Value {
					t.Errorf("Token %d: expected value %q, got %q", i, expected.Value, token.Value)
				}
			}
		})
	}
}

func TestLexerStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		isError  bool
	}{
		{
			name:     "Simple string",
			input:    "'hello world'",
			expected: "hello world",
		},
		{
			name:     "String with escaped quotes",
			input:    "'it''s working'",
			expected: "it's working",
		},
		{
			name:     "Empty string",
			input:    "''",
			expected: "",
		},
		{
			name:    "Unterminated string",
			input:   "'hello",
			isError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			if tt.isError {
				if token.Type != TokenError {
					t.Errorf("Expected error token, got %v", token.Type)
				}
			} else {
				if token.Type != TokenString {
					t.Errorf("Expected string token, got %v", token.Type)
				}
				if token.Value != tt.expected {
					t.Errorf("Expected value %q, got %q", tt.expected, token.Value)
				}
			}
		})
	}
}

func TestLexerNumbers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Integer",
			input:    "123",
			expected: "123",
		},
		{
			name:     "Decimal",
			input:    "123.456",
			expected: "123.456",
		},
		{
			name:     "Zero",
			input:    "0",
			expected: "0",
		},
		{
			name:     "Decimal starting with zero",
			input:    "0.123",
			expected: "0.123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			if token.Type != TokenNumber {
				t.Errorf("Expected number token, got %v", token.Type)
			}
			if token.Value != tt.expected {
				t.Errorf("Expected value %q, got %q", tt.expected, token.Value)
			}
		})
	}
}

func TestLexerKeywords(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"SELECT", TokenSelect},
		{"select", TokenSelect},
		{"FROM", TokenFrom},
		{"WHERE", TokenWhere},
		{"AND", TokenAnd},
		{"OR", TokenOr},
		{"NOT", TokenNot},
		{"NULL", TokenNull},
		{"TRUE", TokenTrue},
		{"FALSE", TokenFalse},
		{"INTEGER", TokenInteger},
		{"VARCHAR", TokenVarchar},
		{"CREATE", TokenCreate},
		{"TABLE", TokenTable},
		{"INSERT", TokenInsert},
		{"INTO", TokenInto},
		{"VALUES", TokenValues},
		{"UPDATE", TokenUpdate},
		{"SET", TokenSet},
		{"DELETE", TokenDelete},
		{"DROP", TokenDrop},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			if token.Type != tt.expected {
				t.Errorf("Input %q: expected token type %v, got %v", tt.input, tt.expected, token.Type)
			}
		})
	}
}

func TestLexerQuotedIdentifiers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		isError  bool
	}{
		{
			name:     "Simple quoted identifier",
			input:    `"user name"`,
			expected: "user name",
		},
		{
			name:     "Quoted identifier with escaped quotes",
			input:    `"my""table"`,
			expected: `my"table`,
		},
		{
			name:    "Unterminated quoted identifier",
			input:   `"hello`,
			isError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			if tt.isError {
				if token.Type != TokenError {
					t.Errorf("Expected error token, got %v", token.Type)
				}
			} else {
				if token.Type != TokenIdentifier {
					t.Errorf("Expected identifier token, got %v", token.Type)
				}
				if token.Value != tt.expected {
					t.Errorf("Expected value %q, got %q", tt.expected, token.Value)
				}
			}
		})
	}
}

func TestLexerComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "Comment at end of line",
			input: "SELECT * -- This is a comment\nFROM users",
			expected: []Token{
				{Type: TokenSelect, Value: "SELECT"},
				{Type: TokenStar, Value: "*"},
				{Type: TokenFrom, Value: "FROM"},
				{Type: TokenIdentifier, Value: "users"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "Comment at beginning",
			input: "-- This is a comment\nSELECT *",
			expected: []Token{
				{Type: TokenSelect, Value: "SELECT"},
				{Type: TokenStar, Value: "*"},
				{Type: TokenEOF, Value: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			for i, expected := range tt.expected {
				token := lexer.NextToken()
				if token.Type != expected.Type {
					t.Errorf("Token %d: expected type %v, got %v", i, expected.Type, token.Type)
				}
				if token.Value != expected.Value {
					t.Errorf("Token %d: expected value %q, got %q", i, expected.Value, token.Value)
				}
			}
		})
	}
}

func TestLexerPositionTracking(t *testing.T) {
	input := "SELECT\n  * FROM\n  users"
	lexer := NewLexer(input)

	tests := []struct {
		expectedType   TokenType
		expectedLine   int
		expectedColumn int
	}{
		{TokenSelect, 1, 1},
		{TokenStar, 2, 3},
		{TokenFrom, 2, 5},
		{TokenIdentifier, 3, 3},
	}

	for i, tt := range tests {
		token := lexer.NextToken()
		if token.Type != tt.expectedType {
			t.Errorf("Token %d: expected type %v, got %v", i, tt.expectedType, token.Type)
		}
		if token.Line != tt.expectedLine {
			t.Errorf("Token %d: expected line %d, got %d", i, tt.expectedLine, token.Line)
		}
		if token.Column != tt.expectedColumn {
			t.Errorf("Token %d: expected column %d, got %d", i, tt.expectedColumn, token.Column)
		}
	}
}

func TestLexerOrderBy(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "ORDER BY as single token",
			input: "ORDER BY id",
			expected: []Token{
				{Type: TokenOrderBy, Value: "ORDER BY"},
				{Type: TokenIdentifier, Value: "id"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "ORDER without BY",
			input: "ORDER id",
			expected: []Token{
				{Type: TokenIdentifier, Value: "ORDER"},
				{Type: TokenIdentifier, Value: "id"},
				{Type: TokenEOF, Value: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			for i, expected := range tt.expected {
				token := lexer.NextToken()
				if token.Type != expected.Type {
					t.Errorf("Token %d: expected type %v, got %v", i, expected.Type, token.Type)
				}
				if token.Value != expected.Value {
					t.Errorf("Token %d: expected value %q, got %q", i, expected.Value, token.Value)
				}
			}
		})
	}
}

