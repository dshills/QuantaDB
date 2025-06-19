package test

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/errors"
	"github.com/dshills/QuantaDB/internal/sql/parser"
)

func TestPostgreSQLErrorCodes(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode string
		wantMsg  string
	}{
		{
			name:     "syntax error",
			err:      errors.NewSyntaxError("unexpected token", 10),
			wantCode: errors.SyntaxError,
			wantMsg:  "unexpected token",
		},
		{
			name:     "undefined table",
			err:      errors.UndefinedTableError("users"),
			wantCode: errors.UndefinedTable,
			wantMsg:  `relation "users" does not exist`,
		},
		{
			name:     "undefined column",
			err:      errors.UndefinedColumnError("email", "users"),
			wantCode: errors.UndefinedColumn,
			wantMsg:  `column "email" does not exist`,
		},
		{
			name:     "duplicate table",
			err:      errors.DuplicateTableError("users"),
			wantCode: errors.DuplicateTable,
			wantMsg:  `relation "users" already exists`,
		},
		{
			name:     "not null violation",
			err:      errors.NotNullViolationError("email", "users"),
			wantCode: errors.NotNullViolation,
			wantMsg:  `null value in column "email" violates not-null constraint`,
		},
		{
			name:     "unique violation",
			err:      errors.UniqueViolationError("john@example.com", "users_email_key"),
			wantCode: errors.UniqueViolation,
			wantMsg:  `duplicate key value violates unique constraint "users_email_key"`,
		},
		{
			name:     "division by zero",
			err:      errors.DivisionByZeroError(),
			wantCode: errors.DivisionByZero,
			wantMsg:  "division by zero",
		},
		{
			name:     "transaction already active",
			err:      errors.TransactionAlreadyActiveError(),
			wantCode: errors.ActiveSQLTransaction,
			wantMsg:  "there is already a transaction in progress",
		},
		{
			name:     "no active transaction",
			err:      errors.NoActiveTransactionError(),
			wantCode: errors.NoActiveSQLTransaction,
			wantMsg:  "there is no transaction in progress",
		},
		{
			name:     "feature not supported",
			err:      errors.FeatureNotSupportedError("window functions"),
			wantCode: errors.FeatureNotSupported,
			wantMsg:  "window functions is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qErr, ok := tt.err.(*errors.Error)
			if !ok {
				t.Fatalf("expected *errors.Error, got %T", tt.err)
			}

			if qErr.Code != tt.wantCode {
				t.Errorf("code = %v, want %v", qErr.Code, tt.wantCode)
			}

			if qErr.Message != tt.wantMsg {
				t.Errorf("message = %v, want %v", qErr.Message, tt.wantMsg)
			}
		})
	}
}

func TestParserErrorConversion(t *testing.T) {
	// Test that parser errors are properly converted
	p := parser.NewParser("SELECT * FROM")
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected parse error")
	}

	// The error should be a ParseError
	pErr, ok := err.(*parser.ParseError)
	if !ok {
		t.Fatalf("expected *parser.ParseError, got %T", err)
	}

	if pErr.Line == 0 {
		t.Error("expected line number > 0")
	}
}

func TestErrorDetails(t *testing.T) {
	// Test error with details
	err := errors.UniqueViolationError("value", "constraint_name").
		WithTable("public", "users").
		WithColumn("email").
		WithDetail("Key (email)=(test@example.com) already exists.").
		WithHint("Use a different email address.")

	qErr := err

	if qErr.Schema != "public" {
		t.Errorf("schema = %v, want public", qErr.Schema)
	}
	if qErr.Table != "users" {
		t.Errorf("table = %v, want users", qErr.Table)
	}
	if qErr.Column != "email" {
		t.Errorf("column = %v, want email", qErr.Column)
	}
	if qErr.Detail != "Key (email)=(test@example.com) already exists." {
		t.Errorf("detail = %v", qErr.Detail)
	}
	if qErr.Hint != "Use a different email address." {
		t.Errorf("hint = %v", qErr.Hint)
	}
}

func TestIsError(t *testing.T) {
	err := errors.DivisionByZeroError()

	if !errors.IsError(err, errors.DivisionByZero) {
		t.Error("IsError should return true for matching code")
	}

	if errors.IsError(err, errors.SyntaxError) {
		t.Error("IsError should return false for non-matching code")
	}

	if errors.IsError(nil, errors.DivisionByZero) {
		t.Error("IsError should return false for nil error")
	}
}

func TestGetError(t *testing.T) {
	// Test with QuantaDB error
	qErr := errors.NewSyntaxError("test", 5)
	got := errors.GetError(qErr)
	if got != qErr {
		t.Error("GetError should return same error for QuantaDB errors")
	}

	// Test with generic error
	genericErr := errors.New("XX000", "generic error")
	got = errors.GetError(genericErr)
	if got.Code != "XX000" {
		t.Errorf("GetError code = %v, want XX000", got.Code)
	}

	// Test with nil
	got = errors.GetError(nil)
	if got != nil {
		t.Error("GetError should return nil for nil input")
	}
}
