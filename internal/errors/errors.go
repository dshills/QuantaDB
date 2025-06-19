package errors

import (
	"fmt"
)

// Error represents a PostgreSQL-compatible error with SQLSTATE code
type Error struct {
	Code             string // SQLSTATE code
	Message          string // Primary error message
	Detail           string // Optional detailed error message
	Hint             string // Optional hint message
	Position         int    // Character position in query (0 if not applicable)
	InternalPosition int    // Position in internal query
	InternalQuery    string // Internal query text
	Where            string // Context where error occurred
	Schema           string // Schema name if applicable
	Table            string // Table name if applicable
	Column           string // Column name if applicable
	DataType         string // Data type name if applicable
	Constraint       string // Constraint name if applicable
	File             string // Source code file where error occurred
	Line             int    // Source code line number
	Routine          string // Source code routine name
}

// Error implements the error interface
func (e *Error) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s (SQLSTATE %s) DETAIL: %s", e.Routine, e.Message, e.Code, e.Detail)
	}
	return fmt.Sprintf("%s: %s (SQLSTATE %s)", e.Routine, e.Message, e.Code)
}

// New creates a new Error with the given code and message
func New(code string, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

// Newf creates a new Error with a formatted message
func Newf(code string, format string, args ...interface{}) *Error {
	return &Error{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

// WithDetail adds detail to the error
func (e *Error) WithDetail(detail string) *Error {
	e.Detail = detail
	return e
}

// WithDetailf adds formatted detail to the error
func (e *Error) WithDetailf(format string, args ...interface{}) *Error {
	e.Detail = fmt.Sprintf(format, args...)
	return e
}

// WithHint adds a hint to the error
func (e *Error) WithHint(hint string) *Error {
	e.Hint = hint
	return e
}

// WithHintf adds a formatted hint to the error
func (e *Error) WithHintf(format string, args ...interface{}) *Error {
	e.Hint = fmt.Sprintf(format, args...)
	return e
}

// WithPosition sets the query position
func (e *Error) WithPosition(pos int) *Error {
	e.Position = pos
	return e
}

// WithTable sets the table name
func (e *Error) WithTable(schema, table string) *Error {
	e.Schema = schema
	e.Table = table
	return e
}

// WithColumn sets the column name
func (e *Error) WithColumn(column string) *Error {
	e.Column = column
	return e
}

// WithConstraint sets the constraint name
func (e *Error) WithConstraint(constraint string) *Error {
	e.Constraint = constraint
	return e
}

// WithDataType sets the data type name
func (e *Error) WithDataType(dataType string) *Error {
	e.DataType = dataType
	return e
}

// WithWhere sets the context where the error occurred
func (e *Error) WithWhere(where string) *Error {
	e.Where = where
	return e
}

// WithSource sets source code location info
func (e *Error) WithSource(file string, line int, routine string) *Error {
	e.File = file
	e.Line = line
	e.Routine = routine
	return e
}

// Common error constructors

// NewSyntaxError creates a syntax error
func NewSyntaxError(message string, position int) *Error {
	return New(SyntaxError, message).WithPosition(position)
}

// SyntaxErrorf creates a formatted syntax error
func SyntaxErrorf(position int, format string, args ...interface{}) *Error {
	return Newf(SyntaxError, format, args...).WithPosition(position)
}

// UndefinedTableError creates an undefined table error
func UndefinedTableError(tableName string) *Error {
	return Newf(UndefinedTable, "relation \"%s\" does not exist", tableName).
		WithTable("", tableName)
}

// UndefinedColumnError creates an undefined column error
func UndefinedColumnError(columnName string, tableName string) *Error {
	return Newf(UndefinedColumn, "column \"%s\" does not exist", columnName).
		WithTable("", tableName).
		WithColumn(columnName)
}

// DuplicateTableError creates a duplicate table error
func DuplicateTableError(tableName string) *Error {
	return Newf(DuplicateTable, "relation \"%s\" already exists", tableName).
		WithTable("", tableName)
}

// NotNullViolationError creates a not null violation error
func NotNullViolationError(columnName string, tableName string) *Error {
	return Newf(NotNullViolation, "null value in column \"%s\" violates not-null constraint", columnName).
		WithTable("", tableName).
		WithColumn(columnName)
}

// UniqueViolationError creates a unique violation error
func UniqueViolationError(value string, constraintName string) *Error {
	return Newf(UniqueViolation, "duplicate key value violates unique constraint \"%s\"", constraintName).
		WithConstraint(constraintName).
		WithDetailf("Key (%s) already exists.", value)
}

// ForeignKeyViolationError creates a foreign key violation error
func ForeignKeyViolationError(constraintName string, tableName string) *Error {
	return Newf(ForeignKeyViolation, "insert or update on table \"%s\" violates foreign key constraint \"%s\"", tableName, constraintName).
		WithTable("", tableName).
		WithConstraint(constraintName)
}

// DivisionByZeroError creates a division by zero error
func DivisionByZeroError() *Error {
	return New(DivisionByZero, "division by zero")
}

// DataTypeMismatchError creates a data type mismatch error
func DataTypeMismatchError(expected, actual string) *Error {
	return Newf(DatatypeMismatch, "column is of type %s but expression is of type %s", expected, actual).
		WithHintf("You will need to rewrite or cast the expression.")
}

// InsufficientPrivilegeError creates an insufficient privilege error
func InsufficientPrivilegeError(operation, objectType, objectName string) *Error {
	return Newf(InsufficientPrivilege, "permission denied for %s %s", objectType, objectName).
		WithDetailf("User does not have %s privilege.", operation)
}

// InvalidTextRepresentationError creates an invalid text representation error
func InvalidTextRepresentationError(dataType, value string) *Error {
	return Newf(InvalidTextRepresentation, "invalid input syntax for type %s: \"%s\"", dataType, value).
		WithDataType(dataType)
}

// NumericValueOutOfRangeError creates a numeric value out of range error
func NumericValueOutOfRangeError(dataType string) *Error {
	return Newf(NumericValueOutOfRange, "value out of range for type %s", dataType).
		WithDataType(dataType)
}

// StringDataRightTruncationError creates a string data right truncation error
func StringDataRightTruncationError(maxLen int) *Error {
	return Newf(StringDataRightTruncation, "value too long for type character varying(%d)", maxLen)
}

// InvalidTransactionStateError creates an invalid transaction state error
func InvalidTransactionStateError(message string) *Error {
	return New(InvalidTransactionState, message)
}

// DeadlockDetectedError creates a deadlock detected error
func DeadlockDetectedError() *Error {
	return New(DeadlockDetected, "deadlock detected").
		WithDetailf("Process was waiting for transaction to complete.").
		WithHint("See server log for query details.")
}

// OutOfMemoryError creates an out of memory error
func OutOfMemoryError(context string) *Error {
	return Newf(OutOfMemory, "out of memory").
		WithDetailf("Failed on request of size in %s.", context)
}

// DiskFullError creates a disk full error
func DiskFullError() *Error {
	return New(DiskFull, "could not write to file: No space left on device")
}

// IOError creates an I/O error
func IOErrorf(format string, args ...interface{}) *Error {
	return Newf(IOError, format, args...)
}

// InternalErrorf creates an internal error
func InternalErrorf(format string, args ...interface{}) *Error {
	return Newf(InternalError, format, args...)
}

// FeatureNotSupportedError creates a feature not supported error
func FeatureNotSupportedError(feature string) *Error {
	return Newf(FeatureNotSupported, "%s is not supported", feature)
}

// QueryCanceledError creates a query canceled error
func QueryCanceledError() *Error {
	return New(QueryCanceled, "canceling statement due to user request")
}

// IsError checks if an error is a QuantaDB Error with a specific code
func IsError(err error, code string) bool {
	if err == nil {
		return false
	}
	qErr, ok := err.(*Error)
	return ok && qErr.Code == code
}

// GetError attempts to extract a QuantaDB Error from any error
func GetError(err error) *Error {
	if err == nil {
		return nil
	}
	if qErr, ok := err.(*Error); ok {
		return qErr
	}
	// Wrap generic errors as internal errors
	return InternalErrorf("%v", err)
}
