package parser

// ParseError represents a parse error with position information
type ParseError struct {
	Msg    string
	Line   int
	Column int
}

// Error implements the error interface
func (e *ParseError) Error() string {
	return e.Msg
}

// NewParseError creates a new parse error
func NewParseError(msg string, line, column int) *ParseError {
	return &ParseError{
		Msg:    msg,
		Line:   line,
		Column: column,
	}
}
