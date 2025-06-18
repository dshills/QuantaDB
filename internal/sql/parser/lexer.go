package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// Lexer tokenizes SQL input.
type Lexer struct {
	input    string
	position int
	line     int
	column   int
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		line:   1,
		column: 1,
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.position >= len(l.input) {
		return l.makeToken(TokenEOF, "")
	}

	ch := l.input[l.position]

	// Handle single-character tokens
	switch ch {
	case '(':
		return l.consumeChar(TokenLeftParen)
	case ')':
		return l.consumeChar(TokenRightParen)
	case ',':
		return l.consumeChar(TokenComma)
	case ';':
		return l.consumeChar(TokenSemicolon)
	case '.':
		return l.consumeChar(TokenDot)
	case '+':
		return l.consumeChar(TokenPlus)
	case '-':
		// Could be minus or start of comment
		if l.peek(1) == '-' {
			l.skipComment()
			return l.NextToken()
		}
		return l.consumeChar(TokenMinus)
	case '*':
		return l.consumeChar(TokenStar)
	case '/':
		return l.consumeChar(TokenSlash)
	case '%':
		return l.consumeChar(TokenPercent)
	case '=':
		return l.consumeChar(TokenEqual)
	case '<':
		if l.peek(1) == '=' {
			return l.consumeChars(TokenLessEqual, 2)
		}
		if l.peek(1) == '>' {
			return l.consumeChars(TokenNotEqual, 2)
		}
		return l.consumeChar(TokenLess)
	case '>':
		if l.peek(1) == '=' {
			return l.consumeChars(TokenGreaterEqual, 2)
		}
		return l.consumeChar(TokenGreater)
	case '!':
		if l.peek(1) == '=' {
			return l.consumeChars(TokenNotEqual, 2)
		}
		return l.makeToken(TokenError, "unexpected character '!'")
	case '\'':
		return l.readString()
	case '"':
		return l.readQuotedIdentifier()
	}

	// Handle multi-character tokens
	if unicode.IsLetter(rune(ch)) || ch == '_' {
		return l.readIdentifier()
	}

	if unicode.IsDigit(rune(ch)) {
		return l.readNumber()
	}

	return l.makeToken(TokenError, fmt.Sprintf("unexpected character '%c'", ch))
}

// skipWhitespace skips whitespace and updates line/column tracking.
func (l *Lexer) skipWhitespace() {
	for l.position < len(l.input) {
		ch := l.input[l.position]
		switch ch {
		case ' ', '\t', '\r':
			l.position++
			l.column++
		case '\n':
			l.position++
			l.line++
			l.column = 1
		default:
			return
		}
	}
}

// skipComment skips SQL comments (-- to end of line).
func (l *Lexer) skipComment() {
	for l.position < len(l.input) && l.input[l.position] != '\n' {
		l.position++
		l.column++
	}
}

// peek looks ahead n characters without consuming.
func (l *Lexer) peek(n int) byte {
	pos := l.position + n
	if pos >= len(l.input) {
		return 0
	}
	return l.input[pos]
}

// consumeChar consumes a single character and returns a token.
func (l *Lexer) consumeChar(tokenType TokenType) Token {
	tok := l.makeToken(tokenType, string(l.input[l.position]))
	l.position++
	l.column++
	return tok
}

// consumeChars consumes n characters and returns a token.
func (l *Lexer) consumeChars(tokenType TokenType, n int) Token {
	value := l.input[l.position : l.position+n]
	tok := l.makeToken(tokenType, value)
	l.position += n
	l.column += n
	return tok
}

// makeToken creates a token at the current position.
func (l *Lexer) makeToken(tokenType TokenType, value string) Token {
	return Token{
		Type:     tokenType,
		Value:    value,
		Position: l.position,
		Line:     l.line,
		Column:   l.column,
	}
}

// readIdentifier reads an identifier or keyword.
func (l *Lexer) readIdentifier() Token {
	start := l.position
	startCol := l.column

	for l.position < len(l.input) {
		ch := l.input[l.position]
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' {
			l.position++
			l.column++
		} else {
			break
		}
	}

	value := l.input[start:l.position]
	tokenType := LookupKeyword(strings.ToUpper(value))

	// Special handling for ORDER BY
	if strings.ToUpper(value) == "ORDER" {
		l.skipWhitespace()
		if l.position < len(l.input)-1 && strings.ToUpper(l.input[l.position:l.position+2]) == "BY" {
			l.position += 2
			l.column += 2
			tokenType = TokenOrderBy
			value = "ORDER BY"
		} else {
			tokenType = TokenIdentifier
		}
	}

	return Token{
		Type:     tokenType,
		Value:    value,
		Position: start,
		Line:     l.line,
		Column:   startCol,
	}
}

// readNumber reads a numeric literal.
func (l *Lexer) readNumber() Token {
	start := l.position
	startCol := l.column
	hasDecimal := false

	for l.position < len(l.input) {
		ch := l.input[l.position]
		if unicode.IsDigit(rune(ch)) {
			l.position++
			l.column++
		} else if ch == '.' && !hasDecimal && l.position+1 < len(l.input) && unicode.IsDigit(rune(l.input[l.position+1])) {
			hasDecimal = true
			l.position++
			l.column++
		} else {
			break
		}
	}

	value := l.input[start:l.position]
	return Token{
		Type:     TokenNumber,
		Value:    value,
		Position: start,
		Line:     l.line,
		Column:   startCol,
	}
}

// readQuoted reads a quoted string with the given quote character.
func (l *Lexer) readQuoted(quoteChar byte, tokenType TokenType, errorMsg string) Token {
	start := l.position
	startCol := l.column
	l.position++ // Skip opening quote
	l.column++

	var builder strings.Builder

	for l.position < len(l.input) {
		ch := l.input[l.position]
		switch ch {
		case quoteChar:
			// Check for escaped quote
			if l.peek(1) == quoteChar {
				builder.WriteByte(quoteChar)
				l.position += 2
				l.column += 2
			} else {
				// End of quoted string
				l.position++
				l.column++
				return Token{
					Type:     tokenType,
					Value:    builder.String(),
					Position: start,
					Line:     l.line,
					Column:   startCol,
				}
			}
		case '\n':
			return Token{
				Type:     TokenError,
				Value:    errorMsg,
				Position: start,
				Line:     l.line,
				Column:   startCol,
			}
		default:
			builder.WriteByte(ch)
			l.position++
			l.column++
		}
	}

	return Token{
		Type:     TokenError,
		Value:    errorMsg,
		Position: start,
		Line:     l.line,
		Column:   startCol,
	}
}

// readString reads a string literal enclosed in single quotes.
func (l *Lexer) readString() Token {
	return l.readQuoted('\'', TokenString, "unterminated string literal")
}

// readQuotedIdentifier reads an identifier enclosed in double quotes.
func (l *Lexer) readQuotedIdentifier() Token {
	return l.readQuoted('"', TokenIdentifier, "unterminated quoted identifier")
}
