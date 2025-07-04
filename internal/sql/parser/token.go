package parser

import "fmt"

// TokenType represents the type of a SQL token.
type TokenType int

const (
	// Special tokens.
	TokenEOF TokenType = iota
	TokenError

	// Literals.
	TokenIdentifier
	TokenNumber
	TokenString
	TokenTrue
	TokenFalse
	TokenNull
	TokenParam // Parameter placeholder like $1, $2

	// Keywords.
	TokenCreate
	TokenTable
	TokenInsert
	TokenInto
	TokenValues
	TokenSelect
	TokenFrom
	TokenWhere
	TokenAnd
	TokenOr
	TokenNot
	TokenDistinct
	TokenOrderBy
	TokenGroupBy
	TokenHaving
	TokenAsc
	TokenDesc
	TokenLimit
	TokenOffset
	TokenAs
	TokenFor
	TokenDrop
	TokenAlter
	TokenAdd
	TokenColumn
	TokenPrimary
	TokenKey
	TokenUnique
	TokenDefault
	TokenUpdate
	TokenSet
	TokenDelete
	TokenIndex
	TokenOn
	TokenUsing
	TokenAnalyze
	TokenVacuum
	TokenWith
	TokenCopy
	TokenTo
	TokenStdin
	TokenStdout
	TokenDelimiter
	TokenFormat
	TokenCsv
	TokenBinary
	TokenPrepare
	TokenExecute
	TokenDeallocate
	TokenExplain
	TokenInclude

	// Constraint tokens
	TokenForeign
	TokenReferences
	TokenCheck
	TokenConstraint
	TokenCascade
	TokenRestrict
	TokenNoAction
	TokenSetNull
	TokenSetDefault

	// Data types.
	TokenInteger
	TokenBigint
	TokenSmallint
	TokenVarchar
	TokenChar
	TokenText
	TokenBoolean
	TokenTimestamp
	TokenDate
	TokenDecimal
	TokenInterval
	TokenFloat
	TokenDouble
	TokenReal
	TokenPrecision
	TokenBytea

	// Functions and date parts
	TokenExtract
	TokenYear
	TokenMonth
	TokenDay
	TokenHour
	TokenMinute
	TokenSecond
	TokenSubstring

	// Operators
	TokenPlus
	TokenMinus
	TokenStar
	TokenSlash
	TokenPercent
	TokenEqual
	TokenNotEqual
	TokenLess
	TokenLessEqual
	TokenGreater
	TokenGreaterEqual
	TokenLike
	TokenIn
	TokenBetween
	TokenIs
	TokenExists
	TokenCase
	TokenWhen
	TokenThen
	TokenElse
	TokenEnd

	// JOIN tokens
	TokenJoin
	TokenInner
	TokenLeft
	TokenRight
	TokenFull
	TokenOuter
	TokenCross
	TokenNatural

	// Delimiters
	TokenLeftParen
	TokenRightParen
	TokenComma
	TokenSemicolon
	TokenDot
	TokenConcat // || operator
)

var tokenStrings = map[TokenType]string{
	TokenEOF:          "EOF",
	TokenError:        "ERROR",
	TokenIdentifier:   "IDENTIFIER",
	TokenNumber:       "NUMBER",
	TokenString:       "STRING",
	TokenTrue:         "TRUE",
	TokenFalse:        "FALSE",
	TokenNull:         "NULL",
	TokenParam:        "PARAM",
	TokenCreate:       "CREATE",
	TokenTable:        "TABLE",
	TokenInsert:       "INSERT",
	TokenInto:         "INTO",
	TokenValues:       "VALUES",
	TokenSelect:       "SELECT",
	TokenFrom:         "FROM",
	TokenWhere:        "WHERE",
	TokenAnd:          "AND",
	TokenOr:           "OR",
	TokenNot:          "NOT",
	TokenDistinct:     "DISTINCT",
	TokenOrderBy:      "ORDER BY",
	TokenGroupBy:      "GROUP BY",
	TokenHaving:       "HAVING",
	TokenAsc:          "ASC",
	TokenDesc:         "DESC",
	TokenLimit:        "LIMIT",
	TokenOffset:       "OFFSET",
	TokenAs:           "AS",
	TokenFor:          "FOR",
	TokenDrop:         "DROP",
	TokenAlter:        "ALTER",
	TokenAdd:          "ADD",
	TokenColumn:       "COLUMN",
	TokenPrimary:      "PRIMARY",
	TokenKey:          "KEY",
	TokenUnique:       "UNIQUE",
	TokenDefault:      "DEFAULT",
	TokenUpdate:       "UPDATE",
	TokenSet:          "SET",
	TokenDelete:       "DELETE",
	TokenIndex:        "INDEX",
	TokenOn:           "ON",
	TokenUsing:        "USING",
	TokenAnalyze:      "ANALYZE",
	TokenVacuum:       "VACUUM",
	TokenWith:         "WITH",
	TokenCopy:         "COPY",
	TokenTo:           "TO",
	TokenStdin:        "STDIN",
	TokenStdout:       "STDOUT",
	TokenDelimiter:    "DELIMITER",
	TokenFormat:       "FORMAT",
	TokenCsv:          "CSV",
	TokenBinary:       "BINARY",
	TokenPrepare:      "PREPARE",
	TokenExecute:      "EXECUTE",
	TokenDeallocate:   "DEALLOCATE",
	TokenExplain:      "EXPLAIN",
	TokenInclude:      "INCLUDE",
	TokenForeign:      "FOREIGN",
	TokenReferences:   "REFERENCES",
	TokenCheck:        "CHECK",
	TokenConstraint:   "CONSTRAINT",
	TokenCascade:      "CASCADE",
	TokenRestrict:     "RESTRICT",
	TokenNoAction:     "NO ACTION",
	TokenSetNull:      "SET NULL",
	TokenSetDefault:   "SET DEFAULT",
	TokenInteger:      "INTEGER",
	TokenBigint:       "BIGINT",
	TokenSmallint:     "SMALLINT",
	TokenVarchar:      "VARCHAR",
	TokenChar:         "CHAR",
	TokenText:         "TEXT",
	TokenBoolean:      "BOOLEAN",
	TokenTimestamp:    "TIMESTAMP",
	TokenDate:         "DATE",
	TokenDecimal:      "DECIMAL",
	TokenInterval:     "INTERVAL",
	TokenFloat:        "FLOAT",
	TokenDouble:       "DOUBLE",
	TokenReal:         "REAL",
	TokenPrecision:    "PRECISION",
	TokenBytea:        "BYTEA",
	TokenExtract:      "EXTRACT",
	TokenYear:         "YEAR",
	TokenMonth:        "MONTH",
	TokenDay:          "DAY",
	TokenHour:         "HOUR",
	TokenMinute:       "MINUTE",
	TokenSecond:       "SECOND",
	TokenSubstring:    "SUBSTRING",
	TokenPlus:         "+",
	TokenMinus:        "-",
	TokenStar:         "*",
	TokenSlash:        "/",
	TokenPercent:      "%",
	TokenEqual:        "=",
	TokenNotEqual:     "!=",
	TokenLess:         "<",
	TokenLessEqual:    "<=",
	TokenGreater:      ">",
	TokenGreaterEqual: ">=",
	TokenLike:         "LIKE",
	TokenIn:           "IN",
	TokenBetween:      "BETWEEN",
	TokenIs:           "IS",
	TokenExists:       "EXISTS",
	TokenCase:         "CASE",
	TokenWhen:         "WHEN",
	TokenThen:         "THEN",
	TokenElse:         "ELSE",
	TokenEnd:          "END",
	TokenJoin:         "JOIN",
	TokenInner:        "INNER",
	TokenLeft:         "LEFT",
	TokenRight:        "RIGHT",
	TokenFull:         "FULL",
	TokenOuter:        "OUTER",
	TokenCross:        "CROSS",
	TokenNatural:      "NATURAL",
	TokenLeftParen:    "(",
	TokenRightParen:   ")",
	TokenComma:        ",",
	TokenSemicolon:    ";",
	TokenDot:          ".",
	TokenConcat:       "||",
}

// String returns the string representation of a token type.
func (t TokenType) String() string {
	if s, ok := tokenStrings[t]; ok {
		return s
	}
	return fmt.Sprintf("Unknown(%d)", t)
}

// Token represents a SQL token.
type Token struct {
	Type     TokenType
	Value    string
	Position int
	Line     int
	Column   int
}

// String returns a string representation of the token.
func (t Token) String() string {
	if t.Type == TokenIdentifier || t.Type == TokenNumber || t.Type == TokenString || t.Type == TokenParam {
		return fmt.Sprintf("%s(%s)", t.Type, t.Value)
	}
	return t.Type.String()
}

// Keywords maps keyword strings to token types.
var keywords = map[string]TokenType{
	"CREATE":     TokenCreate,
	"TABLE":      TokenTable,
	"INSERT":     TokenInsert,
	"INTO":       TokenInto,
	"VALUES":     TokenValues,
	"SELECT":     TokenSelect,
	"FROM":       TokenFrom,
	"WHERE":      TokenWhere,
	"AND":        TokenAnd,
	"OR":         TokenOr,
	"NOT":        TokenNot,
	"DISTINCT":   TokenDistinct,
	"ORDER":      TokenOrderBy,
	"BY":         TokenOrderBy,
	"GROUP":      TokenGroupBy,
	"HAVING":     TokenHaving,
	"ASC":        TokenAsc,
	"DESC":       TokenDesc,
	"LIMIT":      TokenLimit,
	"OFFSET":     TokenOffset,
	"AS":         TokenAs,
	"FOR":        TokenFor,
	"DROP":       TokenDrop,
	"ALTER":      TokenAlter,
	"ADD":        TokenAdd,
	"COLUMN":     TokenColumn,
	"PRIMARY":    TokenPrimary,
	"KEY":        TokenKey,
	"UNIQUE":     TokenUnique,
	"DEFAULT":    TokenDefault,
	"UPDATE":     TokenUpdate,
	"SET":        TokenSet,
	"DELETE":     TokenDelete,
	"INDEX":      TokenIndex,
	"ON":         TokenOn,
	"USING":      TokenUsing,
	"ANALYZE":    TokenAnalyze,
	"INTEGER":    TokenInteger,
	"INT":        TokenInteger,
	"BIGINT":     TokenBigint,
	"SMALLINT":   TokenSmallint,
	"VARCHAR":    TokenVarchar,
	"CHAR":       TokenChar,
	"TEXT":       TokenText,
	"BOOLEAN":    TokenBoolean,
	"BOOL":       TokenBoolean,
	"TIMESTAMP":  TokenTimestamp,
	"DATE":       TokenDate,
	"DECIMAL":    TokenDecimal,
	"INTERVAL":   TokenInterval,
	"NUMERIC":    TokenDecimal,
	"EXTRACT":    TokenExtract,
	"YEAR":       TokenYear,
	"MONTH":      TokenMonth,
	"DAY":        TokenDay,
	"HOUR":       TokenHour,
	"MINUTE":     TokenMinute,
	"SECOND":     TokenSecond,
	"SUBSTRING":  TokenSubstring,
	"TRUE":       TokenTrue,
	"FALSE":      TokenFalse,
	"NULL":       TokenNull,
	"LIKE":       TokenLike,
	"IN":         TokenIn,
	"BETWEEN":    TokenBetween,
	"IS":         TokenIs,
	"EXISTS":     TokenExists,
	"VACUUM":     TokenVacuum,
	"WITH":       TokenWith,
	"COPY":       TokenCopy,
	"TO":         TokenTo,
	"STDIN":      TokenStdin,
	"STDOUT":     TokenStdout,
	"DELIMITER":  TokenDelimiter,
	"FORMAT":     TokenFormat,
	"CSV":        TokenCsv,
	"BINARY":     TokenBinary,
	"PREPARE":    TokenPrepare,
	"EXECUTE":    TokenExecute,
	"DEALLOCATE": TokenDeallocate,
	"EXPLAIN":    TokenExplain,
	"INCLUDE":    TokenInclude,
	"FLOAT":      TokenFloat,
	"DOUBLE":     TokenDouble,
	"REAL":       TokenReal,
	"PRECISION":  TokenPrecision,
	"BYTEA":      TokenBytea,
	"FOREIGN":    TokenForeign,
	"REFERENCES": TokenReferences,
	"CHECK":      TokenCheck,
	"CONSTRAINT": TokenConstraint,
	"CASCADE":    TokenCascade,
	"RESTRICT":   TokenRestrict,
	"CASE":       TokenCase,
	"WHEN":       TokenWhen,
	"THEN":       TokenThen,
	"ELSE":       TokenElse,
	"END":        TokenEnd,
	"JOIN":       TokenJoin,
	"INNER":      TokenInner,
	"LEFT":       TokenLeft,
	"RIGHT":      TokenRight,
	"FULL":       TokenFull,
	"OUTER":      TokenOuter,
	"CROSS":      TokenCross,
	"NATURAL":    TokenNatural,
}

// LookupKeyword returns the token type for a keyword.
func LookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TokenIdentifier
}
