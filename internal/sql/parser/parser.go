package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Parser parses SQL statements from tokens.
type Parser struct {
	lexer    *Lexer
	current  Token
	previous Token
	errors   []error
}

// NewParser creates a new parser for the given SQL input.
func NewParser(sql string) *Parser {
	lexer := NewLexer(sql)
	parser := &Parser{
		lexer:  lexer,
		errors: []error{},
	}
	parser.advance()
	return parser
}

// Parse parses a SQL statement.
func (p *Parser) Parse() (Statement, error) {
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}

	// Expect EOF or semicolon
	if !p.check(TokenEOF) && !p.check(TokenSemicolon) {
		return nil, p.error(fmt.Sprintf("unexpected token %s", p.current))
	}

	return stmt, nil
}

// ParseExpression parses a single expression (for testing).
func (p *Parser) ParseExpression() (Expression, error) {
	p.advance() // Start with the first token
	return p.parseExpression()
}

// ParseMultiple parses multiple SQL statements separated by semicolons.
func (p *Parser) ParseMultiple() ([]Statement, error) {
	var statements []Statement

	for !p.check(TokenEOF) {
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		statements = append(statements, stmt)

		// Consume optional semicolon
		if p.match(TokenSemicolon) {
			continue
		}

		// If not at EOF, there's an error
		if !p.check(TokenEOF) {
			return nil, p.error(fmt.Sprintf("unexpected token %s", p.current))
		}
	}

	return statements, nil
}

// parseStatement parses a single SQL statement.
func (p *Parser) parseStatement() (Statement, error) {
	switch p.current.Type { //nolint:exhaustive
	case TokenCreate:
		return p.parseCreate()
	case TokenInsert:
		return p.parseInsert()
	case TokenSelect:
		return p.parseSelect()
	case TokenWith:
		return p.parseWithSelect() // CTE SELECT statement
	case TokenUpdate:
		return p.parseUpdate()
	case TokenDelete:
		return p.parseDelete()
	case TokenDrop:
		return p.parseDrop()
	case TokenAlter:
		return p.parseAlter()
	case TokenAnalyze:
		return p.parseAnalyze()
	case TokenVacuum:
		return p.parseVacuum()
	case TokenCopy:
		return p.parseCopy()
	case TokenPrepare:
		return p.parsePrepare()
	case TokenExecute:
		return p.parseExecute()
	case TokenDeallocate:
		return p.parseDeallocate()
	default:
		return nil, p.error(fmt.Sprintf("unexpected statement start: %s", p.current))
	}
}

// parseCreate parses CREATE statements (TABLE, INDEX, etc.).
func (p *Parser) parseCreate() (Statement, error) {
	if !p.consume(TokenCreate, "expected CREATE") {
		return nil, p.lastError()
	}

	switch p.current.Type { //nolint:exhaustive
	case TokenTable:
		return p.parseCreateTable()
	case TokenIndex:
		return p.parseCreateIndex()
	case TokenUnique:
		// Handle CREATE UNIQUE INDEX
		p.advance() // consume UNIQUE
		if !p.consume(TokenIndex, "expected INDEX after UNIQUE") {
			return nil, p.lastError()
		}
		return p.parseCreateIndexWithUnique(true)
	default:
		return nil, p.error(fmt.Sprintf("unexpected token after CREATE: %s", p.current))
	}
}

// parseCreateTable parses a CREATE TABLE statement.
func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	if !p.consume(TokenTable, "expected TABLE") {
		return nil, p.lastError()
	}

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	if !p.consume(TokenLeftParen, "expected '('") {
		return nil, p.lastError()
	}

	// Parse column definitions and constraints
	var columns []ColumnDef
	var constraints []TableConstraint

	for !p.check(TokenRightParen) {
		// Check if this is a table constraint
		if p.check(TokenPrimary) || p.check(TokenUnique) || p.check(TokenForeign) || p.check(TokenCheck) || p.check(TokenConstraint) {
			constraint, err := p.parseTableConstraint()
			if err != nil {
				return nil, err
			}
			constraints = append(constraints, constraint)
		} else {
			// Parse column definition
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			columns = append(columns, col)
		}

		// Check for comma or end of list
		if !p.match(TokenComma) {
			break
		}
	}

	if !p.consume(TokenRightParen, "expected ')'") {
		return nil, p.lastError()
	}

	return &CreateTableStmt{
		TableName:   tableName,
		Columns:     columns,
		Constraints: constraints,
	}, nil
}

// parseColumnDef parses a column definition.
func (p *Parser) parseColumnDef() (ColumnDef, error) {
	// Get column name
	name := p.current.Value
	if !p.consume(TokenIdentifier, "expected column name") {
		return ColumnDef{}, p.lastError()
	}

	// Get data type
	dataType, err := p.parseDataType()
	if err != nil {
		return ColumnDef{}, err
	}

	// Parse column constraints
	var constraints []ColumnConstraint
	for {
		constraint, ok := p.parseColumnConstraint()
		if !ok {
			break
		}
		constraints = append(constraints, constraint)
	}

	return ColumnDef{
		Name:        name,
		DataType:    dataType,
		Constraints: constraints,
	}, nil
}

// parseDataType parses a data type.
func (p *Parser) parseDataType() (types.DataType, error) {
	switch p.current.Type { //nolint:exhaustive
	case TokenInteger:
		p.advance()
		return types.Integer, nil
	case TokenBigint:
		p.advance()
		return types.BigInt, nil
	case TokenSmallint:
		p.advance()
		return types.SmallInt, nil
	case TokenBoolean:
		p.advance()
		return types.Boolean, nil
	case TokenText:
		p.advance()
		return types.Text, nil
	case TokenTimestamp:
		p.advance()
		return types.Timestamp, nil
	case TokenDate:
		p.advance()
		return types.Date, nil
	case TokenVarchar:
		p.advance()
		// Check for length parameter
		if p.match(TokenLeftParen) {
			if p.current.Type != TokenNumber {
				return nil, p.error("expected number for VARCHAR length")
			}
			length, err := strconv.Atoi(p.current.Value)
			if err != nil {
				return nil, p.error("invalid VARCHAR length")
			}
			p.advance()
			if !p.consume(TokenRightParen, "expected ')'") {
				return nil, p.lastError()
			}
			return types.Varchar(length), nil
		}
		// Default VARCHAR with no length limit
		return types.Varchar(0), nil
	case TokenChar:
		p.advance()
		// Check for length parameter
		if p.match(TokenLeftParen) {
			if p.current.Type != TokenNumber {
				return nil, p.error("expected number for CHAR length")
			}
			length, err := strconv.Atoi(p.current.Value)
			if err != nil {
				return nil, p.error("invalid CHAR length")
			}
			p.advance()
			if !p.consume(TokenRightParen, "expected ')'") {
				return nil, p.lastError()
			}
			return types.Char(length), nil
		}
		// Default CHAR(1)
		return types.Char(1), nil
	case TokenDecimal:
		p.advance()
		// Check for precision and scale
		if p.match(TokenLeftParen) {
			if p.current.Type != TokenNumber {
				return nil, p.error("expected number for DECIMAL precision")
			}
			precision, err := strconv.Atoi(p.current.Value)
			if err != nil {
				return nil, p.error("invalid DECIMAL precision")
			}
			p.advance()

			scale := 0
			if p.match(TokenComma) {
				if p.current.Type != TokenNumber {
					return nil, p.error("expected number for DECIMAL scale")
				}
				scale, err = strconv.Atoi(p.current.Value)
				if err != nil {
					return nil, p.error("invalid DECIMAL scale")
				}
				p.advance()
			}

			if !p.consume(TokenRightParen, "expected ')'") {
				return nil, p.lastError()
			}
			return types.Decimal(precision, scale), nil
		}
		// Default DECIMAL
		return types.Decimal(10, 0), nil
	case TokenFloat:
		p.advance()
		return types.Float, nil
	case TokenReal:
		p.advance()
		return types.Float, nil // REAL is an alias for FLOAT
	case TokenDouble:
		p.advance()
		// Check for PRECISION keyword
		if p.current.Type == TokenPrecision {
			p.advance()
		}
		return types.Double, nil
	case TokenBytea:
		p.advance()
		return types.Bytea, nil
	default:
		return nil, p.error(fmt.Sprintf("expected data type, got %s", p.current))
	}
}

// parseColumnConstraint parses a column constraint.
func (p *Parser) parseColumnConstraint() (ColumnConstraint, bool) {
	switch p.current.Type { //nolint:exhaustive
	case TokenNot:
		if p.peek(TokenNull) {
			p.advance() // consume NOT
			p.advance() // consume NULL
			return NotNullConstraint{}, true
		}
		return nil, false
	case TokenPrimary:
		if p.peek(TokenKey) {
			p.advance() // consume PRIMARY
			p.advance() // consume KEY
			return PrimaryKeyConstraint{}, true
		}
		return nil, false
	case TokenUnique:
		p.advance()
		return UniqueConstraint{}, true
	case TokenDefault:
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			// Return false to indicate no constraint was parsed
			return nil, false
		}
		return DefaultConstraint{Value: expr}, true
	default:
		return nil, false
	}
}

// parseTableConstraint parses a table-level constraint.
func (p *Parser) parseTableConstraint() (TableConstraint, error) {
	// Check for optional CONSTRAINT name
	var constraintName string
	if p.match(TokenConstraint) {
		if p.current.Type != TokenIdentifier {
			return nil, p.error("expected constraint name")
		}
		constraintName = p.current.Value
		p.advance()
	}

	// PRIMARY KEY constraint
	if p.match(TokenPrimary) {
		if !p.consume(TokenKey, "expected KEY after PRIMARY") {
			return nil, p.lastError()
		}

		if !p.consume(TokenLeftParen, "expected '('") {
			return nil, p.lastError()
		}

		// Parse column list
		columns, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}

		return TablePrimaryKeyConstraint{Columns: columns}, nil
	}

	// UNIQUE constraint
	if p.match(TokenUnique) {
		if !p.consume(TokenLeftParen, "expected '('") {
			return nil, p.lastError()
		}

		// Parse column list
		columns, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}

		return TableUniqueConstraint{Columns: columns}, nil
	}

	// FOREIGN KEY constraint
	if p.match(TokenForeign) {
		if !p.consume(TokenKey, "expected KEY after FOREIGN") {
			return nil, p.lastError()
		}

		if !p.consume(TokenLeftParen, "expected '('") {
			return nil, p.lastError()
		}

		// Parse local columns
		columns, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}

		if !p.consume(TokenReferences, "expected REFERENCES") {
			return nil, p.lastError()
		}

		// Parse referenced table
		if p.current.Type != TokenIdentifier {
			return nil, p.error("expected table name")
		}
		refTable := p.current.Value
		p.advance()

		// Parse referenced columns (optional)
		var refColumns []string
		if p.match(TokenLeftParen) {
			refColumns, err = p.parseIdentifierList()
			if err != nil {
				return nil, err
			}

			if !p.consume(TokenRightParen, "expected ')'") {
				return nil, p.lastError()
			}
		}

		// Parse ON DELETE/UPDATE actions (optional)
		var onDelete, onUpdate string
		for {
			if p.match(TokenOn) {
				if p.match(TokenDelete) {
					onDelete, err = p.parseReferentialAction()
					if err != nil {
						return nil, err
					}
				} else if p.match(TokenUpdate) {
					onUpdate, err = p.parseReferentialAction()
					if err != nil {
						return nil, err
					}
				} else {
					return nil, p.error("expected DELETE or UPDATE after ON")
				}
			} else {
				break
			}
		}

		return TableForeignKeyConstraint{
			Name:       constraintName,
			Columns:    columns,
			RefTable:   refTable,
			RefColumns: refColumns,
			OnDelete:   onDelete,
			OnUpdate:   onUpdate,
		}, nil
	}

	// CHECK constraint
	if p.match(TokenCheck) {
		if !p.consume(TokenLeftParen, "expected '('") {
			return nil, p.lastError()
		}

		// Parse the check expression
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}

		return TableCheckConstraint{
			Name:       constraintName,
			Expression: expr,
		}, nil
	}

	return nil, p.error("unsupported table constraint")
}

// parseReferentialAction parses ON DELETE/UPDATE actions.
func (p *Parser) parseReferentialAction() (string, error) {
	if p.match(TokenCascade) {
		return "CASCADE", nil
	}
	if p.match(TokenRestrict) {
		return "RESTRICT", nil
	}
	if p.match(TokenSet) {
		if p.match(TokenNull) {
			return "SET NULL", nil
		}
		if p.match(TokenDefault) {
			return "SET DEFAULT", nil
		}
		return "", p.error("expected NULL or DEFAULT after SET")
	}
	// Check for NO ACTION (two tokens)
	if p.current.Type == TokenIdentifier && p.current.Value == "NO" {
		p.advance()
		if p.current.Type == TokenIdentifier && p.current.Value == "ACTION" {
			p.advance()
			return "NO ACTION", nil
		}
		return "", p.error("expected ACTION after NO")
	}
	return "", p.error("expected CASCADE, RESTRICT, SET NULL, SET DEFAULT, or NO ACTION")
}

// parseInsert parses an INSERT statement.
func (p *Parser) parseInsert() (*InsertStmt, error) {
	if !p.consume(TokenInsert, "expected INSERT") {
		return nil, p.lastError()
	}

	if !p.consume(TokenInto, "expected INTO") {
		return nil, p.lastError()
	}

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	// Parse optional column list
	var columns []string
	if p.match(TokenLeftParen) {
		for {
			if p.current.Type != TokenIdentifier {
				return nil, p.error("expected column name")
			}
			columns = append(columns, p.current.Value)
			p.advance()

			if !p.match(TokenComma) {
				break
			}
		}

		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}
	}

	if !p.consume(TokenValues, "expected VALUES") {
		return nil, p.lastError()
	}

	// Parse value lists
	var values [][]Expression
	for {
		if !p.consume(TokenLeftParen, "expected '('") {
			return nil, p.lastError()
		}

		var valueSet []Expression
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			valueSet = append(valueSet, expr)

			if !p.match(TokenComma) {
				break
			}
		}

		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}

		values = append(values, valueSet)

		if !p.match(TokenComma) {
			break
		}
	}

	return &InsertStmt{
		TableName: tableName,
		Columns:   columns,
		Values:    values,
	}, nil
}

// parseSelect parses a SELECT statement.
// parseWithSelect parses a SELECT statement that starts with a WITH clause.
func (p *Parser) parseWithSelect() (*SelectStmt, error) {
	if !p.consume(TokenWith, "expected WITH") {
		return nil, p.lastError()
	}

	// Parse CTE definitions
	var ctes []CommonTableExpr
	for {
		// Parse CTE name
		if p.current.Type != TokenIdentifier {
			return nil, p.error("expected CTE name")
		}
		cteName := p.current.Value
		p.advance()

		// Expect AS
		if !p.consume(TokenAs, "expected AS") {
			return nil, p.lastError()
		}

		// Expect opening parenthesis
		if !p.consume(TokenLeftParen, "expected '('") {
			return nil, p.lastError()
		}

		// Parse the CTE query (recursive call to parseSelect)
		cteQuery, err := p.parseSelect()
		if err != nil {
			return nil, err
		}

		// Expect closing parenthesis
		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}

		ctes = append(ctes, CommonTableExpr{
			Name:  cteName,
			Query: cteQuery,
		})

		// Check for more CTEs
		if !p.match(TokenComma) {
			break
		}
	}

	// Now parse the main SELECT statement
	mainQuery, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	// Add CTEs to the main query
	mainQuery.With = ctes
	return mainQuery, nil
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	if !p.consume(TokenSelect, "expected SELECT") {
		return nil, p.lastError()
	}

	// Check for DISTINCT
	distinct := false
	if p.match(TokenDistinct) {
		distinct = true
	}

	// Parse select columns
	var columns []SelectColumn
	for {
		if p.check(TokenStar) {
			p.advance()
			columns = append(columns, SelectColumn{Expr: &Star{}})
		} else {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}

			col := SelectColumn{Expr: expr}

			// Check for alias
			if p.match(TokenAs) {
				if p.current.Type != TokenIdentifier {
					return nil, p.error("expected alias name")
				}
				col.Alias = p.current.Value
				p.advance()
			}

			columns = append(columns, col)
		}

		if !p.match(TokenComma) {
			break
		}
	}

	stmt := &SelectStmt{
		Distinct: distinct,
		Columns:  columns,
	}

	// Check for optional FROM clause
	if p.match(TokenFrom) {
		tableExpr, err := p.parseTableExpression()
		if err != nil {
			return nil, err
		}
		stmt.From = tableExpr
	}

	// Parse optional WHERE clause
	if p.match(TokenWhere) {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// Parse optional GROUP BY clause
	if p.match(TokenGroupBy) {
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = append(stmt.GroupBy, expr)

			if !p.match(TokenComma) {
				break
			}
		}

		// Parse optional HAVING clause
		if p.match(TokenHaving) {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.Having = expr
		}
	}

	// Parse optional ORDER BY clause
	if p.match(TokenOrderBy) {
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}

			orderBy := OrderByClause{Expr: expr}
			if p.match(TokenDesc) {
				orderBy.Desc = true
			} else {
				p.match(TokenAsc) // Optional ASC
			}

			stmt.OrderBy = append(stmt.OrderBy, orderBy)

			if !p.match(TokenComma) {
				break
			}
		}
	}

	// Parse optional LIMIT clause
	if p.match(TokenLimit) {
		if p.current.Type != TokenNumber {
			return nil, p.error("expected number after LIMIT")
		}
		limit, err := strconv.Atoi(p.current.Value)
		if err != nil {
			return nil, p.error("invalid LIMIT value")
		}
		stmt.Limit = &limit
		p.advance()
	}

	// Parse optional OFFSET clause
	if p.match(TokenOffset) {
		if p.current.Type != TokenNumber {
			return nil, p.error("expected number after OFFSET")
		}
		offset, err := strconv.Atoi(p.current.Value)
		if err != nil {
			return nil, p.error("invalid OFFSET value")
		}
		stmt.Offset = &offset
		p.advance()
	}

	return stmt, nil
}

// parseUpdate parses an UPDATE statement.
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	if !p.consume(TokenUpdate, "expected UPDATE") {
		return nil, p.lastError()
	}

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	if !p.consume(TokenSet, "expected SET") {
		return nil, p.lastError()
	}

	// Parse set clauses
	var assignments []Assignment
	for {
		columnName := p.current.Value
		if !p.consume(TokenIdentifier, "expected column name") {
			return nil, p.lastError()
		}

		if !p.consume(TokenEqual, "expected '='") {
			return nil, p.lastError()
		}

		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		assignments = append(assignments, Assignment{
			Column: columnName,
			Value:  expr,
		})

		if !p.match(TokenComma) {
			break
		}
	}

	stmt := &UpdateStmt{
		TableName:   tableName,
		Assignments: assignments,
	}

	// Parse optional WHERE clause
	if p.match(TokenWhere) {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	return stmt, nil
}

// parseDelete parses a DELETE statement.
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	if !p.consume(TokenDelete, "expected DELETE") {
		return nil, p.lastError()
	}

	if !p.consume(TokenFrom, "expected FROM") {
		return nil, p.lastError()
	}

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	stmt := &DeleteStmt{
		TableName: tableName,
	}

	// Parse optional WHERE clause
	if p.match(TokenWhere) {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	return stmt, nil
}

// parseDrop parses DROP statements (TABLE, INDEX, etc.).
func (p *Parser) parseDrop() (Statement, error) {
	if !p.consume(TokenDrop, "expected DROP") {
		return nil, p.lastError()
	}

	switch p.current.Type { //nolint:exhaustive
	case TokenTable:
		return p.parseDropTable()
	case TokenIndex:
		return p.parseDropIndex()
	default:
		return nil, p.error(fmt.Sprintf("unexpected token after DROP: %s", p.current))
	}
}

// parseDropTable parses a DROP TABLE statement.
func (p *Parser) parseDropTable() (*DropTableStmt, error) {
	if !p.consume(TokenTable, "expected TABLE") {
		return nil, p.lastError()
	}

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	return &DropTableStmt{
		TableName: tableName,
	}, nil
}

// parseAlter parses an ALTER statement.
func (p *Parser) parseAlter() (Statement, error) {
	if !p.consume(TokenAlter, "expected ALTER") {
		return nil, p.lastError()
	}

	switch p.current.Type { //nolint:exhaustive
	case TokenTable:
		return p.parseAlterTable()
	default:
		return nil, p.error(fmt.Sprintf("unexpected token after ALTER: %s", p.current))
	}
}

// parseAlterTable parses an ALTER TABLE statement.
func (p *Parser) parseAlterTable() (*AlterTableStmt, error) {
	if !p.consume(TokenTable, "expected TABLE") {
		return nil, p.lastError()
	}

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	// Parse ALTER TABLE action
	switch p.current.Type { //nolint:exhaustive
	case TokenAdd:
		return p.parseAlterTableAdd(tableName)
	case TokenDrop:
		return p.parseAlterTableDrop(tableName)
	default:
		return nil, p.error(fmt.Sprintf("unexpected ALTER TABLE action: %s", p.current))
	}
}

// parseAlterTableAdd parses ALTER TABLE ... ADD COLUMN
func (p *Parser) parseAlterTableAdd(tableName string) (*AlterTableStmt, error) {
	if !p.consume(TokenAdd, "expected ADD") {
		return nil, p.lastError()
	}

	// COLUMN keyword is optional
	p.match(TokenColumn) // Consume if present

	// Parse column definition
	columnDef, err := p.parseColumnDef()
	if err != nil {
		return nil, err
	}

	return &AlterTableStmt{
		TableName: tableName,
		Action:    AlterTableActionAddColumn,
		Column:    &columnDef,
	}, nil
}

// parseAlterTableDrop parses ALTER TABLE ... DROP COLUMN
func (p *Parser) parseAlterTableDrop(tableName string) (*AlterTableStmt, error) {
	if !p.consume(TokenDrop, "expected DROP") {
		return nil, p.lastError()
	}

	// COLUMN keyword is required for DROP
	if !p.consume(TokenColumn, "expected COLUMN") {
		return nil, p.lastError()
	}

	// Get column name
	columnName := p.current.Value
	if !p.consume(TokenIdentifier, "expected column name") {
		return nil, p.lastError()
	}

	return &AlterTableStmt{
		TableName:  tableName,
		Action:     AlterTableActionDropColumn,
		ColumnName: columnName,
	}, nil
}

// parseDropIndex parses a DROP INDEX statement.
func (p *Parser) parseDropIndex() (*DropIndexStmt, error) {
	if !p.consume(TokenIndex, "expected INDEX") {
		return nil, p.lastError()
	}

	// Get index name
	indexName := p.current.Value
	if !p.consume(TokenIdentifier, "expected index name") {
		return nil, p.lastError()
	}

	// Optional ON table_name
	tableName := ""
	if p.match(TokenOn) {
		tableName = p.current.Value
		if !p.consume(TokenIdentifier, "expected table name") {
			return nil, p.lastError()
		}
	}

	return &DropIndexStmt{
		IndexName: indexName,
		TableName: tableName,
	}, nil
}

// parseAnalyze parses an ANALYZE statement.
func (p *Parser) parseAnalyze() (*AnalyzeStmt, error) {
	if !p.consume(TokenAnalyze, "expected ANALYZE") {
		return nil, p.lastError()
	}

	// Optional TABLE keyword
	p.match(TokenTable)

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	stmt := &AnalyzeStmt{
		TableName: tableName,
		Columns:   []string{},
	}

	// Optional column list
	if p.match(TokenLeftParen) {
		for {
			columnName := p.current.Value
			if !p.consume(TokenIdentifier, "expected column name") {
				return nil, p.lastError()
			}
			stmt.Columns = append(stmt.Columns, columnName)

			if !p.match(TokenComma) {
				break
			}
		}

		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}
	}

	return stmt, nil
}

// parseVacuum parses a VACUUM statement.
func (p *Parser) parseVacuum() (*VacuumStmt, error) {
	if !p.consume(TokenVacuum, "expected VACUUM") {
		return nil, p.lastError()
	}

	stmt := &VacuumStmt{
		TableName: "", // Empty means vacuum all tables
		Analyze:   false,
	}

	// Check for VACUUM ANALYZE
	if p.match(TokenAnalyze) {
		stmt.Analyze = true
	}

	// Optional TABLE keyword
	p.match(TokenTable)

	// Optional table name
	if p.check(TokenIdentifier) {
		stmt.TableName = p.current.Value
		p.advance()
	}

	return stmt, nil
}

// parseCopy parses a COPY statement.
// Syntax: COPY table [(column_list)] FROM|TO STDIN|STDOUT|'filename' [WITH (options)]
func (p *Parser) parseCopy() (*CopyStmt, error) {
	if !p.consume(TokenCopy, "expected COPY") {
		return nil, p.lastError()
	}

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	stmt := &CopyStmt{
		TableName: tableName,
		Options:   make(map[string]string),
	}

	// Optional column list
	if p.match(TokenLeftParen) {
		columns, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = columns
		if !p.consume(TokenRightParen, "expected )") {
			return nil, p.lastError()
		}
	}

	// Direction: FROM or TO
	if p.match(TokenFrom) {
		stmt.Direction = CopyFrom
	} else if p.match(TokenTo) {
		stmt.Direction = CopyTo
	} else {
		return nil, p.error("expected FROM or TO")
	}

	// Source: STDIN, STDOUT, or filename (string literal)
	if p.match(TokenStdin) {
		stmt.Source = "STDIN"
	} else if p.match(TokenStdout) {
		stmt.Source = "STDOUT"
	} else if p.check(TokenString) {
		stmt.Source = p.current.Value
		p.advance()
	} else {
		return nil, p.error("expected STDIN, STDOUT, or filename")
	}

	// Optional WITH clause
	if p.match(TokenWith) {
		if !p.consume(TokenLeftParen, "expected ( after WITH") {
			return nil, p.lastError()
		}

		// Parse options
		for {
			// Option name
			if !p.check(TokenIdentifier) && !p.check(TokenFormat) && !p.check(TokenDelimiter) {
				return nil, p.error("expected option name")
			}
			optName := strings.ToUpper(p.current.Value)
			p.advance()

			// Option value (optional for some options)
			optValue := ""
			if p.check(TokenString) || p.check(TokenIdentifier) || p.check(TokenCsv) || p.check(TokenBinary) {
				optValue = p.current.Value
				p.advance()
			}

			stmt.Options[optName] = optValue

			// Check for more options
			if !p.match(TokenComma) {
				break
			}
		}

		if !p.consume(TokenRightParen, "expected ) after WITH options") {
			return nil, p.lastError()
		}
	}

	return stmt, nil
}

// parseIdentifierList parses a comma-separated list of identifiers.
func (p *Parser) parseIdentifierList() ([]string, error) {
	var identifiers []string

	for {
		if p.current.Type != TokenIdentifier {
			return nil, p.error("expected identifier")
		}
		identifiers = append(identifiers, p.current.Value)
		p.advance()

		if !p.match(TokenComma) {
			break
		}
	}

	if len(identifiers) == 0 {
		return nil, p.error("expected at least one identifier")
	}

	return identifiers, nil
}

// parsePrepare parses a PREPARE statement.
// Syntax: PREPARE name [(type1, type2, ...)] AS statement
func (p *Parser) parsePrepare() (*PrepareStmt, error) {
	if !p.consume(TokenPrepare, "expected PREPARE") {
		return nil, p.lastError()
	}

	// Get statement name
	name := p.current.Value
	if !p.consume(TokenIdentifier, "expected statement name") {
		return nil, p.lastError()
	}

	stmt := &PrepareStmt{
		Name: name,
	}

	// Optional parameter type list
	if p.match(TokenLeftParen) {
		for {
			dataType, err := p.parseDataType()
			if err != nil {
				return nil, err
			}
			stmt.ParamTypes = append(stmt.ParamTypes, dataType)

			if !p.match(TokenComma) {
				break
			}
		}

		if !p.consume(TokenRightParen, "expected ) after parameter types") {
			return nil, p.lastError()
		}
	}

	// AS keyword
	if !p.consume(TokenAs, "expected AS") {
		return nil, p.lastError()
	}

	// Parse the statement to prepare
	query, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	stmt.Query = query

	return stmt, nil
}

// parseExecute parses an EXECUTE statement.
// Syntax: EXECUTE name [(param1, param2, ...)]
func (p *Parser) parseExecute() (*ExecuteStmt, error) {
	if !p.consume(TokenExecute, "expected EXECUTE") {
		return nil, p.lastError()
	}

	// Get statement name
	name := p.current.Value
	if !p.consume(TokenIdentifier, "expected statement name") {
		return nil, p.lastError()
	}

	stmt := &ExecuteStmt{
		Name: name,
	}

	// Optional parameter list
	if p.match(TokenLeftParen) {
		for {
			param, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.Params = append(stmt.Params, param)

			if !p.match(TokenComma) {
				break
			}
		}

		if !p.consume(TokenRightParen, "expected ) after parameters") {
			return nil, p.lastError()
		}
	}

	return stmt, nil
}

// parseDeallocate parses a DEALLOCATE statement.
// Syntax: DEALLOCATE [PREPARE] name
func (p *Parser) parseDeallocate() (*DeallocateStmt, error) {
	if !p.consume(TokenDeallocate, "expected DEALLOCATE") {
		return nil, p.lastError()
	}

	// Optional PREPARE keyword
	p.match(TokenPrepare)

	// Get statement name
	name := p.current.Value
	if !p.consume(TokenIdentifier, "expected statement name") {
		return nil, p.lastError()
	}

	return &DeallocateStmt{
		Name: name,
	}, nil
}

// parseCreateIndex parses a CREATE INDEX statement.
func (p *Parser) parseCreateIndex() (*CreateIndexStmt, error) {
	if !p.consume(TokenIndex, "expected INDEX") {
		return nil, p.lastError()
	}
	return p.parseCreateIndexWithUnique(false)
}

// parseCreateIndexWithUnique parses CREATE [UNIQUE] INDEX statement.
func (p *Parser) parseCreateIndexWithUnique(unique bool) (*CreateIndexStmt, error) {
	// Get index name
	indexName := p.current.Value
	if !p.consume(TokenIdentifier, "expected index name") {
		return nil, p.lastError()
	}

	if !p.consume(TokenOn, "expected ON") {
		return nil, p.lastError()
	}

	// Get table name
	tableName := p.current.Value
	if !p.consume(TokenIdentifier, "expected table name") {
		return nil, p.lastError()
	}

	if !p.consume(TokenLeftParen, "expected '('") {
		return nil, p.lastError()
	}

	// Parse column list
	var columns []string
	for {
		colName := p.current.Value
		if !p.consume(TokenIdentifier, "expected column name") {
			return nil, p.lastError()
		}
		columns = append(columns, colName)

		// Optional ASC/DESC (ignored for now)
		if p.match(TokenAsc) || p.match(TokenDesc) {
			// TODO: Store sort order when CREATE INDEX supports it
			// Currently skipping as indexes don't store sort direction
			_ = columns // Placeholder to avoid empty branch warning
		}

		if !p.match(TokenComma) {
			break
		}
	}

	if !p.consume(TokenRightParen, "expected ')'") {
		return nil, p.lastError()
	}

	// Optional USING clause
	indexType := "BTREE" // default
	if p.match(TokenUsing) {
		indexType = p.current.Value
		if !p.consume(TokenIdentifier, "expected index type") {
			return nil, p.lastError()
		}
	}

	return &CreateIndexStmt{
		IndexName: indexName,
		TableName: tableName,
		Columns:   columns,
		Unique:    unique,
		IndexType: indexType,
	}, nil
}

// parseExpression parses an expression with operator precedence.
func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOr()
}

// parseOr parses OR expressions.
func (p *Parser) parseOr() (Expression, error) {
	expr, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.match(TokenOr) {
		op := p.previous.Type
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpr{
			Left:     expr,
			Operator: op,
			Right:    right,
		}
	}

	return expr, nil
}

// parseAnd parses AND expressions.
func (p *Parser) parseAnd() (Expression, error) {
	expr, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.match(TokenAnd) {
		op := p.previous.Type
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpr{
			Left:     expr,
			Operator: op,
			Right:    right,
		}
	}

	return expr, nil
}

// parseNot parses NOT expressions.
func (p *Parser) parseNot() (Expression, error) {
	// Handle NOT EXISTS
	if p.match(TokenNot) {
		// Check for NOT EXISTS
		if p.match(TokenExists) {
			if !p.consume(TokenLeftParen, "expected '(' after EXISTS") {
				return nil, p.lastError()
			}

			subquery, err := p.parseSelect()
			if err != nil {
				return nil, err
			}

			if !p.consume(TokenRightParen, "expected ')' after subquery") {
				return nil, p.lastError()
			}

			return &ExistsExpr{
				Subquery: &SubqueryExpr{Query: subquery},
				Not:      true,
			}, nil
		}

		// General NOT expression
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{
			Operator: TokenNot,
			Expr:     expr,
		}, nil
	}

	// Handle EXISTS without NOT
	if p.match(TokenExists) {
		if !p.consume(TokenLeftParen, "expected '(' after EXISTS") {
			return nil, p.lastError()
		}

		subquery, err := p.parseSelect()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenRightParen, "expected ')' after subquery") {
			return nil, p.lastError()
		}

		return &ExistsExpr{
			Subquery: &SubqueryExpr{Query: subquery},
			Not:      false,
		}, nil
	}

	return p.parseComparison()
}

// parseComparison parses comparison expressions.
func (p *Parser) parseComparison() (Expression, error) {
	expr, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	// Handle comparison operators
	if p.matchAny(TokenEqual, TokenNotEqual, TokenLess, TokenLessEqual, TokenGreater, TokenGreaterEqual) {
		op := p.previous.Type
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ComparisonExpr{
			Left:     expr,
			Operator: op,
			Right:    right,
		}, nil
	}

	// Handle LIKE
	if p.match(TokenLike) {
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ComparisonExpr{
			Left:     expr,
			Operator: TokenLike,
			Right:    right,
		}, nil
	}

	// Handle NOT IN and NOT BETWEEN
	if p.match(TokenNot) {
		if p.match(TokenIn) {
			// Parse NOT IN
			return p.parseInExpression(expr, true)
		}

		if p.match(TokenBetween) {
			// Parse NOT BETWEEN
			lower, err := p.parseTerm()
			if err != nil {
				return nil, err
			}

			if !p.consume(TokenAnd, "expected AND in NOT BETWEEN expression") {
				return nil, p.lastError()
			}

			upper, err := p.parseTerm()
			if err != nil {
				return nil, err
			}

			return &BetweenExpr{
				Expr:  expr,
				Lower: lower,
				Upper: upper,
				Not:   true,
			}, nil
		}

		// If NOT is not followed by IN or BETWEEN, it's an error at this position
		return nil, p.error("unexpected NOT")
	}

	// Handle IN
	if p.match(TokenIn) {
		return p.parseInExpression(expr, false)
	}

	// Handle BETWEEN
	if p.match(TokenBetween) {
		lower, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenAnd, "expected AND in BETWEEN expression") {
			return nil, p.lastError()
		}

		upper, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		return &BetweenExpr{
			Expr:  expr,
			Lower: lower,
			Upper: upper,
			Not:   false,
		}, nil
	}

	// Handle IS NULL
	if p.match(TokenIs) {
		notNull := false
		if p.match(TokenNot) {
			notNull = true
		}

		if !p.consume(TokenNull, "expected NULL after IS") {
			return nil, p.lastError()
		}

		return &IsNullExpr{
			Expr: expr,
			Not:  notNull,
		}, nil
	}

	return expr, nil
}

// parseConcat parses string concatenation (||).
func (p *Parser) parseConcat() (Expression, error) {
	expr, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for p.match(TokenConcat) {
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpr{
			Left:     expr,
			Operator: TokenConcat,
			Right:    right,
		}
	}

	return expr, nil
}

// parseTerm parses addition and subtraction.
func (p *Parser) parseTerm() (Expression, error) {
	expr, err := p.parseFactor()
	if err != nil {
		return nil, err
	}

	for p.matchAny(TokenPlus, TokenMinus) {
		op := p.previous.Type
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpr{
			Left:     expr,
			Operator: op,
			Right:    right,
		}
	}

	return expr, nil
}

// parseFactor parses multiplication, division, and modulo.
func (p *Parser) parseFactor() (Expression, error) {
	expr, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.matchAny(TokenStar, TokenSlash, TokenPercent) {
		op := p.previous.Type
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpr{
			Left:     expr,
			Operator: op,
			Right:    right,
		}
	}

	return expr, nil
}

// parseUnary parses unary expressions.
func (p *Parser) parseUnary() (Expression, error) {
	if p.matchAny(TokenPlus, TokenMinus) {
		op := p.previous.Type
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{
			Operator: op,
			Expr:     expr,
		}, nil
	}

	return p.parsePrimary()
}

// parsePrimary parses primary expressions.
func (p *Parser) parsePrimary() (Expression, error) {
	// Literals
	switch p.current.Type { //nolint:exhaustive
	case TokenNumber:
		value := p.current.Value
		p.advance()

		// Try to parse as integer first
		if !strings.Contains(value, ".") {
			if i, err := strconv.ParseInt(value, 10, 64); err == nil {
				// Always use int64 for consistency
				return &Literal{Value: types.NewValue(i)}, nil
			}
		}

		// Parse as decimal
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, p.error("invalid number")
		}
		// For now, use float64 for decimal values until we have proper decimal support
		return &Literal{Value: types.NewValue(f)}, nil

	case TokenString:
		value := p.current.Value
		p.advance()
		return &Literal{Value: types.NewValue(value)}, nil

	case TokenTrue:
		p.advance()
		return &Literal{Value: types.NewValue(true)}, nil

	case TokenFalse:
		p.advance()
		return &Literal{Value: types.NewValue(false)}, nil

	case TokenNull:
		p.advance()
		return &Literal{Value: types.NullValue(types.Unknown)}, nil

	case TokenDate:
		// Handle date literal: date 'YYYY-MM-DD'
		p.advance()
		if p.current.Type != TokenString {
			return nil, p.error("expected string literal after DATE")
		}
		dateStr := p.current.Value
		p.advance()

		// Parse the date string
		dateValue, err := types.ParseDate(dateStr)
		if err != nil {
			return nil, p.error(fmt.Sprintf("invalid date literal: %v", err))
		}
		return &Literal{Value: dateValue}, nil

	case TokenTimestamp:
		// Handle timestamp literal: timestamp 'YYYY-MM-DD HH:MM:SS'
		p.advance()
		if p.current.Type != TokenString {
			return nil, p.error("expected string literal after TIMESTAMP")
		}
		timestampStr := p.current.Value
		p.advance()

		// Parse the timestamp string
		timestampValue, err := types.ParseTimestamp(timestampStr)
		if err != nil {
			return nil, p.error(fmt.Sprintf("invalid timestamp literal: %v", err))
		}
		return &Literal{Value: timestampValue}, nil

	case TokenInterval:
		// Handle interval literal: interval '1 day', interval '2 months', etc.
		p.advance()
		if p.current.Type != TokenString {
			return nil, p.error("expected string literal after INTERVAL")
		}
		intervalStr := p.current.Value
		p.advance()

		// Parse the interval string
		intervalValue, err := p.parseIntervalString(intervalStr)
		if err != nil {
			return nil, p.error(fmt.Sprintf("invalid interval literal: %v", err))
		}
		return &Literal{Value: types.NewIntervalValue(intervalValue)}, nil

	case TokenSubstring:
		// Handle SUBSTRING(string FROM start [FOR length])
		p.advance() // consume 'SUBSTRING'

		if !p.consume(TokenLeftParen, "expected '(' after SUBSTRING") {
			return nil, p.lastError()
		}

		// Parse the string expression
		strExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenFrom, "expected 'FROM' in SUBSTRING expression") {
			return nil, p.lastError()
		}

		// Parse the start position
		startExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		// Optional FOR length
		var lengthExpr Expression
		if p.match(TokenFor) {
			lengthExpr, err = p.parseExpression()
			if err != nil {
				return nil, err
			}
		}

		if !p.consume(TokenRightParen, "expected ')' after SUBSTRING expression") {
			return nil, p.lastError()
		}

		return &SubstringExpr{
			Str:    strExpr,
			Start:  startExpr,
			Length: lengthExpr,
		}, nil

	case TokenExtract:
		// Handle EXTRACT(field FROM expression)
		p.advance() // consume 'EXTRACT'

		if !p.consume(TokenLeftParen, "expected '(' after EXTRACT") {
			return nil, p.lastError()
		}

		// Parse the field (YEAR, MONTH, DAY, etc.)
		if p.current.Type != TokenYear && p.current.Type != TokenMonth &&
			p.current.Type != TokenDay && p.current.Type != TokenHour &&
			p.current.Type != TokenMinute && p.current.Type != TokenSecond {
			return nil, p.error("expected date field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) in EXTRACT")
		}

		field := p.current.Value
		p.advance()

		if !p.consume(TokenFrom, "expected 'FROM' in EXTRACT expression") {
			return nil, p.lastError()
		}

		// Parse the source expression
		fromExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenRightParen, "expected ')' after EXTRACT expression") {
			return nil, p.lastError()
		}

		return &ExtractExpr{
			Field: strings.ToUpper(field),
			From:  fromExpr,
		}, nil

	case TokenCase:
		// Handle CASE expression
		p.advance() // consume 'CASE'

		var expr Expression
		var whenList []WhenClause
		var elseExpr Expression

		// Check if this is a simple CASE (CASE expr WHEN...) or searched CASE (CASE WHEN...)
		if p.current.Type != TokenWhen {
			// Simple CASE - parse the expression after CASE
			var err error
			expr, err = p.parseComparison() // Parse at comparison level to avoid infinite recursion
			if err != nil {
				return nil, err
			}
		}

		// Parse WHEN clauses
		for p.current.Type == TokenWhen {
			p.advance() // consume 'WHEN'

			// Parse the condition
			condition, err := p.parseComparison() // Parse at comparison level for conditions
			if err != nil {
				return nil, err
			}

			if !p.consume(TokenThen, "expected 'THEN' after WHEN condition") {
				return nil, p.lastError()
			}

			// Parse the result
			result, err := p.parseComparison() // Parse at comparison level for results
			if err != nil {
				return nil, err
			}

			whenList = append(whenList, WhenClause{
				Condition: condition,
				Result:    result,
			})
		}

		if len(whenList) == 0 {
			return nil, p.error("CASE expression must have at least one WHEN clause")
		}

		// Parse optional ELSE clause
		if p.match(TokenElse) {
			var err error
			elseExpr, err = p.parseComparison() // Parse at comparison level
			if err != nil {
				return nil, err
			}
		}

		if !p.consume(TokenEnd, "expected 'END' to close CASE expression") {
			return nil, p.lastError()
		}

		return &CaseExpr{
			Expr:     expr,
			WhenList: whenList,
			Else:     elseExpr,
		}, nil

	case TokenIdentifier:
		name := p.current.Value
		p.advance()

		// Check if it's a function call
		if p.check(TokenLeftParen) {
			p.advance() // consume '('

			var args []Expression
			distinct := false

			// Handle COUNT(*) special case
			if strings.ToUpper(name) == "COUNT" && p.check(TokenStar) {
				p.advance() // consume '*'
				args = append(args, &Star{})
			} else {
				// Check for DISTINCT keyword
				if p.match(TokenDistinct) {
					distinct = true
				}

				// Parse function arguments
				if !p.check(TokenRightParen) {
					for {
						arg, err := p.parseExpression()
						if err != nil {
							return nil, err
						}
						args = append(args, arg)

						if !p.match(TokenComma) {
							break
						}
					}
				}
			}

			if !p.consume(TokenRightParen, "expected ')' after function arguments") {
				return nil, p.lastError()
			}

			return &FunctionCall{
				Name:     strings.ToUpper(name),
				Args:     args,
				Distinct: distinct,
			}, nil
		}

		// Check for qualified name (table.column)
		table := ""
		if p.match(TokenDot) {
			if !p.canBeIdentifier() {
				return nil, p.error("expected column name after '.'")
			}
			table = name
			name = p.current.Value
			p.advance()
		}

		return &Identifier{Name: name, Table: table}, nil

	case TokenParam:
		paramStr := p.current.Value
		p.advance()
		// Extract the number from $N
		if len(paramStr) < 2 || paramStr[0] != '$' {
			return nil, p.error("invalid parameter format")
		}
		index, err := strconv.Atoi(paramStr[1:])
		if err != nil {
			return nil, p.error("invalid parameter number")
		}
		if index < 1 {
			return nil, p.error("parameter index must be >= 1")
		}
		return &ParameterRef{Index: index}, nil

	case TokenLeftParen:
		p.advance()

		// Check if it's a subquery by looking for SELECT
		if p.check(TokenSelect) {
			subquery, err := p.parseSelect()
			if err != nil {
				return nil, err
			}

			if !p.consume(TokenRightParen, "expected ')' after subquery") {
				return nil, p.lastError()
			}

			return &SubqueryExpr{Query: subquery}, nil
		}

		// Otherwise, parse as regular parenthesized expression
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if !p.consume(TokenRightParen, "expected ')'") {
			return nil, p.lastError()
		}
		return &ParenExpr{Expr: expr}, nil

	default:
		return nil, p.error(fmt.Sprintf("unexpected token in expression: %s", p.current))
	}
}

// Helper methods.

func (p *Parser) advance() {
	p.previous = p.current
	p.current = p.lexer.NextToken()
}

func (p *Parser) check(tokenType TokenType) bool {
	return p.current.Type == tokenType
}

func (p *Parser) peek(tokenType TokenType) bool {
	// Save current state
	savedCurrent := p.current
	savedPrevious := p.previous
	savedPosition := p.lexer.position
	savedLine := p.lexer.line
	savedColumn := p.lexer.column

	// Look ahead
	p.advance()
	result := p.check(tokenType)

	// Restore state
	p.current = savedCurrent
	p.previous = savedPrevious
	p.lexer.position = savedPosition
	p.lexer.line = savedLine
	p.lexer.column = savedColumn

	return result
}

func (p *Parser) match(tokenType TokenType) bool {
	if p.check(tokenType) {
		p.advance()
		return true
	}
	return false
}

func (p *Parser) matchAny(types ...TokenType) bool {
	for _, t := range types {
		if p.match(t) {
			return true
		}
	}
	return false
}

func (p *Parser) consume(tokenType TokenType, message string) bool {
	if p.check(tokenType) {
		p.advance()
		return true
	}
	p.errors = append(p.errors, p.error(message))
	return false
}

func (p *Parser) error(message string) error {
	err := NewParseError(message, p.current.Line, p.current.Column)
	p.errors = append(p.errors, err)
	return err
}

func (p *Parser) lastError() error {
	if len(p.errors) > 0 {
		return p.errors[len(p.errors)-1]
	}
	return NewParseError("unknown parse error", 0, 0)
}

// parseInExpression parses IN expressions with either value lists or subqueries
func (p *Parser) parseInExpression(expr Expression, not bool) (Expression, error) {
	if !p.consume(TokenLeftParen, "expected '(' after IN") {
		return nil, p.lastError()
	}

	// Check if it's a subquery by looking for SELECT
	if p.check(TokenSelect) {
		subquery, err := p.parseSelect()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenRightParen, "expected ')' after subquery") {
			return nil, p.lastError()
		}

		return &InExpr{
			Expr:     expr,
			Subquery: &SubqueryExpr{Query: subquery},
			Not:      not,
		}, nil
	}

	// Otherwise, parse value list
	var values []Expression

	// Handle empty list case
	if p.check(TokenRightParen) {
		// Empty list is valid in SQL
	} else {
		for {
			val, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			values = append(values, val)

			if !p.match(TokenComma) {
				break
			}
		}
	}

	if !p.consume(TokenRightParen, "expected ')'") {
		return nil, p.lastError()
	}

	return &InExpr{
		Expr:   expr,
		Values: values,
		Not:    not,
	}, nil
}

// Additional AST node definitions.

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	TableName   string
	Assignments []Assignment
	Where       Expression
}

func (s *UpdateStmt) statementNode() {}
func (s *UpdateStmt) String() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("UPDATE %s SET", s.TableName))

	var assigns []string //nolint:prealloc
	for _, a := range s.Assignments {
		assigns = append(assigns, fmt.Sprintf("%s = %s", a.Column, a.Value.String()))
	}
	parts = append(parts, strings.Join(assigns, ", "))

	if s.Where != nil {
		parts = append(parts, fmt.Sprintf("WHERE %s", s.Where.String()))
	}

	return strings.Join(parts, " ")
}

// Assignment represents a column assignment in UPDATE.
type Assignment struct {
	Column string
	Value  Expression
}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	TableName string
	Where     Expression
}

func (s *DeleteStmt) statementNode() {}
func (s *DeleteStmt) String() string {
	parts := []string{fmt.Sprintf("DELETE FROM %s", s.TableName)}
	if s.Where != nil {
		parts = append(parts, fmt.Sprintf("WHERE %s", s.Where.String()))
	}
	return strings.Join(parts, " ")
}

// DropTableStmt represents a DROP TABLE statement.
type DropTableStmt struct {
	TableName string
}

func (s *DropTableStmt) statementNode() {}
func (s *DropTableStmt) String() string {
	return fmt.Sprintf("DROP TABLE %s", s.TableName)
}

// CreateIndexStmt represents a CREATE INDEX statement.
type CreateIndexStmt struct {
	IndexName string
	TableName string
	Columns   []string
	Unique    bool
	IndexType string // BTREE, HASH, etc.
}

func (s *CreateIndexStmt) statementNode() {}
func (s *CreateIndexStmt) String() string {
	unique := ""
	if s.Unique {
		unique = "UNIQUE "
	}
	indexType := ""
	if s.IndexType != "" {
		indexType = fmt.Sprintf(" USING %s", s.IndexType)
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)%s",
		unique, s.IndexName, s.TableName, strings.Join(s.Columns, ", "), indexType)
}

// DropIndexStmt represents a DROP INDEX statement.
type DropIndexStmt struct {
	IndexName string
	TableName string // Optional in some SQL dialects
}

func (s *DropIndexStmt) statementNode() {}
func (s *DropIndexStmt) String() string {
	if s.TableName != "" {
		return fmt.Sprintf("DROP INDEX %s ON %s", s.IndexName, s.TableName)
	}
	return fmt.Sprintf("DROP INDEX %s", s.IndexName)
}

// AlterTableAction represents the type of ALTER TABLE action
type AlterTableAction int

const (
	AlterTableActionAddColumn AlterTableAction = iota
	AlterTableActionDropColumn
)

// AlterTableStmt represents an ALTER TABLE statement.
type AlterTableStmt struct {
	TableName  string
	Action     AlterTableAction
	Column     *ColumnDef // Used for ADD COLUMN
	ColumnName string     // Used for DROP COLUMN
}

func (s *AlterTableStmt) statementNode() {}
func (s *AlterTableStmt) String() string {
	switch s.Action {
	case AlterTableActionAddColumn:
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", s.TableName, s.Column.String())
	case AlterTableActionDropColumn:
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", s.TableName, s.ColumnName)
	default:
		return fmt.Sprintf("ALTER TABLE %s", s.TableName)
	}
}

// parseTableExpression parses a table expression which can be:
// - Simple table reference: table_name [AS alias]
// - Subquery: (SELECT ...) AS alias
// - Join expression: table1 JOIN table2 ON condition
// - Comma-separated tables: table1, table2, table3 (implicit CROSS JOIN)
func (p *Parser) parseTableExpression() (TableExpression, error) {
	// Parse the left side (can be a table reference or subquery)
	left, err := p.parseTableOrSubquery()
	if err != nil {
		return nil, err
	}

	// Handle both JOIN keywords and comma-separated tables
	for {
		if p.peekJoinKeyword() {
			// Explicit JOIN syntax
			joinType := p.parseJoinType()

			// Parse the right side table reference or subquery
			right, err := p.parseTableOrSubquery()
			if err != nil {
				return nil, err
			}

			// Parse ON condition
			var condition Expression
			if p.match(TokenOn) {
				condition, err = p.parseExpression()
				if err != nil {
					return nil, err
				}
			} else if joinType != CrossJoin {
				// ON condition is required for all joins except CROSS JOIN
				return nil, fmt.Errorf("expected ON condition for %s", joinType.String())
			}

			// Create join expression
			left = &JoinExpr{
				Left:      left,
				Right:     right,
				JoinType:  joinType,
				Condition: condition,
			}
		} else if p.match(TokenComma) {
			// Comma-separated tables (implicit CROSS JOIN)
			right, err := p.parseTableOrSubquery()
			if err != nil {
				return nil, err
			}

			// Create CROSS JOIN for comma-separated tables
			left = &JoinExpr{
				Left:      left,
				Right:     right,
				JoinType:  CrossJoin,
				Condition: nil,
			}
		} else {
			// No more joins or commas
			break
		}
	}

	return left, nil
}

// parseTableOrSubquery parses either a table reference or a subquery
func (p *Parser) parseTableOrSubquery() (TableExpression, error) {
	if p.check(TokenLeftParen) {
		// It's a subquery
		p.advance() // consume '('

		// Parse the SELECT statement
		subquery, err := p.parseSelect()
		if err != nil {
			return nil, err
		}

		if !p.consume(TokenRightParen, "expected ')' after subquery") {
			return nil, p.lastError()
		}

		// Subquery in FROM must have an alias
		var alias string
		if p.match(TokenAs) {
			if p.current.Type != TokenIdentifier {
				return nil, p.error("expected alias after AS")
			}
			alias = p.current.Value
			p.advance()
		} else if p.current.Type == TokenIdentifier {
			// Implicit alias (without AS keyword)
			alias = p.current.Value
			p.advance()
		} else {
			return nil, p.error("subquery in FROM must have an alias")
		}

		return &SubqueryRef{
			Query: subquery,
			Alias: alias,
		}, nil
	}

	// It's a regular table reference
	return p.parseTableRef()
}

// parseTableRef parses a simple table reference with optional alias
func (p *Parser) parseTableRef() (*TableRef, error) {
	if p.current.Type != TokenIdentifier {
		return nil, fmt.Errorf("expected table name")
	}

	tableName := p.current.Value
	p.advance()

	// Check for optional alias
	alias := ""
	if p.match(TokenAs) {
		if p.current.Type != TokenIdentifier {
			return nil, fmt.Errorf("expected alias after AS")
		}
		alias = p.current.Value
		p.advance()
	} else if p.current.Type == TokenIdentifier {
		// Implicit alias (without AS keyword)
		alias = p.current.Value
		p.advance()
	}

	return &TableRef{
		TableName: tableName,
		Alias:     alias,
	}, nil
}

// peekJoinKeyword checks if the current token is a JOIN-related keyword
func (p *Parser) peekJoinKeyword() bool {
	switch p.current.Type {
	case TokenJoin, TokenInner, TokenLeft, TokenRight, TokenFull, TokenCross:
		return true
	default:
		return false
	}
}

// parseJoinType parses the JOIN type from keywords
func (p *Parser) parseJoinType() JoinType {
	// Handle different JOIN syntaxes:
	// - JOIN (defaults to INNER)
	// - INNER JOIN
	// - LEFT [OUTER] JOIN
	// - RIGHT [OUTER] JOIN
	// - FULL [OUTER] JOIN
	// - CROSS JOIN

	if p.match(TokenCross) {
		if !p.consume(TokenJoin, "expected JOIN after CROSS") {
			// This shouldn't happen if peekJoinKeyword worked correctly
			return InnerJoin
		}
		return CrossJoin
	}

	if p.match(TokenInner) {
		if !p.consume(TokenJoin, "expected JOIN after INNER") {
			return InnerJoin
		}
		return InnerJoin
	}

	if p.match(TokenLeft) {
		p.match(TokenOuter) // Optional OUTER keyword
		if !p.consume(TokenJoin, "expected JOIN after LEFT") {
			return InnerJoin
		}
		return LeftJoin
	}

	if p.match(TokenRight) {
		p.match(TokenOuter) // Optional OUTER keyword
		if !p.consume(TokenJoin, "expected JOIN after RIGHT") {
			return InnerJoin
		}
		return RightJoin
	}

	if p.match(TokenFull) {
		p.match(TokenOuter) // Optional OUTER keyword
		if !p.consume(TokenJoin, "expected JOIN after FULL") {
			return InnerJoin
		}
		return FullJoin
	}

	// Default JOIN (without qualifier) is INNER JOIN
	if p.match(TokenJoin) {
		return InnerJoin
	}

	// This shouldn't happen if peekJoinKeyword worked correctly
	return InnerJoin
}

// canBeIdentifier checks if a token can be used as an identifier.
// This includes actual identifiers and keywords that can be used as column names.
func (p *Parser) canBeIdentifier() bool {
	// Most keywords can be used as identifiers in certain contexts
	// This is especially true for column names after a dot (table.column)
	switch p.current.Type {
	case TokenIdentifier:
		return true
	// Common SQL keywords that are often used as column names
	case TokenDate, TokenTimestamp, TokenYear, TokenMonth, TokenDay,
		TokenHour, TokenMinute, TokenSecond:
		return true
	// Keywords that might be column names
	case TokenKey, TokenIndex, TokenTable:
		return true
	default:
		// In PostgreSQL and many other databases, most keywords can be used as identifiers
		// when they're in a context where an identifier is expected
		// Accept any token that has a non-empty value
		return p.current.Value != "" && p.current.Type != TokenEOF &&
			p.current.Type != TokenError && p.current.Type != TokenNumber &&
			p.current.Type != TokenString && p.current.Type != TokenParam
	}
}

// parseIntervalString parses an interval string like '1 day', '2 months', '3 hours', etc.
func (p *Parser) parseIntervalString(s string) (types.Interval, error) {
	// Simple interval parser - handles patterns like:
	// '1 day', '2 days', '3 months', '1 year', '2 hours', '30 minutes', '45 seconds'
	// '1 year 2 months', '1 day 02:30:00'

	parts := strings.Fields(strings.ToLower(s))
	if len(parts) == 0 {
		return types.Interval{}, fmt.Errorf("empty interval string")
	}

	var interval types.Interval
	i := 0

	for i < len(parts) {
		// Try to parse a number
		if i >= len(parts) {
			break
		}

		num, err := strconv.ParseInt(parts[i], 10, 64)
		if err != nil {
			// Maybe it's a time format like '02:30:00'
			if strings.Contains(parts[i], ":") {
				duration, err := time.ParseDuration(strings.ReplaceAll(parts[i], ":", "h") + "m")
				if err != nil {
					return types.Interval{}, fmt.Errorf("invalid time format: %s", parts[i])
				}
				interval.Duration = duration
				i++
				continue
			}
			return types.Interval{}, fmt.Errorf("expected number, got: %s", parts[i])
		}

		i++
		if i >= len(parts) {
			return types.Interval{}, fmt.Errorf("expected time unit after number")
		}

		unit := parts[i]
		i++

		// Handle singular/plural forms
		unit = strings.TrimSuffix(unit, "s")

		switch unit {
		case "year":
			interval.Months += int32(num * 12)
		case "month":
			interval.Months += int32(num)
		case "week":
			interval.Days += int32(num * 7)
		case "day":
			interval.Days += int32(num)
		case "hour":
			interval.Duration += time.Duration(num) * time.Hour
		case "minute":
			interval.Duration += time.Duration(num) * time.Minute
		case "second":
			interval.Duration += time.Duration(num) * time.Second
		default:
			return types.Interval{}, fmt.Errorf("unknown interval unit: %s", unit)
		}
	}

	return interval, nil
}
