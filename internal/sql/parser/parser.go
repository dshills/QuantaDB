package parser

import (
	"fmt"
	"strconv"
	"strings"

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
	case TokenAnalyze:
		return p.parseAnalyze()
	case TokenVacuum:
		return p.parseVacuum()
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
		if p.check(TokenPrimary) || p.check(TokenUnique) {
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
	if p.match(TokenPrimary) {
		if !p.consume(TokenKey, "expected KEY after PRIMARY") {
			return nil, p.lastError()
		}

		if !p.consume(TokenLeftParen, "expected '('") {
			return nil, p.lastError()
		}

		// Parse column list
		var columns []string
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

		return TablePrimaryKeyConstraint{Columns: columns}, nil
	}

	return nil, p.error("unsupported table constraint")
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
		Columns: columns,
	}

	// Check for optional FROM clause
	if p.match(TokenFrom) {
		// Get table name
		tableName := p.current.Value
		if !p.consume(TokenIdentifier, "expected table name") {
			return nil, p.lastError()
		}
		stmt.From = tableName
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
	expr, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	// Handle comparison operators
	if p.matchAny(TokenEqual, TokenNotEqual, TokenLess, TokenLessEqual, TokenGreater, TokenGreaterEqual) {
		op := p.previous.Type
		right, err := p.parseTerm()
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
		right, err := p.parseTerm()
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
				return &Literal{Value: types.NewValue(i)}, nil
			}
		}

		// Parse as decimal
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, p.error("invalid number")
		}
		return &Literal{Value: types.NewDecimalValueFromFloat(f)}, nil

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
		
		return &Identifier{Name: name}, nil

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
