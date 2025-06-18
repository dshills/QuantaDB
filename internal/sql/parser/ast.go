package parser

import (
	"fmt"
	"strings"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Node is the base interface for all AST nodes.
type Node interface {
	String() string
}

// Statement is the base interface for all SQL statements.
type Statement interface {
	Node
	statementNode()
}

// Expression is the base interface for all SQL expressions.
type Expression interface {
	Node
	expressionNode()
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	TableName   string
	Columns     []ColumnDef
	Constraints []TableConstraint
}

func (s *CreateTableStmt) statementNode() {}
func (s *CreateTableStmt) String() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("CREATE TABLE %s (", s.TableName))

	var cols []string
	for _, col := range s.Columns {
		cols = append(cols, col.String())
	}
	for _, con := range s.Constraints {
		cols = append(cols, con.String())
	}

	parts = append(parts, strings.Join(cols, ", "))
	parts = append(parts, ")")

	return strings.Join(parts, "")
}

// ColumnDef represents a column definition.
type ColumnDef struct {
	Name        string
	DataType    types.DataType
	Constraints []ColumnConstraint
}

func (c ColumnDef) String() string {
	parts := []string{c.Name, c.DataType.Name()}
	for _, con := range c.Constraints {
		parts = append(parts, con.String())
	}
	return strings.Join(parts, " ")
}

// ColumnConstraint represents a column constraint.
type ColumnConstraint interface {
	String() string
}

// NotNullConstraint represents a NOT NULL constraint.
type NotNullConstraint struct{}

func (NotNullConstraint) String() string { return "NOT NULL" }

// PrimaryKeyConstraint represents a PRIMARY KEY constraint.
type PrimaryKeyConstraint struct{}

func (PrimaryKeyConstraint) String() string { return "PRIMARY KEY" }

// UniqueConstraint represents a UNIQUE constraint.
type UniqueConstraint struct{}

func (UniqueConstraint) String() string { return "UNIQUE" }

// DefaultConstraint represents a DEFAULT constraint.
type DefaultConstraint struct {
	Value Expression
}

func (d DefaultConstraint) String() string {
	return fmt.Sprintf("DEFAULT %s", d.Value.String())
}

// TableConstraint represents a table-level constraint.
type TableConstraint interface {
	String() string
}

// TablePrimaryKeyConstraint represents a table-level PRIMARY KEY constraint.
type TablePrimaryKeyConstraint struct {
	Columns []string
}

func (c TablePrimaryKeyConstraint) String() string {
	return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(c.Columns, ", "))
}

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	TableName string
	Columns   []string
	Values    [][]Expression
}

func (s *InsertStmt) statementNode() {}
func (s *InsertStmt) String() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("INSERT INTO %s", s.TableName))

	if len(s.Columns) > 0 {
		parts = append(parts, fmt.Sprintf("(%s)", strings.Join(s.Columns, ", ")))
	}

	parts = append(parts, "VALUES")

	var valueSets []string
	for _, valueSet := range s.Values {
		var values []string
		for _, v := range valueSet {
			values = append(values, v.String())
		}
		valueSets = append(valueSets, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	parts = append(parts, strings.Join(valueSets, ", "))
	return strings.Join(parts, " ")
}

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Columns []SelectColumn
	From    string
	Where   Expression
	OrderBy []OrderByClause
	Limit   *int
	Offset  *int
}

func (s *SelectStmt) statementNode() {}
func (s *SelectStmt) String() string {
	var parts []string

	// SELECT clause
	var cols []string
	for _, col := range s.Columns {
		cols = append(cols, col.String())
	}
	parts = append(parts, fmt.Sprintf("SELECT %s", strings.Join(cols, ", ")))

	// FROM clause
	parts = append(parts, fmt.Sprintf("FROM %s", s.From))

	// WHERE clause
	if s.Where != nil {
		parts = append(parts, fmt.Sprintf("WHERE %s", s.Where.String()))
	}

	// ORDER BY clause
	if len(s.OrderBy) > 0 {
		var orderCols []string
		for _, o := range s.OrderBy {
			orderCols = append(orderCols, o.String())
		}
		parts = append(parts, fmt.Sprintf("ORDER BY %s", strings.Join(orderCols, ", ")))
	}

	// LIMIT clause
	if s.Limit != nil {
		parts = append(parts, fmt.Sprintf("LIMIT %d", *s.Limit))
	}

	// OFFSET clause
	if s.Offset != nil {
		parts = append(parts, fmt.Sprintf("OFFSET %d", *s.Offset))
	}

	return strings.Join(parts, " ")
}

// SelectColumn represents a column in a SELECT statement.
type SelectColumn struct {
	Expr  Expression
	Alias string
}

func (c SelectColumn) String() string {
	if c.Alias != "" {
		return fmt.Sprintf("%s AS %s", c.Expr.String(), c.Alias)
	}
	return c.Expr.String()
}

// OrderByClause represents an ORDER BY clause.
type OrderByClause struct {
	Expr Expression
	Desc bool
}

func (o OrderByClause) String() string {
	if o.Desc {
		return fmt.Sprintf("%s DESC", o.Expr.String())
	}
	return fmt.Sprintf("%s ASC", o.Expr.String())
}

// Literal represents a literal value.
type Literal struct {
	Value types.Value
}

func (l *Literal) expressionNode() {}
func (l *Literal) String() string {
	if l.Value.IsNull() {
		return "NULL"
	}

	switch v := l.Value.Data.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", l.Value.Data)
	}
}

// Identifier represents a column or table identifier.
type Identifier struct {
	Name string
}

func (i *Identifier) expressionNode() {}
func (i *Identifier) String() string {
	return i.Name
}

// ParameterRef represents a parameter placeholder like $1, $2.
type ParameterRef struct {
	Index int // 1-based parameter index
}

func (p *ParameterRef) expressionNode() {}
func (p *ParameterRef) String() string {
	return fmt.Sprintf("$%d", p.Index)
}

// Star represents the * in SELECT *.
type Star struct{}

func (s *Star) expressionNode() {}
func (s *Star) String() string {
	return "*"
}

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	Left     Expression
	Operator TokenType
	Right    Expression
}

func (b *BinaryExpr) expressionNode() {}
func (b *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left.String(), b.Operator.String(), b.Right.String())
}

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	Operator TokenType
	Expr     Expression
}

func (u *UnaryExpr) expressionNode() {}
func (u *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", u.Operator.String(), u.Expr.String())
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expression
}

func (p *ParenExpr) expressionNode() {}
func (p *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", p.Expr.String())
}

// ComparisonExpr represents a comparison expression.
type ComparisonExpr struct {
	Left     Expression
	Operator TokenType
	Right    Expression
}

func (c *ComparisonExpr) expressionNode() {}
func (c *ComparisonExpr) String() string {
	return fmt.Sprintf("%s %s %s", c.Left.String(), c.Operator.String(), c.Right.String())
}

// InExpr represents an IN expression.
type InExpr struct {
	Expr   Expression
	Values []Expression
	Not    bool
}

func (i *InExpr) expressionNode() {}
func (i *InExpr) String() string {
	var values []string
	for _, v := range i.Values {
		values = append(values, v.String())
	}

	if i.Not {
		return fmt.Sprintf("%s NOT IN (%s)", i.Expr.String(), strings.Join(values, ", "))
	}
	return fmt.Sprintf("%s IN (%s)", i.Expr.String(), strings.Join(values, ", "))
}

// BetweenExpr represents a BETWEEN expression.
type BetweenExpr struct {
	Expr  Expression
	Lower Expression
	Upper Expression
	Not   bool
}

func (b *BetweenExpr) expressionNode() {}
func (b *BetweenExpr) String() string {
	if b.Not {
		return fmt.Sprintf("%s NOT BETWEEN %s AND %s", b.Expr.String(), b.Lower.String(), b.Upper.String())
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", b.Expr.String(), b.Lower.String(), b.Upper.String())
}

// IsNullExpr represents an IS NULL expression.
type IsNullExpr struct {
	Expr Expression
	Not  bool
}

func (i *IsNullExpr) expressionNode() {}
func (i *IsNullExpr) String() string {
	if i.Not {
		return fmt.Sprintf("%s IS NOT NULL", i.Expr.String())
	}
	return fmt.Sprintf("%s IS NULL", i.Expr.String())
}
