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

	var cols []string //nolint:prealloc
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

// TableForeignKeyConstraint represents a table-level FOREIGN KEY constraint.
type TableForeignKeyConstraint struct {
	Name           string   // Optional constraint name
	Columns        []string // Local columns
	RefTable       string   // Referenced table
	RefColumns     []string // Referenced columns
	OnDelete       string   // ON DELETE action
	OnUpdate       string   // ON UPDATE action
}

func (c TableForeignKeyConstraint) String() string {
	var parts []string
	if c.Name != "" {
		parts = append(parts, fmt.Sprintf("CONSTRAINT %s", c.Name))
	}
	parts = append(parts, fmt.Sprintf("FOREIGN KEY (%s)", strings.Join(c.Columns, ", ")))
	
	refPart := fmt.Sprintf("REFERENCES %s", c.RefTable)
	if len(c.RefColumns) > 0 {
		refPart += fmt.Sprintf(" (%s)", strings.Join(c.RefColumns, ", "))
	}
	parts = append(parts, refPart)
	
	if c.OnDelete != "" {
		parts = append(parts, fmt.Sprintf("ON DELETE %s", c.OnDelete))
	}
	if c.OnUpdate != "" {
		parts = append(parts, fmt.Sprintf("ON UPDATE %s", c.OnUpdate))
	}
	
	return strings.Join(parts, " ")
}

// TableCheckConstraint represents a table-level CHECK constraint.
type TableCheckConstraint struct {
	Name       string     // Optional constraint name
	Expression Expression // The CHECK expression
}

func (c TableCheckConstraint) String() string {
	if c.Name != "" {
		return fmt.Sprintf("CONSTRAINT %s CHECK (%s)", c.Name, c.Expression.String())
	}
	return fmt.Sprintf("CHECK (%s)", c.Expression.String())
}

// TableUniqueConstraint represents a table-level UNIQUE constraint.
type TableUniqueConstraint struct {
	Columns []string
}

func (c TableUniqueConstraint) String() string {
	return fmt.Sprintf("UNIQUE (%s)", strings.Join(c.Columns, ", "))
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

	var valueSets []string //nolint:prealloc
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

// TableExpression represents a table expression in a FROM clause.
type TableExpression interface {
	tableExpressionNode()
	String() string
}

// TableRef represents a simple table reference with optional alias.
type TableRef struct {
	TableName string
	Alias     string
}

func (t *TableRef) tableExpressionNode() {}
func (t *TableRef) String() string {
	if t.Alias != "" {
		return fmt.Sprintf("%s AS %s", t.TableName, t.Alias)
	}
	return t.TableName
}

// SubqueryRef represents a subquery in the FROM clause with an alias.
type SubqueryRef struct {
	Query *SelectStmt
	Alias string
}

func (s *SubqueryRef) tableExpressionNode() {}
func (s *SubqueryRef) String() string {
	return fmt.Sprintf("(%s) AS %s", s.Query.String(), s.Alias)
}

// JoinType represents the type of JOIN.
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
	CrossJoin
)

func (jt JoinType) String() string {
	switch jt {
	case InnerJoin:
		return "INNER JOIN"
	case LeftJoin:
		return "LEFT JOIN"
	case RightJoin:
		return "RIGHT JOIN"
	case FullJoin:
		return "FULL JOIN"
	case CrossJoin:
		return "CROSS JOIN"
	default:
		return "UNKNOWN JOIN"
	}
}

// JoinExpr represents a JOIN expression.
type JoinExpr struct {
	Left      TableExpression
	Right     TableExpression
	JoinType  JoinType
	Condition Expression // ON condition
}

func (j *JoinExpr) tableExpressionNode() {}
func (j *JoinExpr) String() string {
	result := fmt.Sprintf("%s %s %s", j.Left.String(), j.JoinType.String(), j.Right.String())
	if j.Condition != nil {
		result += fmt.Sprintf(" ON %s", j.Condition.String())
	}
	return result
}

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	With    []CommonTableExpr // CTE definitions
	Columns []SelectColumn
	From    TableExpression
	Where   Expression
	GroupBy []Expression
	Having  Expression
	OrderBy []OrderByClause
	Limit   *int
	Offset  *int
}

// CommonTableExpr represents a Common Table Expression (CTE).
type CommonTableExpr struct {
	Name  string      // Name of the CTE
	Query *SelectStmt // The query that defines the CTE
}

func (s *SelectStmt) statementNode() {}
func (s *SelectStmt) String() string {
	var parts []string

	// WITH clause (CTEs)
	if len(s.With) > 0 {
		var ctes []string
		for _, cte := range s.With {
			ctes = append(ctes, fmt.Sprintf("%s AS (%s)", cte.Name, cte.Query.String()))
		}
		parts = append(parts, fmt.Sprintf("WITH %s", strings.Join(ctes, ", ")))
	}

	// SELECT clause
	var cols []string //nolint:prealloc
	for _, col := range s.Columns {
		cols = append(cols, col.String())
	}
	parts = append(parts, fmt.Sprintf("SELECT %s", strings.Join(cols, ", ")))

	// FROM clause (only if present)
	if s.From != nil {
		parts = append(parts, fmt.Sprintf("FROM %s", s.From.String()))
	}

	// WHERE clause
	if s.Where != nil {
		parts = append(parts, fmt.Sprintf("WHERE %s", s.Where.String()))
	}

	// GROUP BY clause
	if len(s.GroupBy) > 0 {
		var groupCols []string
		for _, g := range s.GroupBy {
			groupCols = append(groupCols, g.String())
		}
		parts = append(parts, fmt.Sprintf("GROUP BY %s", strings.Join(groupCols, ", ")))
	}

	// HAVING clause
	if s.Having != nil {
		parts = append(parts, fmt.Sprintf("HAVING %s", s.Having.String()))
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
	Name  string
	Table string // Optional table qualifier
}

func (i *Identifier) expressionNode() {}
func (i *Identifier) String() string {
	if i.Table != "" {
		return fmt.Sprintf("%s.%s", i.Table, i.Name)
	}
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
	Expr     Expression
	Values   []Expression  // Either Values or Subquery is set
	Subquery *SubqueryExpr // Either Values or Subquery is set
	Not      bool
}

func (i *InExpr) expressionNode() {}
func (i *InExpr) String() string {
	if i.Subquery != nil {
		if i.Not {
			return fmt.Sprintf("%s NOT IN %s", i.Expr.String(), i.Subquery.String())
		}
		return fmt.Sprintf("%s IN %s", i.Expr.String(), i.Subquery.String())
	}

	var values []string //nolint:prealloc
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

// SubqueryExpr represents a subquery expression.
type SubqueryExpr struct {
	Query *SelectStmt
}

func (s *SubqueryExpr) expressionNode() {}
func (s *SubqueryExpr) String() string {
	return fmt.Sprintf("(%s)", s.Query.String())
}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Subquery *SubqueryExpr
	Not      bool
}

func (e *ExistsExpr) expressionNode() {}
func (e *ExistsExpr) String() string {
	if e.Not {
		return fmt.Sprintf("NOT EXISTS %s", e.Subquery.String())
	}
	return fmt.Sprintf("EXISTS %s", e.Subquery.String())
}

// FunctionCall represents a function call expression.
type FunctionCall struct {
	Name     string
	Args     []Expression
	Distinct bool // For aggregate functions like COUNT(DISTINCT x)
}

func (f *FunctionCall) expressionNode() {}
func (f *FunctionCall) String() string {
	var args []string
	for _, arg := range f.Args {
		args = append(args, arg.String())
	}
	
	distinct := ""
	if f.Distinct {
		distinct = "DISTINCT "
	}
	
	return fmt.Sprintf("%s(%s%s)", f.Name, distinct, strings.Join(args, ", "))
}

// ExtractExpr represents an EXTRACT(field FROM expression) expression.
type ExtractExpr struct {
	Field string     // YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
	From  Expression // The date/timestamp expression to extract from
}

func (e *ExtractExpr) expressionNode() {}
func (e *ExtractExpr) String() string {
	return fmt.Sprintf("EXTRACT(%s FROM %s)", e.Field, e.From.String())
}

// SubstringExpr represents a SUBSTRING function call.
type SubstringExpr struct {
	Str    Expression
	Start  Expression
	Length Expression // Can be nil
}

func (e *SubstringExpr) expressionNode() {}
func (e *SubstringExpr) String() string {
	if e.Length != nil {
		return fmt.Sprintf("SUBSTRING(%s FROM %s FOR %s)", e.Str.String(), e.Start.String(), e.Length.String())
	}
	return fmt.Sprintf("SUBSTRING(%s FROM %s)", e.Str.String(), e.Start.String())
}

// AnalyzeStmt represents an ANALYZE statement.
type AnalyzeStmt struct {
	TableName string
	Columns   []string // Empty means analyze all columns
}

func (s *AnalyzeStmt) statementNode() {}
func (s *AnalyzeStmt) String() string {
	if len(s.Columns) > 0 {
		return fmt.Sprintf("ANALYZE %s (%s)", s.TableName, strings.Join(s.Columns, ", "))
	}
	return fmt.Sprintf("ANALYZE %s", s.TableName)
}

// VacuumStmt represents a VACUUM statement.
type VacuumStmt struct {
	TableName string // Empty means vacuum all tables
	Analyze   bool   // True for VACUUM ANALYZE
}

func (s *VacuumStmt) statementNode() {}
func (s *VacuumStmt) String() string {
	cmd := "VACUUM"
	if s.Analyze {
		cmd = "VACUUM ANALYZE"
	}
	if s.TableName != "" {
		return fmt.Sprintf("%s %s", cmd, s.TableName)
	}
	return cmd
}

// CopyStmt represents a COPY statement for bulk data loading/export.
// Examples:
// - COPY table FROM STDIN
// - COPY table TO STDOUT
// - COPY table (col1, col2) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
type CopyStmt struct {
	TableName   string
	Columns     []string          // Optional column list
	Direction   CopyDirection     // FROM or TO
	Source      string            // STDIN, STDOUT, or filename
	Options     map[string]string // WITH options like FORMAT, DELIMITER, etc.
}

// CopyDirection represents the direction of COPY (FROM or TO)
type CopyDirection int

const (
	CopyFrom CopyDirection = iota
	CopyTo
)

func (s *CopyStmt) statementNode() {}
func (s *CopyStmt) String() string {
	var parts []string
	parts = append(parts, "COPY", s.TableName)
	
	if len(s.Columns) > 0 {
		parts = append(parts, "("+strings.Join(s.Columns, ", ")+")")
	}
	
	if s.Direction == CopyFrom {
		parts = append(parts, "FROM")
	} else {
		parts = append(parts, "TO")
	}
	
	parts = append(parts, s.Source)
	
	if len(s.Options) > 0 {
		var opts []string
		for k, v := range s.Options {
			if v == "" {
				opts = append(opts, k)
			} else {
				opts = append(opts, fmt.Sprintf("%s %s", k, v))
			}
		}
		parts = append(parts, "WITH ("+strings.Join(opts, ", ")+")")
	}
	
	return strings.Join(parts, " ")
}

// CaseExpr represents a CASE expression.
// Supports both simple and searched CASE forms:
// - Simple: CASE expr WHEN val1 THEN result1 WHEN val2 THEN result2 ELSE default END
// - Searched: CASE WHEN cond1 THEN result1 WHEN cond2 THEN result2 ELSE default END
type CaseExpr struct {
	Expr     Expression    // nil for searched CASE
	WhenList []WhenClause  // List of WHEN clauses
	Else     Expression    // Optional ELSE expression
}

func (c *CaseExpr) expressionNode() {}
func (c *CaseExpr) String() string {
	var parts []string
	parts = append(parts, "CASE")
	
	if c.Expr != nil {
		parts = append(parts, c.Expr.String())
	}
	
	for _, when := range c.WhenList {
		parts = append(parts, when.String())
	}
	
	if c.Else != nil {
		parts = append(parts, "ELSE", c.Else.String())
	}
	
	parts = append(parts, "END")
	return strings.Join(parts, " ")
}

// WhenClause represents a WHEN condition THEN result clause in a CASE expression.
type WhenClause struct {
	Condition Expression // For searched CASE or value for simple CASE
	Result    Expression // Result expression
}

func (w WhenClause) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", w.Condition.String(), w.Result.String())
}

// PrepareStmt represents a PREPARE statement.
// Example: PREPARE stmt_name [(type1, type2, ...)] AS SELECT ...
type PrepareStmt struct {
	Name       string            // Statement name
	ParamTypes []types.DataType  // Optional parameter type list
	Query      Statement         // The statement to prepare
}

func (s *PrepareStmt) statementNode() {}
func (s *PrepareStmt) String() string {
	var parts []string
	parts = append(parts, "PREPARE", s.Name)
	
	if len(s.ParamTypes) > 0 {
		var types []string
		for _, t := range s.ParamTypes {
			types = append(types, t.Name())
		}
		parts = append(parts, "("+strings.Join(types, ", ")+")")
	}
	
	parts = append(parts, "AS", s.Query.String())
	return strings.Join(parts, " ")
}

// ExecuteStmt represents an EXECUTE statement.
// Example: EXECUTE stmt_name (param1, param2, ...)
type ExecuteStmt struct {
	Name   string       // Statement name
	Params []Expression // Parameter values
}

func (s *ExecuteStmt) statementNode() {}
func (s *ExecuteStmt) String() string {
	parts := []string{"EXECUTE", s.Name}
	
	if len(s.Params) > 0 {
		var params []string
		for _, p := range s.Params {
			params = append(params, p.String())
		}
		parts = append(parts, "("+strings.Join(params, ", ")+")")
	}
	
	return strings.Join(parts, " ")
}

// DeallocateStmt represents a DEALLOCATE statement.
// Example: DEALLOCATE [PREPARE] stmt_name
type DeallocateStmt struct {
	Name string // Statement name
}

func (s *DeallocateStmt) statementNode() {}
func (s *DeallocateStmt) String() string {
	return fmt.Sprintf("DEALLOCATE %s", s.Name)
}
