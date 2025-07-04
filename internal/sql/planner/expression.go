package planner

import (
	"fmt"
	"strings"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Expression represents an expression in a query plan.
type Expression interface {
	// String returns a string representation.
	String() string
	// DataType returns the data type of the expression.
	DataType() types.DataType
	// Accept accepts a visitor.
	Accept(visitor ExpressionVisitor) error
}

// ExpressionVisitor visits expressions.
type ExpressionVisitor interface {
	VisitColumnRef(expr *ColumnRef) error
	VisitLiteral(expr *Literal) error
	VisitBinaryOp(expr *BinaryOp) error
	VisitUnaryOp(expr *UnaryOp) error
	VisitFunctionCall(expr *FunctionCall) error
	VisitExtract(expr *ExtractExpr) error
	VisitAggregate(expr *AggregateExpr) error
	VisitStar(expr *Star) error
	VisitParameterRef(expr *ParameterRef) error
	VisitSubquery(expr *SubqueryExpr) error
	VisitExists(expr *ExistsExpr) error
	VisitIn(expr *InExpr) error
	VisitCase(expr *CaseExpr) error
}

// ColumnRef represents a reference to a column.
type ColumnRef struct {
	TableAlias string
	ColumnName string
	ColumnType types.DataType
}

func (c *ColumnRef) String() string {
	if c.TableAlias != "" {
		return fmt.Sprintf("%s.%s", c.TableAlias, c.ColumnName)
	}
	return c.ColumnName
}

func (c *ColumnRef) DataType() types.DataType {
	return c.ColumnType
}

func (c *ColumnRef) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitColumnRef(c)
}

// Literal represents a literal value.
type Literal struct {
	Value types.Value
	Type  types.DataType
}

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

func (l *Literal) DataType() types.DataType {
	return l.Type
}

func (l *Literal) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitLiteral(l)
}

// ParameterRef represents a parameter placeholder like $1, $2.
type ParameterRef struct {
	Index int            // 1-based parameter index
	Type  types.DataType // Type may be unknown initially
}

func (p *ParameterRef) String() string {
	return fmt.Sprintf("$%d", p.Index)
}

func (p *ParameterRef) DataType() types.DataType {
	if p.Type == nil {
		return types.Unknown
	}
	return p.Type
}

func (p *ParameterRef) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitParameterRef(p)
}

// BinaryOp represents a binary operation.
type BinaryOp struct {
	Left     Expression
	Right    Expression
	Operator BinaryOperator
	Type     types.DataType
}

// BinaryOperator represents a binary operator.
type BinaryOperator int

const (
	// Arithmetic operators
	OpAdd BinaryOperator = iota
	OpSubtract
	OpMultiply
	OpDivide
	OpModulo

	// Comparison operators
	OpEqual
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual

	// Logical operators
	OpAnd
	OpOr

	// String operators
	OpConcat
	OpLike
	OpNotLike

	// Other operators
	OpIn
	OpNotIn
	OpIs
	OpIsNot
)

func (op BinaryOperator) String() string {
	switch op {
	case OpAdd:
		return "+"
	case OpSubtract:
		return "-"
	case OpMultiply:
		return "*"
	case OpDivide:
		return "/"
	case OpModulo:
		return "%"
	case OpEqual:
		return "="
	case OpNotEqual:
		return "!="
	case OpLess:
		return "<"
	case OpLessEqual:
		return "<="
	case OpGreater:
		return ">"
	case OpGreaterEqual:
		return ">="
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpConcat:
		return "||"
	case OpLike:
		return "LIKE"
	case OpNotLike:
		return "NOT LIKE"
	case OpIn:
		return "IN"
	case OpNotIn:
		return "NOT IN"
	case OpIs:
		return "IS"
	case OpIsNot:
		return "IS NOT"
	default:
		return fmt.Sprintf("Unknown(%d)", op)
	}
}

func (b *BinaryOp) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left.String(), b.Operator.String(), b.Right.String())
}

func (b *BinaryOp) DataType() types.DataType {
	return b.Type
}

func (b *BinaryOp) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitBinaryOp(b)
}

// UnaryOp represents a unary operation.
type UnaryOp struct {
	Expr     Expression
	Operator UnaryOperator
	Type     types.DataType
}

// UnaryOperator represents a unary operator.
type UnaryOperator int

const (
	OpNot UnaryOperator = iota
	OpNegate
	OpIsNull
	OpIsNotNull
)

func (op UnaryOperator) String() string {
	switch op {
	case OpNot:
		return "NOT"
	case OpNegate:
		return "-"
	case OpIsNull:
		return "IS NULL"
	case OpIsNotNull:
		return "IS NOT NULL"
	default:
		return fmt.Sprintf("Unknown(%d)", op)
	}
}

func (u *UnaryOp) String() string {
	if u.Operator == OpIsNull || u.Operator == OpIsNotNull {
		return fmt.Sprintf("%s %s", u.Expr.String(), u.Operator.String())
	}
	return fmt.Sprintf("%s %s", u.Operator.String(), u.Expr.String())
}

func (u *UnaryOp) DataType() types.DataType {
	return u.Type
}

func (u *UnaryOp) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitUnaryOp(u)
}

// FunctionCall represents a function call.
type FunctionCall struct {
	Name string
	Args []Expression
	Type types.DataType
}

func (f *FunctionCall) String() string {
	var argStrs []string //nolint:prealloc
	for _, arg := range f.Args {
		argStrs = append(argStrs, arg.String())
	}
	return fmt.Sprintf("%s(%s)", f.Name, strings.Join(argStrs, ", "))
}

func (f *FunctionCall) DataType() types.DataType {
	return f.Type
}

func (f *FunctionCall) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitFunctionCall(f)
}

// ExtractExpr represents an EXTRACT expression.
type ExtractExpr struct {
	Field string     // YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
	From  Expression // The date/timestamp expression to extract from
	Type  types.DataType
}

func (e *ExtractExpr) String() string {
	return fmt.Sprintf("EXTRACT(%s FROM %s)", e.Field, e.From.String())
}

func (e *ExtractExpr) DataType() types.DataType {
	return e.Type
}

func (e *ExtractExpr) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitExtract(e)
}

// AggregateExpr represents an aggregate expression.
type AggregateExpr struct {
	Function AggregateFunc
	Args     []Expression
	Distinct bool
	Type     types.DataType
	Alias    string
}

// AggregateFunc represents an aggregate function.
type AggregateFunc int

const (
	AggCount AggregateFunc = iota
	AggSum
	AggAvg
	AggMin
	AggMax
	AggStdDev
)

func (f AggregateFunc) String() string {
	switch f {
	case AggCount:
		return "COUNT"
	case AggSum:
		return "SUM"
	case AggAvg:
		return "AVG"
	case AggMin:
		return "MIN"
	case AggMax:
		return "MAX"
	case AggStdDev:
		return "STDDEV"
	default:
		return fmt.Sprintf("Unknown(%d)", f)
	}
}

func (a *AggregateExpr) String() string {
	var argStrs []string //nolint:prealloc
	for _, arg := range a.Args {
		argStrs = append(argStrs, arg.String())
	}

	distinct := ""
	if a.Distinct {
		distinct = "DISTINCT "
	}

	return fmt.Sprintf("%s(%s%s)", a.Function.String(), distinct, strings.Join(argStrs, ", "))
}

func (a *AggregateExpr) DataType() types.DataType {
	return a.Type
}

func (a *AggregateExpr) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitAggregate(a)
}

// Star represents the * in SELECT *.
type Star struct {
	TableAlias string
}

func (s *Star) String() string {
	if s.TableAlias != "" {
		return fmt.Sprintf("%s.*", s.TableAlias)
	}
	return "*"
}

func (s *Star) DataType() types.DataType {
	// Star doesn't have a single data type
	return nil
}

func (s *Star) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitStar(s)
}

// SubqueryExpr represents a subquery expression.
type SubqueryExpr struct {
	Subplan      LogicalPlan
	Type         types.DataType
	IsCorrelated bool
	ExternalRefs []string
}

func (s *SubqueryExpr) String() string {
	return fmt.Sprintf("(%s)", s.Subplan.String())
}

func (s *SubqueryExpr) DataType() types.DataType {
	return s.Type
}

func (s *SubqueryExpr) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitSubquery(s)
}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Subquery *SubqueryExpr
	Not      bool
}

func (e *ExistsExpr) String() string {
	if e.Not {
		return fmt.Sprintf("NOT EXISTS %s", e.Subquery.String())
	}
	return fmt.Sprintf("EXISTS %s", e.Subquery.String())
}

func (e *ExistsExpr) DataType() types.DataType {
	return types.Boolean
}

func (e *ExistsExpr) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitExists(e)
}

// InExpr represents an IN expression.
type InExpr struct {
	Expr     Expression
	Values   []Expression  // Either Values or Subquery is set
	Subquery *SubqueryExpr // Either Values or Subquery is set
	Not      bool
}

func (i *InExpr) String() string {
	if i.Subquery != nil {
		if i.Not {
			return fmt.Sprintf("%s NOT IN %s", i.Expr.String(), i.Subquery.String())
		}
		return fmt.Sprintf("%s IN %s", i.Expr.String(), i.Subquery.String())
	}

	var values []string
	for _, v := range i.Values {
		values = append(values, v.String())
	}

	if i.Not {
		return fmt.Sprintf("%s NOT IN (%s)", i.Expr.String(), strings.Join(values, ", "))
	}
	return fmt.Sprintf("%s IN (%s)", i.Expr.String(), strings.Join(values, ", "))
}

func (i *InExpr) DataType() types.DataType {
	return types.Boolean
}

func (i *InExpr) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitIn(i)
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr     Expression   // nil for searched CASE
	WhenList []WhenClause // List of WHEN clauses
	Else     Expression   // Optional ELSE expression
	Type     types.DataType
}

// WhenClause represents a WHEN condition THEN result clause.
type WhenClause struct {
	Condition Expression
	Result    Expression
}

func (c *CaseExpr) String() string {
	var parts []string
	parts = append(parts, "CASE")

	if c.Expr != nil {
		parts = append(parts, c.Expr.String())
	}

	for _, when := range c.WhenList {
		parts = append(parts, "WHEN", when.Condition.String(), "THEN", when.Result.String())
	}

	if c.Else != nil {
		parts = append(parts, "ELSE", c.Else.String())
	}

	parts = append(parts, "END")
	return strings.Join(parts, " ")
}

func (c *CaseExpr) DataType() types.DataType {
	return c.Type
}

func (c *CaseExpr) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitCase(c)
}
