package planner

import (
	"fmt"
	"strings"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// LogicalScan represents a table scan operation.
type LogicalScan struct {
	basePlan
	TableName string
	Alias     string
}

func (s *LogicalScan) logicalNode() {}

func (s *LogicalScan) String() string {
	if s.Alias != "" && s.Alias != s.TableName {
		return fmt.Sprintf("Scan(%s AS %s)", s.TableName, s.Alias)
	}
	return fmt.Sprintf("Scan(%s)", s.TableName)
}

// LogicalFilter represents a filter operation.
type LogicalFilter struct {
	basePlan
	Predicate Expression
}

func (f *LogicalFilter) logicalNode() {}

func (f *LogicalFilter) String() string {
	return fmt.Sprintf("Filter(%s)", f.Predicate.String())
}

// LogicalProject represents a projection operation.
type LogicalProject struct {
	basePlan
	Projections []Expression
	Aliases     []string
}

func (p *LogicalProject) logicalNode() {}

func (p *LogicalProject) String() string {
	var projStrs []string //nolint:prealloc
	for i, proj := range p.Projections {
		str := proj.String()
		if i < len(p.Aliases) && p.Aliases[i] != "" {
			str += " AS " + p.Aliases[i]
		}
		projStrs = append(projStrs, str)
	}
	return fmt.Sprintf("Project(%s)", strings.Join(projStrs, ", "))
}

// LogicalSort represents a sort operation.
type LogicalSort struct {
	basePlan
	OrderBy []OrderByExpr
}

func (s *LogicalSort) logicalNode() {}

func (s *LogicalSort) String() string {
	var orderStrs []string //nolint:prealloc
	for _, o := range s.OrderBy {
		orderStrs = append(orderStrs, o.String())
	}
	return fmt.Sprintf("Sort(%s)", strings.Join(orderStrs, ", "))
}

// OrderByExpr represents an ORDER BY expression.
type OrderByExpr struct {
	Expr  Expression
	Order SortOrder
}

func (o OrderByExpr) String() string {
	return fmt.Sprintf("%s %s", o.Expr.String(), o.Order.String())
}

// LogicalLimit represents a limit operation.
type LogicalLimit struct {
	basePlan
	Limit  int64
	Offset int64
}

func (l *LogicalLimit) logicalNode() {}

func (l *LogicalLimit) String() string {
	if l.Offset > 0 {
		return fmt.Sprintf("Limit(%d, %d)", l.Limit, l.Offset)
	}
	return fmt.Sprintf("Limit(%d)", l.Limit)
}

// LogicalJoin represents a join operation.
type LogicalJoin struct {
	basePlan
	JoinType  JoinType
	Condition Expression
}

func (j *LogicalJoin) logicalNode() {}

func (j *LogicalJoin) String() string {
	return fmt.Sprintf("%sJoin(%s)", j.JoinType.String(), j.Condition.String())
}

// LogicalAggregate represents an aggregation operation.
type LogicalAggregate struct {
	basePlan
	GroupBy    []Expression
	Aggregates []AggregateExpr
}

func (a *LogicalAggregate) logicalNode() {}

func (a *LogicalAggregate) String() string {
	var parts []string

	if len(a.GroupBy) > 0 {
		var groupStrs []string
		for _, g := range a.GroupBy {
			groupStrs = append(groupStrs, g.String())
		}
		parts = append(parts, "GROUP BY "+strings.Join(groupStrs, ", "))
	}

	if len(a.Aggregates) > 0 {
		var aggStrs []string
		for _, agg := range a.Aggregates {
			aggStrs = append(aggStrs, agg.String())
		}
		parts = append(parts, strings.Join(aggStrs, ", "))
	}

	return fmt.Sprintf("Aggregate(%s)", strings.Join(parts, " "))
}

// NewLogicalScan creates a new logical scan node.
func NewLogicalScan(tableName, alias string, schema *Schema) *LogicalScan {
	return &LogicalScan{
		basePlan: basePlan{
			schema: schema,
		},
		TableName: tableName,
		Alias:     alias,
	}
}

// NewLogicalFilter creates a new logical filter node.
func NewLogicalFilter(child LogicalPlan, predicate Expression) *LogicalFilter {
	return &LogicalFilter{
		basePlan: basePlan{
			children: []Plan{child},
			schema:   child.Schema(),
		},
		Predicate: predicate,
	}
}

// NewLogicalProject creates a new logical project node.
func NewLogicalProject(child LogicalPlan, projections []Expression, aliases []string, schema *Schema) *LogicalProject {
	return &LogicalProject{
		basePlan: basePlan{
			children: []Plan{child},
			schema:   schema,
		},
		Projections: projections,
		Aliases:     aliases,
	}
}

// NewLogicalSort creates a new logical sort node.
func NewLogicalSort(child LogicalPlan, orderBy []OrderByExpr) *LogicalSort {
	return &LogicalSort{
		basePlan: basePlan{
			children: []Plan{child},
			schema:   child.Schema(),
		},
		OrderBy: orderBy,
	}
}

// NewLogicalLimit creates a new logical limit node.
func NewLogicalLimit(child LogicalPlan, limit, offset int64) *LogicalLimit {
	return &LogicalLimit{
		basePlan: basePlan{
			children: []Plan{child},
			schema:   child.Schema(),
		},
		Limit:  limit,
		Offset: offset,
	}
}

// NewLogicalJoin creates a new logical join node.
func NewLogicalJoin(left, right LogicalPlan, joinType JoinType, condition Expression, schema *Schema) *LogicalJoin {
	return &LogicalJoin{
		basePlan: basePlan{
			children: []Plan{left, right},
			schema:   schema,
		},
		JoinType:  joinType,
		Condition: condition,
	}
}

// NewLogicalAggregate creates a new logical aggregate node.
func NewLogicalAggregate(child LogicalPlan, groupBy []Expression, aggregates []AggregateExpr, schema *Schema) *LogicalAggregate {
	return &LogicalAggregate{
		basePlan: basePlan{
			children: []Plan{child},
			schema:   schema,
		},
		GroupBy:    groupBy,
		Aggregates: aggregates,
	}
}

// LogicalValues represents a plan node that returns constant values
// Used for queries like SELECT 1, SELECT 'hello' that don't have a FROM clause
type LogicalValues struct {
	basePlan
	Rows [][]types.Value
}

func (v *LogicalValues) logicalNode() {}

func (v *LogicalValues) String() string {
	return fmt.Sprintf("Values(%d rows)", len(v.Rows))
}

// NewLogicalValues creates a new logical values node.
func NewLogicalValues(rows [][]types.Value, schema *Schema) *LogicalValues {
	return &LogicalValues{
		basePlan: basePlan{
			schema: schema,
		},
		Rows: rows,
	}
}

// LogicalCTE represents a Common Table Expression (CTE).
type LogicalCTE struct {
	basePlan
	Name string      // Name of the CTE
	Plan LogicalPlan // The plan that defines the CTE
}

func (c *LogicalCTE) logicalNode() {}

func (c *LogicalCTE) String() string {
	return fmt.Sprintf("CTE(%s)", c.Name)
}

// NewLogicalCTE creates a new logical CTE node.
func NewLogicalCTE(name string, plan LogicalPlan, schema *Schema) *LogicalCTE {
	return &LogicalCTE{
		basePlan: basePlan{
			children: []Plan{plan},
			schema:   schema,
		},
		Name: name,
		Plan: plan,
	}
}

// LogicalWithClause represents a WITH clause containing multiple CTEs.
type LogicalWithClause struct {
	basePlan
	CTEs []LogicalCTE // The CTE definitions
	Main LogicalPlan  // The main query
}

func (w *LogicalWithClause) logicalNode() {}

func (w *LogicalWithClause) String() string {
	var cteNames []string
	for _, cte := range w.CTEs {
		cteNames = append(cteNames, cte.Name)
	}
	return fmt.Sprintf("WITH(%s)", strings.Join(cteNames, ", "))
}

// NewLogicalWithClause creates a new logical WITH clause node.
func NewLogicalWithClause(ctes []LogicalCTE, main LogicalPlan, schema *Schema) *LogicalWithClause {
	children := make([]Plan, len(ctes)+1)
	for i, cte := range ctes {
		children[i] = &cte
	}
	children[len(ctes)] = main

	return &LogicalWithClause{
		basePlan: basePlan{
			children: children,
			schema:   schema,
		},
		CTEs: ctes,
		Main: main,
	}
}
