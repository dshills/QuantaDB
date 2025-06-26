package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Plan represents a node in a query execution plan.
type Plan interface {
	// Children returns the child plans.
	Children() []Plan
	// Schema returns the output schema of this plan node.
	Schema() *Schema
	// String returns a string representation for debugging.
	String() string
}

// Schema represents the output schema of a plan node.
type Schema struct {
	Columns []Column
}

// Column represents a column in a schema.
type Column struct {
	Name       string
	DataType   types.DataType
	Nullable   bool
	TableName  string // Source table name
	TableAlias string // Table alias used in query
}

// LogicalPlan represents a logical plan node.
type LogicalPlan interface {
	Plan
	logicalNode()
}

// RequiredColumnsAnalyzer computes required columns for plan nodes
type RequiredColumnsAnalyzer interface {
	// RequiredColumns returns columns this node needs from its children
	RequiredColumns() *ColumnSet

	// RequiredInputColumns returns columns needed from specific child
	RequiredInputColumns(childIndex int) *ColumnSet
}

// PhysicalPlan interface is defined in physical.go to avoid circular dependencies

// JoinType represents the type of join.
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
	CrossJoin
	SemiJoin
	AntiJoin
)

func (j JoinType) String() string {
	switch j {
	case InnerJoin:
		return "INNER"
	case LeftJoin:
		return "LEFT"
	case RightJoin:
		return "RIGHT"
	case FullJoin:
		return "FULL"
	case CrossJoin:
		return "CROSS"
	case SemiJoin:
		return "SEMI"
	case AntiJoin:
		return "ANTI"
	default:
		return fmt.Sprintf("Unknown(%d)", j)
	}
}

// SortOrder represents the sort order.
type SortOrder int

const (
	Ascending SortOrder = iota
	Descending
)

func (s SortOrder) String() string {
	if s == Descending {
		return "DESC"
	}
	return "ASC"
}

// SortKey is defined in physical.go to avoid duplication

// PlanVisitor is used to traverse plan trees.
type PlanVisitor interface {
	VisitLogicalScan(plan *LogicalScan) error
	VisitLogicalFilter(plan *LogicalFilter) error
	VisitLogicalProject(plan *LogicalProject) error
	VisitLogicalSort(plan *LogicalSort) error
	VisitLogicalLimit(plan *LogicalLimit) error
	VisitLogicalJoin(plan *LogicalJoin) error
	VisitLogicalAggregate(plan *LogicalAggregate) error
}

// basePlan provides common functionality for plan nodes.
type basePlan struct {
	children []Plan
	schema   *Schema
}

func (p *basePlan) Children() []Plan {
	return p.children
}

func (p *basePlan) Schema() *Schema {
	return p.schema
}
