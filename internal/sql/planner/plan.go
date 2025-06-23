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

// PhysicalPlan represents a physical plan node.
type PhysicalPlan interface {
	Plan
	// EstimatedCost returns the estimated cost of this plan.
	EstimatedCost() Cost
	// EstimatedRows returns the estimated number of rows.
	EstimatedRows() float64
	physicalNode()
}

// Cost represents the cost of a plan.
type Cost struct {
	StartupCost float64 // Cost before first row
	TotalCost   float64 // Total execution cost
	Rows        float64 // Estimated row count
	Width       int     // Average row width in bytes
}

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

// SortKey represents a sort key.
type SortKey struct {
	Column int       // Column index in schema
	Order  SortOrder // Sort order
}

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
