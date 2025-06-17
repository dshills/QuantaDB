package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// PhysicalPlan represents a physical execution plan.
type PhysicalPlan interface {
	planner.Plan
	// Cardinality returns the estimated number of rows.
	Cardinality() int64
	// Cost returns the estimated cost of execution.
	Cost() float64
}

// PhysicalPlanner converts logical plans to physical plans.
type PhysicalPlanner struct {
	catalog CatalogProvider
	stats   StatisticsProvider
}

// CatalogProvider provides metadata about tables.
type CatalogProvider interface {
	GetTableStats(schema, table string) (*TableStats, error)
}

// StatisticsProvider provides statistics for cost estimation.
type StatisticsProvider interface {
	GetColumnStats(schema, table, column string) (*ColumnStats, error)
}

// TableStats contains table statistics.
type TableStats struct {
	RowCount    int64
	BlockCount  int64
	AvgRowSize  int64
	LastUpdated int64
}

// ColumnStats contains column statistics.
type ColumnStats struct {
	DistinctCount int64
	NullCount     int64
	MinValue      types.Value
	MaxValue      types.Value
	Histogram     []HistogramBucket
}

// HistogramBucket represents a histogram bucket.
type HistogramBucket struct {
	LowerBound types.Value
	UpperBound types.Value
	Count      int64
}

// Physical plan nodes

// PhysicalScan represents a physical table scan.
type PhysicalScan struct {
	schema      *planner.Schema
	children    []planner.Plan
	TableName   string
	Alias       string
	Predicate   planner.Expression // Optional pushed-down predicate
	cardinality int64
	cost        float64
}

func (p *PhysicalScan) String() string {
	if p.Predicate != nil {
		return fmt.Sprintf("PhysicalScan(%s, filter=%s)", p.TableName, p.Predicate.String())
	}
	return fmt.Sprintf("PhysicalScan(%s)", p.TableName)
}

func (p *PhysicalScan) Children() []planner.Plan { return p.children }
func (p *PhysicalScan) Schema() *planner.Schema  { return p.schema }
func (p *PhysicalScan) Cardinality() int64       { return p.cardinality }
func (p *PhysicalScan) Cost() float64            { return p.cost }

// PhysicalNestedLoopJoin represents a nested loop join.
type PhysicalNestedLoopJoin struct {
	schema      *planner.Schema
	children    []planner.Plan
	JoinType    JoinType
	Condition   planner.Expression
	cardinality int64
	cost        float64
}

func (p *PhysicalNestedLoopJoin) String() string {
	return fmt.Sprintf("PhysicalNestedLoopJoin(%s, %s)", joinTypeString(p.JoinType), p.Condition.String())
}

func (p *PhysicalNestedLoopJoin) Children() []planner.Plan { return p.children }
func (p *PhysicalNestedLoopJoin) Schema() *planner.Schema  { return p.schema }
func (p *PhysicalNestedLoopJoin) Cardinality() int64       { return p.cardinality }
func (p *PhysicalNestedLoopJoin) Cost() float64            { return p.cost }

// PhysicalHashJoin represents a hash join.
type PhysicalHashJoin struct {
	schema      *planner.Schema
	children    []planner.Plan
	JoinType    JoinType
	Condition   planner.Expression
	BuildSide   BuildSide // Which side to build hash table
	cardinality int64
	cost        float64
}

// BuildSide indicates which side of the join to build the hash table.
type BuildSide int

const (
	BuildLeft BuildSide = iota
	BuildRight
)

func (p *PhysicalHashJoin) String() string {
	side := "right"
	if p.BuildSide == BuildLeft {
		side = "left"
	}
	return fmt.Sprintf("PhysicalHashJoin(%s, %s, build=%s)", joinTypeString(p.JoinType), p.Condition.String(), side)
}

func (p *PhysicalHashJoin) Children() []planner.Plan { return p.children }
func (p *PhysicalHashJoin) Schema() *planner.Schema  { return p.schema }
func (p *PhysicalHashJoin) Cardinality() int64       { return p.cardinality }
func (p *PhysicalHashJoin) Cost() float64            { return p.cost }

// PhysicalSort represents a sort operation.
type PhysicalSort struct {
	schema      *planner.Schema
	children    []planner.Plan
	OrderBy     []OrderByExpr
	cardinality int64
	cost        float64
}

func (p *PhysicalSort) String() string {
	return fmt.Sprintf("PhysicalSort(%v)", p.OrderBy)
}

func (p *PhysicalSort) Children() []planner.Plan { return p.children }
func (p *PhysicalSort) Schema() *planner.Schema  { return p.schema }
func (p *PhysicalSort) Cardinality() int64       { return p.cardinality }
func (p *PhysicalSort) Cost() float64            { return p.cost }

// PhysicalHashAggregate represents hash-based aggregation.
type PhysicalHashAggregate struct {
	schema      *planner.Schema
	children    []planner.Plan
	GroupBy     []planner.Expression
	Aggregates  []planner.AggregateExpr
	cardinality int64
	cost        float64
}

func (p *PhysicalHashAggregate) String() string {
	return fmt.Sprintf("PhysicalHashAggregate(groups=%d, aggs=%d)", len(p.GroupBy), len(p.Aggregates))
}

func (p *PhysicalHashAggregate) Children() []planner.Plan { return p.children }
func (p *PhysicalHashAggregate) Schema() *planner.Schema  { return p.schema }
func (p *PhysicalHashAggregate) Cardinality() int64       { return p.cardinality }
func (p *PhysicalHashAggregate) Cost() float64            { return p.cost }

// NewPhysicalPlanner creates a new physical planner.
func NewPhysicalPlanner(catalog CatalogProvider, stats StatisticsProvider) *PhysicalPlanner {
	return &PhysicalPlanner{
		catalog: catalog,
		stats:   stats,
	}
}

// OptimizePlan converts a logical plan to an optimal physical plan.
func (pp *PhysicalPlanner) OptimizePlan(logical planner.LogicalPlan) (PhysicalPlan, error) {
	return pp.optimizeNode(logical)
}

// optimizeNode recursively optimizes a logical plan node.
func (pp *PhysicalPlanner) optimizeNode(logical planner.LogicalPlan) (PhysicalPlan, error) {
	switch node := logical.(type) {
	case *planner.LogicalScan:
		return pp.optimizeScan(node)
	case *planner.LogicalFilter:
		return pp.optimizeFilter(node)
	case *planner.LogicalProject:
		return pp.optimizeProject(node)
	case *planner.LogicalSort:
		return pp.optimizeSort(node)
	case *planner.LogicalLimit:
		return pp.optimizeLimit(node)
	case *planner.LogicalJoin:
		return pp.optimizeJoin(node)
	case *planner.LogicalAggregate:
		return pp.optimizeAggregate(node)
	default:
		return nil, fmt.Errorf("unsupported logical plan node: %T", logical)
	}
}

// optimizeScan optimizes a table scan.
func (pp *PhysicalPlanner) optimizeScan(scan *planner.LogicalScan) (PhysicalPlan, error) {
	// Get table statistics
	stats, err := pp.catalog.GetTableStats("public", scan.TableName)
	if err != nil {
		// Use default estimates if stats unavailable
		stats = &TableStats{
			RowCount:   1000,
			BlockCount: 10,
			AvgRowSize: 100,
		}
	}

	// Estimate cost as sequential scan cost
	// Cost = (blocks to read) * (cost per block read)
	cost := float64(stats.BlockCount) * 1.0

	return &PhysicalScan{
		schema:      scan.Schema(),
		children:    []planner.Plan{},
		TableName:   scan.TableName,
		Alias:       scan.Alias,
		cardinality: stats.RowCount,
		cost:        cost,
	}, nil
}

// optimizeFilter optimizes a filter operation.
func (pp *PhysicalPlanner) optimizeFilter(filter *planner.LogicalFilter) (PhysicalPlan, error) {
	// Optimize child first
	child, err := pp.optimizeNode(filter.Children()[0].(planner.LogicalPlan))
	if err != nil {
		return nil, err
	}

	// Try to push filter down to scan
	if scan, ok := child.(*PhysicalScan); ok && scan.Predicate == nil {
		// Push filter down to scan level
		scan.Predicate = filter.Predicate
		// Update cardinality with selectivity estimate
		selectivity := pp.estimateSelectivity(filter.Predicate)
		scan.cardinality = int64(float64(scan.cardinality) * selectivity)
		return scan, nil
	}

	// Cannot push down, create separate filter node
	// For now, just return the child (filter is incorporated in scan)
	return child, nil
}

// optimizeProject optimizes a projection operation.
func (pp *PhysicalPlanner) optimizeProject(project *planner.LogicalProject) (PhysicalPlan, error) {
	// Project operations are typically merged into other operators
	// For now, just optimize the child
	return pp.optimizeNode(project.Children()[0].(planner.LogicalPlan))
}

// optimizeSort optimizes a sort operation.
func (pp *PhysicalPlanner) optimizeSort(sort *planner.LogicalSort) (PhysicalPlan, error) {
	child, err := pp.optimizeNode(sort.Children()[0].(planner.LogicalPlan))
	if err != nil {
		return nil, err
	}

	// Cost = child cost + sort cost
	// Sort cost = N * log(N) * cost_per_comparison
	cardinality := child.Cardinality()
	sortCost := float64(cardinality) * pp.logBase2(float64(cardinality)) * 0.01
	totalCost := child.Cost() + sortCost

	// Convert planner OrderByExpr to executor OrderByExpr
	orderBy := make([]OrderByExpr, len(sort.OrderBy))
	for i, expr := range sort.OrderBy {
		orderBy[i] = OrderByExpr{
			Expr:  nil, // Will need expression evaluator conversion
			Order: expr.Order,
		}
	}

	return &PhysicalSort{
		schema:      sort.Schema(),
		children:    []planner.Plan{child},
		OrderBy:     orderBy,
		cardinality: cardinality,
		cost:        totalCost,
	}, nil
}

// optimizeLimit optimizes a limit operation.
func (pp *PhysicalPlanner) optimizeLimit(limit *planner.LogicalLimit) (PhysicalPlan, error) {
	child, err := pp.optimizeNode(limit.Children()[0].(planner.LogicalPlan))
	if err != nil {
		return nil, err
	}

	// Limit reduces cardinality
	cardinality := limit.Limit + limit.Offset
	if cardinality > child.Cardinality() {
		cardinality = child.Cardinality()
	}

	// Cost is same as child since we stop early
	return &PhysicalSort{ // Reuse sort structure for now
		schema:      limit.Schema(),
		children:    []planner.Plan{child},
		cardinality: cardinality,
		cost:        child.Cost(),
	}, nil
}

// optimizeJoin optimizes a join operation.
func (pp *PhysicalPlanner) optimizeJoin(join *planner.LogicalJoin) (PhysicalPlan, error) {
	if len(join.Children()) != 2 {
		return nil, fmt.Errorf("join must have exactly 2 children")
	}

	left, err := pp.optimizeNode(join.Children()[0].(planner.LogicalPlan))
	if err != nil {
		return nil, err
	}

	right, err := pp.optimizeNode(join.Children()[1].(planner.LogicalPlan))
	if err != nil {
		return nil, err
	}

	// Choose join algorithm based on cardinalities
	leftCard := left.Cardinality()
	rightCard := right.Cardinality()

	// If one side is much smaller, use hash join with smaller side as build
	if pp.isEquiJoin(join.Condition) {
		if leftCard < rightCard && leftCard < 10000 {
			// Hash join with left as build side
			hashCost := left.Cost() + right.Cost() + float64(leftCard)*0.01 + float64(rightCard)*0.005
			return &PhysicalHashJoin{
				schema:      join.Schema(),
				children:    []planner.Plan{left, right},
				JoinType:    pp.convertJoinType(join.JoinType),
				Condition:   join.Condition,
				BuildSide:   BuildLeft,
				cardinality: pp.estimateJoinCardinality(leftCard, rightCard),
				cost:        hashCost,
			}, nil
		} else if rightCard < leftCard && rightCard < 10000 {
			// Hash join with right as build side
			hashCost := left.Cost() + right.Cost() + float64(rightCard)*0.01 + float64(leftCard)*0.005
			return &PhysicalHashJoin{
				schema:      join.Schema(),
				children:    []planner.Plan{left, right},
				JoinType:    pp.convertJoinType(join.JoinType),
				Condition:   join.Condition,
				BuildSide:   BuildRight,
				cardinality: pp.estimateJoinCardinality(leftCard, rightCard),
				cost:        hashCost,
			}, nil
		}
	}

	// Default to nested loop join
	nlCost := left.Cost() + float64(leftCard)*right.Cost()
	return &PhysicalNestedLoopJoin{
		schema:      join.Schema(),
		children:    []planner.Plan{left, right},
		JoinType:    pp.convertJoinType(join.JoinType),
		Condition:   join.Condition,
		cardinality: pp.estimateJoinCardinality(leftCard, rightCard),
		cost:        nlCost,
	}, nil
}

// optimizeAggregate optimizes an aggregate operation.
func (pp *PhysicalPlanner) optimizeAggregate(agg *planner.LogicalAggregate) (PhysicalPlan, error) {
	child, err := pp.optimizeNode(agg.Children()[0].(planner.LogicalPlan))
	if err != nil {
		return nil, err
	}

	// Estimate number of groups
	var groupCount int64 = 1
	if len(agg.GroupBy) > 0 {
		// Rough estimate: assume each group by column reduces cardinality by factor of 10
		groupCount = child.Cardinality()
		for range agg.GroupBy {
			groupCount = groupCount / 10
			if groupCount < 1 {
				groupCount = 1
				break
			}
		}
	}

	// Cost = child cost + hash aggregation cost
	aggCost := child.Cost() + float64(child.Cardinality())*0.01

	return &PhysicalHashAggregate{
		schema:      agg.Schema(),
		children:    []planner.Plan{child},
		GroupBy:     agg.GroupBy,
		Aggregates:  agg.Aggregates,
		cardinality: groupCount,
		cost:        aggCost,
	}, nil
}

// Helper methods

// estimateSelectivity estimates the selectivity of a predicate.
func (pp *PhysicalPlanner) estimateSelectivity(predicate planner.Expression) float64 {
	// Very simple selectivity estimation
	// In a real system, this would analyze the predicate structure
	return 0.1 // Assume 10% selectivity
}

// isEquiJoin checks if a join condition is an equi-join.
func (pp *PhysicalPlanner) isEquiJoin(condition planner.Expression) bool {
	if binOp, ok := condition.(*planner.BinaryOp); ok {
		return binOp.Operator == planner.OpEqual
	}
	return false
}

// estimateJoinCardinality estimates the cardinality of a join.
func (pp *PhysicalPlanner) estimateJoinCardinality(leftCard, rightCard int64) int64 {
	// Simple estimate: assume joins are selective
	result := (leftCard * rightCard) / 10
	if result < 1 {
		result = 1
	}
	return result
}

// convertJoinType converts planner join type to executor join type.
func (pp *PhysicalPlanner) convertJoinType(joinType planner.JoinType) JoinType {
	switch joinType {
	case planner.InnerJoin:
		return InnerJoin
	case planner.LeftJoin:
		return LeftJoin
	case planner.RightJoin:
		return RightJoin
	case planner.FullJoin:
		return FullJoin
	case planner.CrossJoin:
		return InnerJoin // Cross join is inner join without predicate
	default:
		return InnerJoin
	}
}

// logBase2 computes log base 2.
func (pp *PhysicalPlanner) logBase2(x float64) float64 {
	if x <= 1 {
		return 1
	}
	// Simple approximation
	result := 1.0
	for x > 2 {
		x = x / 2
		result++
	}
	return result
}

// joinTypeString returns a string representation of a join type.
func joinTypeString(joinType JoinType) string {
	switch joinType {
	case InnerJoin:
		return "INNER"
	case LeftJoin:
		return "LEFT"
	case RightJoin:
		return "RIGHT"
	case FullJoin:
		return "FULL"
	default:
		return fmt.Sprintf("Unknown(%d)", joinType)
	}
}
