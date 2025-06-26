package planner

import (
	"fmt"
	"math"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// ExecutionMode determines how an operator should be executed
type ExecutionMode int

const (
	ExecutionModeScalar ExecutionMode = iota
	ExecutionModeVectorized
	ExecutionModeAdaptive
	ExecutionModeHybrid
)

func (em ExecutionMode) String() string {
	switch em {
	case ExecutionModeScalar:
		return "scalar"
	case ExecutionModeVectorized:
		return "vectorized"
	case ExecutionModeAdaptive:
		return "adaptive"
	case ExecutionModeHybrid:
		return "hybrid"
	default:
		return fmt.Sprintf("unknown(%d)", em)
	}
}

// OperatorType identifies the type of physical operator
type OperatorType int

const (
	OperatorTypeScan OperatorType = iota
	OperatorTypeFilter
	OperatorTypeJoin
	OperatorTypeAggregate
	OperatorTypeSort
	OperatorTypeLimit
	OperatorTypeProjection
)

func (ot OperatorType) String() string {
	switch ot {
	case OperatorTypeScan:
		return "scan"
	case OperatorTypeFilter:
		return "filter"
	case OperatorTypeJoin:
		return "join"
	case OperatorTypeAggregate:
		return "aggregate"
	case OperatorTypeSort:
		return "sort"
	case OperatorTypeLimit:
		return "limit"
	case OperatorTypeProjection:
		return "projection"
	default:
		return fmt.Sprintf("unknown(%d)", ot)
	}
}

// Cost represents the estimated cost of executing an operation
type Cost struct {
	CPUCost    float64 // CPU processing cost
	IOCost     float64 // I/O operation cost
	MemoryCost float64 // Memory usage cost
	SetupCost  float64 // One-time setup cost
	TotalCost  float64 // Total estimated cost
}

// Add combines two costs
func (c *Cost) Add(other *Cost) *Cost {
	return &Cost{
		CPUCost:    c.CPUCost + other.CPUCost,
		IOCost:     c.IOCost + other.IOCost,
		MemoryCost: c.MemoryCost + other.MemoryCost,
		SetupCost:  c.SetupCost + other.SetupCost,
		TotalCost:  c.TotalCost + other.TotalCost,
	}
}

// EstimateRows estimates the number of rows from cost (temporary compatibility method)
func (c *Cost) EstimateRows() float64 {
	// Very rough estimation based on total cost
	// This should be replaced with proper cardinality estimation
	return c.TotalCost / 10.0 // Assume 10 cost units per row
}

// PhysicalPlan represents a physical execution plan
type PhysicalPlan interface {
	Plan

	// Cost estimation
	EstimateCost(context *PhysicalPlanContext) *Cost
	EstimateMemory() int64
	EstimateCardinality() int64

	// Execution properties
	GetOperatorType() OperatorType
	GetExecutionMode() ExecutionMode
	RequiresVectorization() bool

	// Plan properties
	GetInputs() []PhysicalPlan
	SetInputs(inputs []PhysicalPlan)

	// Optimization support
	GetProperties() *PhysicalProperties
	SetProperties(props *PhysicalProperties)
}

// PhysicalPlanContext provides context for physical planning
type PhysicalPlanContext struct {
	CostModel         *VectorizedCostModel
	MemoryAvailable   int64
	RuntimeStats      *RuntimeStatistics
	TableStats        map[string]*catalog.TableStats
	IndexStats        map[string]*catalog.IndexStats
	VectorizationMode VectorizationMode
	BatchSize         int
	EnableCaching     bool
}

// VectorizationMode controls how vectorization decisions are made
type VectorizationMode int

const (
	VectorizationModeDisabled VectorizationMode = iota
	VectorizationModeEnabled
	VectorizationModeAdaptive
	VectorizationModeForced
)

// RuntimeStatistics provides runtime feedback for planning decisions
type RuntimeStatistics struct {
	OperatorStats  map[string]*OperatorStats
	MemoryPressure float64
	CacheHitRate   float64
	LastUpdate     time.Time
}

// OperatorStats tracks performance metrics for specific operators
type OperatorStats struct {
	AvgExecutionTime  time.Duration
	AvgMemoryUsage    int64
	AvgRowsProcessed  int64
	VectorizationRate float64
	LastSeen          time.Time
}

// PhysicalProperties describe the output properties of a physical plan
type PhysicalProperties struct {
	EstimatedRows   int64
	OutputColumns   []*OutputColumn
	SortOrder       []SortKey
	DistributionKey []string
	HasDuplicates   bool
	IsPartitioned   bool
}

// OutputColumn describes a column in the output
type OutputColumn struct {
	Name         string
	Type         types.DataType
	IsNullable   bool
	SourceTable  string
	SourceColumn string
}

// SortKey describes a sort ordering
type SortKey struct {
	Column    string
	Direction SortDirection
}

// SortDirection represents sort ordering
type SortDirection int

const (
	SortDirectionAsc SortDirection = iota
	SortDirectionDesc
)

// BasePhysicalPlan provides common functionality for physical plans
type BasePhysicalPlan struct {
	id         int
	schema     *Schema
	inputs     []PhysicalPlan
	properties *PhysicalProperties
	memoryEst  int64
}

func (bpp *BasePhysicalPlan) ID() int         { return bpp.id }
func (bpp *BasePhysicalPlan) Schema() *Schema { return bpp.schema }
func (bpp *BasePhysicalPlan) Children() []Plan {
	children := make([]Plan, len(bpp.inputs))
	for i, input := range bpp.inputs {
		children[i] = input
	}
	return children
}
func (bpp *BasePhysicalPlan) GetInputs() []PhysicalPlan               { return bpp.inputs }
func (bpp *BasePhysicalPlan) SetInputs(inputs []PhysicalPlan)         { bpp.inputs = inputs }
func (bpp *BasePhysicalPlan) GetProperties() *PhysicalProperties      { return bpp.properties }
func (bpp *BasePhysicalPlan) SetProperties(props *PhysicalProperties) { bpp.properties = props }
func (bpp *BasePhysicalPlan) EstimateMemory() int64                   { return bpp.memoryEst }

// PhysicalScan represents a table scan operation
type PhysicalScan struct {
	*BasePhysicalPlan
	Table         *catalog.Table
	Predicate     Expression
	ExecutionMode ExecutionMode
	IndexAccess   *IndexAccessPath
	BatchSize     int
}

// IndexAccessPath describes index access strategy
type IndexAccessPath struct {
	Index       *catalog.Index
	KeyColumns  []string
	Conditions  []Expression
	Selectivity float64
}

func NewPhysicalScan(table *catalog.Table, predicate Expression) *PhysicalScan {
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name:     col.Name,
			DataType: col.DataType,
		}
	}

	return &PhysicalScan{
		BasePhysicalPlan: &BasePhysicalPlan{
			schema: schema,
		},
		Table:         table,
		Predicate:     predicate,
		ExecutionMode: ExecutionModeScalar, // Default to scalar
		BatchSize:     1024,                // Default batch size
	}
}

func (ps *PhysicalScan) GetOperatorType() OperatorType   { return OperatorTypeScan }
func (ps *PhysicalScan) GetExecutionMode() ExecutionMode { return ps.ExecutionMode }
func (ps *PhysicalScan) RequiresVectorization() bool {
	return ps.ExecutionMode == ExecutionModeVectorized || ps.ExecutionMode == ExecutionModeHybrid
}

func (ps *PhysicalScan) EstimateCost(context *PhysicalPlanContext) *Cost {
	if context == nil || context.CostModel == nil {
		return &Cost{TotalCost: 1000.0} // Default fallback
	}

	tableStats := context.TableStats[ps.Table.TableName]
	estimatedRows := int64(1000) // Default
	if tableStats != nil {
		estimatedRows = tableStats.RowCount
	}

	// Base scan cost
	baseCost := &Cost{
		IOCost:    float64(estimatedRows) * 1.0, // Default page cost
		CPUCost:   float64(estimatedRows) * context.CostModel.scalarRowCost,
		TotalCost: 0,
	}
	baseCost.TotalCost = baseCost.IOCost + baseCost.CPUCost

	// Apply vectorization if applicable
	if ps.RequiresVectorization() {
		vectorizedCost := float64(estimatedRows) * context.CostModel.vectorizedRowCost
		setupCost := context.CostModel.vectorizedSetupCost

		baseCost.CPUCost = vectorizedCost
		baseCost.SetupCost = setupCost
		baseCost.TotalCost = baseCost.IOCost + baseCost.CPUCost + baseCost.SetupCost
	}

	return baseCost
}

func (ps *PhysicalScan) EstimateCardinality() int64 {
	// Apply selectivity if predicate exists
	if ps.Predicate != nil {
		return int64(float64(ps.Table.Stats.RowCount) * 0.1) // Default selectivity
	}
	return ps.Table.Stats.RowCount
}

func (ps *PhysicalScan) String() string {
	mode := ""
	if ps.ExecutionMode != ExecutionModeScalar {
		mode = fmt.Sprintf(" [%s]", ps.ExecutionMode.String())
	}

	if ps.IndexAccess != nil {
		return fmt.Sprintf("IndexScan(%s.%s)%s", ps.Table.SchemaName, ps.Table.TableName, mode)
	}
	return fmt.Sprintf("TableScan(%s.%s)%s", ps.Table.SchemaName, ps.Table.TableName, mode)
}

// PhysicalFilter represents a filter operation
type PhysicalFilter struct {
	*BasePhysicalPlan
	Input         PhysicalPlan
	Predicate     Expression
	ExecutionMode ExecutionMode
	BatchSize     int
}

func NewPhysicalFilter(input PhysicalPlan, predicate Expression) *PhysicalFilter {
	return &PhysicalFilter{
		BasePhysicalPlan: &BasePhysicalPlan{
			schema: input.Schema(),
			inputs: []PhysicalPlan{input},
		},
		Input:         input,
		Predicate:     predicate,
		ExecutionMode: ExecutionModeScalar,
		BatchSize:     1024,
	}
}

func (pf *PhysicalFilter) GetOperatorType() OperatorType   { return OperatorTypeFilter }
func (pf *PhysicalFilter) GetExecutionMode() ExecutionMode { return pf.ExecutionMode }
func (pf *PhysicalFilter) RequiresVectorization() bool {
	return pf.ExecutionMode == ExecutionModeVectorized || pf.ExecutionMode == ExecutionModeHybrid
}

func (pf *PhysicalFilter) EstimateCost(context *PhysicalPlanContext) *Cost {
	if context == nil || context.CostModel == nil {
		return &Cost{TotalCost: 100.0}
	}

	inputCost := pf.Input.EstimateCost(context)
	inputRows := pf.Input.EstimateCardinality()

	// Filter processing cost
	filterCost := &Cost{
		CPUCost: float64(inputRows) * context.CostModel.scalarRowCost * 0.1, // 10% of row cost
	}

	// Apply vectorization if applicable
	if pf.RequiresVectorization() {
		filterCost.CPUCost = float64(inputRows) * context.CostModel.vectorizedRowCost * 0.08
		filterCost.SetupCost = context.CostModel.vectorizedSetupCost * 0.1
	}

	filterCost.TotalCost = filterCost.CPUCost + filterCost.SetupCost
	return inputCost.Add(filterCost)
}

func (pf *PhysicalFilter) EstimateCardinality() int64 {
	// Default selectivity of 10%
	return int64(float64(pf.Input.EstimateCardinality()) * 0.1)
}

func (pf *PhysicalFilter) String() string {
	mode := ""
	if pf.ExecutionMode != ExecutionModeScalar {
		mode = fmt.Sprintf(" [%s]", pf.ExecutionMode.String())
	}
	return fmt.Sprintf("Filter(%s)%s", pf.Predicate.String(), mode)
}

// JoinAlgorithm represents different join algorithms
type JoinAlgorithm int

const (
	JoinAlgorithmHash JoinAlgorithm = iota
	JoinAlgorithmNestedLoop
	JoinAlgorithmMerge
	JoinAlgorithmBroadcast
)

func (ja JoinAlgorithm) String() string {
	switch ja {
	case JoinAlgorithmHash:
		return "hash"
	case JoinAlgorithmNestedLoop:
		return "nested_loop"
	case JoinAlgorithmMerge:
		return "merge"
	case JoinAlgorithmBroadcast:
		return "broadcast"
	default:
		return fmt.Sprintf("unknown(%d)", ja)
	}
}

// PhysicalJoin represents a join operation
type PhysicalJoin struct {
	*BasePhysicalPlan
	Left, Right   PhysicalPlan
	JoinType      JoinType
	Condition     Expression
	Algorithm     JoinAlgorithm
	ExecutionMode ExecutionMode
	BatchSize     int
}

func NewPhysicalJoin(left, right PhysicalPlan, joinType JoinType, condition Expression) *PhysicalJoin {
	// Combine schemas
	leftSchema := left.Schema()
	rightSchema := right.Schema()

	combinedColumns := make([]Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns))
	combinedColumns = append(combinedColumns, leftSchema.Columns...)
	combinedColumns = append(combinedColumns, rightSchema.Columns...)

	schema := &Schema{Columns: combinedColumns}

	return &PhysicalJoin{
		BasePhysicalPlan: &BasePhysicalPlan{
			schema: schema,
			inputs: []PhysicalPlan{left, right},
		},
		Left:          left,
		Right:         right,
		JoinType:      joinType,
		Condition:     condition,
		Algorithm:     JoinAlgorithmHash, // Default
		ExecutionMode: ExecutionModeScalar,
		BatchSize:     1024,
	}
}

func (pj *PhysicalJoin) GetOperatorType() OperatorType   { return OperatorTypeJoin }
func (pj *PhysicalJoin) GetExecutionMode() ExecutionMode { return pj.ExecutionMode }
func (pj *PhysicalJoin) RequiresVectorization() bool {
	return pj.ExecutionMode == ExecutionModeVectorized || pj.ExecutionMode == ExecutionModeHybrid
}

func (pj *PhysicalJoin) EstimateCost(context *PhysicalPlanContext) *Cost {
	if context == nil || context.CostModel == nil {
		return &Cost{TotalCost: 1000.0}
	}

	leftCost := pj.Left.EstimateCost(context)
	rightCost := pj.Right.EstimateCost(context)

	leftRows := pj.Left.EstimateCardinality()
	rightRows := pj.Right.EstimateCardinality()

	// Algorithm-specific costs
	var joinCost *Cost
	switch pj.Algorithm {
	case JoinAlgorithmHash:
		// Build hash table + probe
		buildCost := float64(rightRows) * context.CostModel.scalarRowCost * 2.0
		probeCost := float64(leftRows) * context.CostModel.scalarRowCost * 1.5
		joinCost = &Cost{
			CPUCost:    buildCost + probeCost,
			MemoryCost: float64(rightRows) * 50, // Estimated hash table overhead
		}

	case JoinAlgorithmNestedLoop:
		// Nested loop cost
		joinCost = &Cost{
			CPUCost: float64(leftRows*rightRows) * context.CostModel.scalarRowCost * 0.1,
		}

	default:
		joinCost = &Cost{CPUCost: float64(leftRows+rightRows) * context.CostModel.scalarRowCost}
	}

	// Apply vectorization if applicable
	if pj.RequiresVectorization() {
		joinCost.CPUCost *= context.CostModel.vectorizedBatchFactor
		joinCost.SetupCost = context.CostModel.vectorizedSetupCost
	}

	joinCost.TotalCost = joinCost.CPUCost + joinCost.MemoryCost + joinCost.SetupCost
	return leftCost.Add(rightCost).Add(joinCost)
}

func (pj *PhysicalJoin) EstimateCardinality() int64 {
	leftRows := pj.Left.EstimateCardinality()
	rightRows := pj.Right.EstimateCardinality()

	// Simplified join cardinality estimation
	switch pj.JoinType {
	case InnerJoin:
		return int64(float64(leftRows*rightRows) * 0.1) // 10% selectivity
	case LeftJoin, RightJoin:
		return leftRows // Conservative estimate
	case FullJoin:
		return leftRows + rightRows
	case CrossJoin:
		return leftRows * rightRows
	default:
		return leftRows
	}
}

func (pj *PhysicalJoin) String() string {
	mode := ""
	if pj.ExecutionMode != ExecutionModeScalar {
		mode = fmt.Sprintf(" [%s]", pj.ExecutionMode.String())
	}
	return fmt.Sprintf("%sJoin_%s(%s)%s",
		pj.JoinType.String(),
		pj.Algorithm.String(),
		pj.Condition.String(),
		mode)
}

// PhysicalProject represents a projection operation
type PhysicalProject struct {
	*BasePhysicalPlan
	Projections   []Expression
	ExecutionMode ExecutionMode
}

func NewPhysicalProject(input PhysicalPlan, projections []Expression) *PhysicalProject {
	// Create schema from projections
	schema := &Schema{
		Columns: make([]Column, len(projections)),
	}

	for i, proj := range projections {
		switch p := proj.(type) {
		case *ColumnRef:
			schema.Columns[i] = Column{
				Name:     p.ColumnName,
				DataType: p.ColumnType,
			}
		default:
			// For expressions, use a default name and infer type
			schema.Columns[i] = Column{
				Name:     fmt.Sprintf("expr_%d", i),
				DataType: types.Text, // Default type, should be inferred properly
			}
		}
	}

	return &PhysicalProject{
		BasePhysicalPlan: &BasePhysicalPlan{
			schema: schema,
			inputs: []PhysicalPlan{input},
		},
		Projections:   projections,
		ExecutionMode: ExecutionModeScalar,
	}
}

func (pp *PhysicalProject) GetOperatorType() OperatorType   { return OperatorTypeProjection }
func (pp *PhysicalProject) GetExecutionMode() ExecutionMode { return pp.ExecutionMode }
func (pp *PhysicalProject) RequiresVectorization() bool {
	return pp.ExecutionMode == ExecutionModeVectorized || pp.ExecutionMode == ExecutionModeHybrid
}

func (pp *PhysicalProject) EstimateCost(context *PhysicalPlanContext) *Cost {
	if len(pp.inputs) == 0 {
		return &Cost{TotalCost: 0}
	}

	inputCost := pp.inputs[0].EstimateCost(context)
	cardinality := pp.inputs[0].EstimateCardinality()

	// Project cost is proportional to number of expressions and rows
	projectionCost := float64(len(pp.Projections)) * float64(cardinality) * 0.1

	if pp.RequiresVectorization() && context.CostModel != nil {
		projectionCost *= 0.8 // Vectorized expressions are more efficient
	}

	return &Cost{
		IOCost:    inputCost.IOCost,
		CPUCost:   inputCost.CPUCost + projectionCost,
		TotalCost: inputCost.TotalCost + projectionCost,
	}
}

func (pp *PhysicalProject) EstimateCardinality() int64 {
	if len(pp.inputs) == 0 {
		return 0
	}
	return pp.inputs[0].EstimateCardinality()
}

func (pp *PhysicalProject) String() string {
	mode := ""
	if pp.ExecutionMode != ExecutionModeScalar {
		mode = fmt.Sprintf(" [%s]", pp.ExecutionMode.String())
	}
	return fmt.Sprintf("Project%s(%d exprs)", mode, len(pp.Projections))
}

// PhysicalAggregate represents an aggregation operation
type PhysicalAggregate struct {
	*BasePhysicalPlan
	GroupBy       []Expression
	Aggregates    []AggregateExpr
	ExecutionMode ExecutionMode
	Algorithm     AggregateAlgorithm
}

type AggregateAlgorithm int

const (
	AggregateAlgorithmHash AggregateAlgorithm = iota
	AggregateAlgorithmSort
	AggregateAlgorithmStream
)

func (aa AggregateAlgorithm) String() string {
	switch aa {
	case AggregateAlgorithmHash:
		return "hash"
	case AggregateAlgorithmSort:
		return "sort"
	case AggregateAlgorithmStream:
		return "stream"
	default:
		return "unknown"
	}
}

func NewPhysicalAggregate(input PhysicalPlan, groupBy []Expression, aggregates []AggregateExpr) *PhysicalAggregate {
	// Create schema from group by columns and aggregates
	numCols := len(groupBy) + len(aggregates)
	schema := &Schema{
		Columns: make([]Column, numCols),
	}

	// Add group by columns
	for i, expr := range groupBy {
		if col, ok := expr.(*ColumnRef); ok {
			schema.Columns[i] = Column{
				Name:     col.ColumnName,
				DataType: col.ColumnType,
			}
		} else {
			schema.Columns[i] = Column{
				Name:     fmt.Sprintf("group_%d", i),
				DataType: types.Text,
			}
		}
	}

	// Add aggregate columns
	for i, agg := range aggregates {
		schema.Columns[len(groupBy)+i] = Column{
			Name:     agg.Function.String(),
			DataType: types.Integer, // Default, should be inferred
		}
	}

	return &PhysicalAggregate{
		BasePhysicalPlan: &BasePhysicalPlan{
			schema: schema,
			inputs: []PhysicalPlan{input},
		},
		GroupBy:       groupBy,
		Aggregates:    aggregates,
		ExecutionMode: ExecutionModeScalar,
		Algorithm:     AggregateAlgorithmHash,
	}
}

func (pa *PhysicalAggregate) GetOperatorType() OperatorType   { return OperatorTypeAggregate }
func (pa *PhysicalAggregate) GetExecutionMode() ExecutionMode { return pa.ExecutionMode }
func (pa *PhysicalAggregate) RequiresVectorization() bool {
	return pa.ExecutionMode == ExecutionModeVectorized || pa.ExecutionMode == ExecutionModeHybrid
}

func (pa *PhysicalAggregate) EstimateCost(context *PhysicalPlanContext) *Cost {
	if len(pa.inputs) == 0 {
		return &Cost{TotalCost: 0}
	}

	inputCost := pa.inputs[0].EstimateCost(context)
	inputCardinality := pa.inputs[0].EstimateCardinality()

	// Aggregate cost depends on algorithm and input size
	var aggregateCost float64
	switch pa.Algorithm {
	case AggregateAlgorithmHash:
		// Hash aggregation: O(n) + hash table overhead
		aggregateCost = float64(inputCardinality) * 2.0
	case AggregateAlgorithmSort:
		// Sort-based aggregation: O(n log n)
		aggregateCost = float64(inputCardinality) * 3.0
	case AggregateAlgorithmStream:
		// Stream aggregation: O(n) but requires sorted input
		aggregateCost = float64(inputCardinality) * 1.5
	}

	if pa.RequiresVectorization() && context.CostModel != nil {
		aggregateCost *= 0.7 // Vectorized aggregation is more efficient
	}

	return &Cost{
		IOCost:    inputCost.IOCost,
		CPUCost:   inputCost.CPUCost + aggregateCost,
		TotalCost: inputCost.TotalCost + aggregateCost,
	}
}

func (pa *PhysicalAggregate) EstimateCardinality() int64 {
	if len(pa.inputs) == 0 {
		return 0
	}

	inputCardinality := pa.inputs[0].EstimateCardinality()

	// Estimate output cardinality based on group by columns
	if len(pa.GroupBy) == 0 {
		return 1 // Single group (no GROUP BY)
	}

	// Rough estimate: sqrt of input cardinality for grouping
	// This is a simplification - should use column statistics
	return int64(float64(inputCardinality) * 0.1)
}

func (pa *PhysicalAggregate) String() string {
	mode := ""
	if pa.ExecutionMode != ExecutionModeScalar {
		mode = fmt.Sprintf(" [%s]", pa.ExecutionMode.String())
	}
	return fmt.Sprintf("Aggregate%s(%s, %d groups, %d aggs)",
		mode, pa.Algorithm.String(), len(pa.GroupBy), len(pa.Aggregates))
}

// PhysicalSort represents a sort operation
type PhysicalSort struct {
	*BasePhysicalPlan
	SortKeys      []SortKey
	ExecutionMode ExecutionMode
	Algorithm     SortAlgorithm
}

type SortAlgorithm int

const (
	SortAlgorithmQuickSort SortAlgorithm = iota
	SortAlgorithmMergeSort
	SortAlgorithmTopK
)

func (sa SortAlgorithm) String() string {
	switch sa {
	case SortAlgorithmQuickSort:
		return "quicksort"
	case SortAlgorithmMergeSort:
		return "mergesort"
	case SortAlgorithmTopK:
		return "topk"
	default:
		return "unknown"
	}
}

func NewPhysicalSort(input PhysicalPlan, sortKeys []SortKey) *PhysicalSort {
	return &PhysicalSort{
		BasePhysicalPlan: &BasePhysicalPlan{
			schema: input.Schema(),
			inputs: []PhysicalPlan{input},
		},
		SortKeys:      sortKeys,
		ExecutionMode: ExecutionModeScalar,
		Algorithm:     SortAlgorithmQuickSort,
	}
}

func (ps *PhysicalSort) GetOperatorType() OperatorType   { return OperatorTypeSort }
func (ps *PhysicalSort) GetExecutionMode() ExecutionMode { return ps.ExecutionMode }
func (ps *PhysicalSort) RequiresVectorization() bool {
	return ps.ExecutionMode == ExecutionModeVectorized || ps.ExecutionMode == ExecutionModeHybrid
}

func (ps *PhysicalSort) EstimateCost(context *PhysicalPlanContext) *Cost {
	if len(ps.inputs) == 0 {
		return &Cost{TotalCost: 0}
	}

	inputCost := ps.inputs[0].EstimateCost(context)
	inputCardinality := ps.inputs[0].EstimateCardinality()

	// Sort cost: O(n log n)
	sortCost := float64(inputCardinality) * math.Log2(float64(inputCardinality)) * 0.5

	if ps.RequiresVectorization() && context.CostModel != nil {
		sortCost *= 0.8 // Vectorized sorting can be more efficient
	}

	return &Cost{
		IOCost:    inputCost.IOCost,
		CPUCost:   inputCost.CPUCost + sortCost,
		TotalCost: inputCost.TotalCost + sortCost,
	}
}

func (ps *PhysicalSort) EstimateCardinality() int64 {
	if len(ps.inputs) == 0 {
		return 0
	}
	return ps.inputs[0].EstimateCardinality()
}

func (ps *PhysicalSort) String() string {
	mode := ""
	if ps.ExecutionMode != ExecutionModeScalar {
		mode = fmt.Sprintf(" [%s]", ps.ExecutionMode.String())
	}
	return fmt.Sprintf("Sort%s(%s, %d keys)", mode, ps.Algorithm.String(), len(ps.SortKeys))
}

// PhysicalLimit represents a limit/offset operation
type PhysicalLimit struct {
	*BasePhysicalPlan
	Limit         int64
	Offset        int64
	ExecutionMode ExecutionMode
}

func NewPhysicalLimit(input PhysicalPlan, limit, offset int64) *PhysicalLimit {
	return &PhysicalLimit{
		BasePhysicalPlan: &BasePhysicalPlan{
			schema: input.Schema(),
			inputs: []PhysicalPlan{input},
		},
		Limit:         limit,
		Offset:        offset,
		ExecutionMode: ExecutionModeScalar,
	}
}

func (pl *PhysicalLimit) GetOperatorType() OperatorType   { return OperatorTypeLimit }
func (pl *PhysicalLimit) GetExecutionMode() ExecutionMode { return pl.ExecutionMode }
func (pl *PhysicalLimit) RequiresVectorization() bool {
	return pl.ExecutionMode == ExecutionModeVectorized || pl.ExecutionMode == ExecutionModeHybrid
}

func (pl *PhysicalLimit) EstimateCost(context *PhysicalPlanContext) *Cost {
	if len(pl.inputs) == 0 {
		return &Cost{TotalCost: 0}
	}

	inputCost := pl.inputs[0].EstimateCost(context)

	// Limit operation is very cheap - just track how many rows to output
	limitCost := float64(pl.Limit+pl.Offset) * 0.01

	return &Cost{
		IOCost:    inputCost.IOCost,
		CPUCost:   inputCost.CPUCost + limitCost,
		TotalCost: inputCost.TotalCost + limitCost,
	}
}

func (pl *PhysicalLimit) EstimateCardinality() int64 {
	if len(pl.inputs) == 0 {
		return 0
	}

	inputCardinality := pl.inputs[0].EstimateCardinality()

	// Output cardinality is limited by the LIMIT clause
	available := inputCardinality - pl.Offset
	if available <= 0 {
		return 0
	}

	if pl.Limit >= 0 && available > pl.Limit {
		return pl.Limit
	}

	return available
}

func (pl *PhysicalLimit) String() string {
	mode := ""
	if pl.ExecutionMode != ExecutionModeScalar {
		mode = fmt.Sprintf(" [%s]", pl.ExecutionMode.String())
	}

	if pl.Offset > 0 {
		return fmt.Sprintf("Limit%s(%d, %d)", mode, pl.Limit, pl.Offset)
	}
	return fmt.Sprintf("Limit%s(%d)", mode, pl.Limit)
}
