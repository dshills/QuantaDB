package executor

import (
	"fmt"
	"hash/fnv"
	"math"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// AggregateOperator implements GROUP BY and aggregation.
type AggregateOperator struct {
	baseOperator
	child       Operator
	groupBy     []ExprEvaluator
	aggregates  []AggregateExpr
	groups      map[uint64]*aggregateGroup
	groupIter   []*aggregateGroup
	iterIndex   int
	initialized bool // Track if Open() completed successfully
}

// AggregateExpr represents an aggregate expression.
type AggregateExpr struct {
	Function AggregateFunction
	Expr     ExprEvaluator
	Alias    string
}

// AggregateFunction represents an aggregate function.
type AggregateFunction interface {
	// Initialize initializes the aggregate state.
	Initialize() AggregateState
	// Accumulate adds a value to the aggregate state.
	Accumulate(state AggregateState, value types.Value) error
	// Finalize computes the final aggregate value.
	Finalize(state AggregateState) (types.Value, error)
	// ResultType returns the result type of the aggregate.
	ResultType() types.DataType
}

// AggregateState holds the state for an aggregate computation.
type AggregateState interface{}

// aggregateGroup represents a group of rows with aggregate states.
type aggregateGroup struct {
	key    []types.Value
	states []AggregateState
}

// NewAggregateOperator creates a new aggregate operator.
func NewAggregateOperator(child Operator, groupBy []ExprEvaluator, aggregates []AggregateExpr) *AggregateOperator {
	return NewAggregateOperatorWithNames(child, groupBy, aggregates, nil)
}

// NewAggregateOperatorWithNames creates a new aggregate operator with explicit GROUP BY column names.
func NewAggregateOperatorWithNames(child Operator, groupBy []ExprEvaluator, aggregates []AggregateExpr, groupByNames []string) *AggregateOperator {
	// Build output schema
	columns := make([]Column, 0, len(groupBy)+len(aggregates))

	// Add GROUP BY columns
	for i := range groupBy {
		var name string
		if groupByNames != nil && i < len(groupByNames) && groupByNames[i] != "" {
			name = groupByNames[i]
		} else {
			// For simplicity, name them group_0, group_1, etc.
			name = fmt.Sprintf("group_%d", i)
		}
		columns = append(columns, Column{
			Name:     name,
			Type:     types.Unknown, // Would need to infer from expression
			Nullable: true,
		})
	}

	// Add aggregate columns
	for _, agg := range aggregates {
		name := agg.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%s", getFunctionName(agg.Function))
		}
		columns = append(columns, Column{
			Name:     name,
			Type:     agg.Function.ResultType(),
			Nullable: true,
		})
	}

	schema := &Schema{Columns: columns}

	return &AggregateOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		child:       child,
		groupBy:     groupBy,
		aggregates:  aggregates,
		groups:      make(map[uint64]*aggregateGroup),
		groupIter:   make([]*aggregateGroup, 0), // Initialize to empty slice
		initialized: false,
	}
}

// Open initializes the aggregate operator.
func (a *AggregateOperator) Open(ctx *ExecContext) error {
	a.ctx = ctx
	a.groups = make(map[uint64]*aggregateGroup)
	a.groupIter = make([]*aggregateGroup, 0) // Always initialize to empty slice
	a.iterIndex = 0
	a.initialized = false

	// Validate child operator
	if a.child == nil {
		return fmt.Errorf("aggregate operator has no child")
	}

	// Open child
	if err := a.child.Open(ctx); err != nil {
		return fmt.Errorf("failed to open child: %w", err)
	}

	// Process all input rows
	if err := a.processInput(); err != nil {
		// Ensure we're in a valid state even if processing fails
		a.groupIter = make([]*aggregateGroup, 0)
		return fmt.Errorf("failed to process input: %w", err)
	}

	// Mark as successfully initialized
	a.initialized = true
	return nil
}

// processInput reads all rows from the child and groups them.
func (a *AggregateOperator) processInput() error {
	if a.groups == nil {
		return fmt.Errorf("aggregate operator groups map is nil")
	}

	// Track rows read for statistics
	rowsProcessed := 0

	for {
		row, err := a.child.Next()
		if err != nil {
			return fmt.Errorf("error reading row for aggregation: %w", err)
		}
		if row == nil {
			break // EOF
		}

		// Validate row
		if row.Values == nil {
			return fmt.Errorf("received row with nil values")
		}

		// Compute group key
		groupKey := make([]types.Value, len(a.groupBy))
		for i, expr := range a.groupBy {
			if expr == nil {
				return fmt.Errorf("group by expression %d is nil", i)
			}
			val, err := expr.Eval(row, a.ctx)
			if err != nil {
				return fmt.Errorf("error evaluating group by expression %d: %w", i, err)
			}
			groupKey[i] = val
		}

		// Hash the group key
		hash := a.hashGroupKey(groupKey)

		// Get or create group
		group, exists := a.groups[hash]
		if !exists {
			// Initialize new group
			group = &aggregateGroup{
				key:    groupKey,
				states: make([]AggregateState, len(a.aggregates)),
			}
			for i, agg := range a.aggregates {
				group.states[i] = agg.Function.Initialize()
			}
			a.groups[hash] = group
		}

		// Accumulate values
		for i, agg := range a.aggregates {
			val, err := agg.Expr.Eval(row, a.ctx)
			if err != nil {
				return fmt.Errorf("error evaluating aggregate expression %d: %w", i, err)
			}

			if err := agg.Function.Accumulate(group.states[i], val); err != nil {
				return fmt.Errorf("error accumulating aggregate %d: %w", i, err)
			}
		}

		rowsProcessed++
		if a.ctx != nil && a.ctx.Stats != nil {
			a.ctx.Stats.RowsRead++
		}
	}

	// Convert map to slice for iteration
	a.groupIter = make([]*aggregateGroup, 0, len(a.groups))
	for _, group := range a.groups {
		a.groupIter = append(a.groupIter, group)
	}

	// Special case: if no GROUP BY and no groups were created (no input rows),
	// we still need to return one row with aggregate results
	if len(a.groupBy) == 0 && len(a.groups) == 0 {
		// Create a single group with initialized aggregate states
		group := &aggregateGroup{
			key:    []types.Value{}, // Empty key for no GROUP BY
			states: make([]AggregateState, len(a.aggregates)),
		}
		for i, agg := range a.aggregates {
			group.states[i] = agg.Function.Initialize()
		}
		a.groupIter = append(a.groupIter, group)
	}

	return nil
}

// Next returns the next aggregated row.
func (a *AggregateOperator) Next() (*Row, error) {
	// Check if operator was properly initialized
	if !a.initialized {
		return nil, fmt.Errorf("aggregate operator not properly initialized")
	}

	// Safety check for groupIter
	if a.groupIter == nil {
		return nil, fmt.Errorf("group iterator is nil (internal error)")
	}

	if a.iterIndex >= len(a.groupIter) {
		return nil, nil // nolint:nilnil // EOF - this is the standard iterator pattern
	}

	group := a.groupIter[a.iterIndex]
	if group == nil {
		return nil, fmt.Errorf("group at index %d is nil", a.iterIndex)
	}

	a.iterIndex++

	// Build result row
	values := make([]types.Value, 0, len(a.groupBy)+len(a.aggregates))

	// Add group key values
	if group.key != nil {
		values = append(values, group.key...)
	}

	// Add aggregate results
	for i, agg := range a.aggregates {
		if agg.Function == nil {
			return nil, fmt.Errorf("aggregate function %d is nil", i)
		}
		if i >= len(group.states) {
			return nil, fmt.Errorf("aggregate state %d missing", i)
		}
		result, err := agg.Function.Finalize(group.states[i])
		if err != nil {
			return nil, fmt.Errorf("error finalizing aggregate %d: %w", i, err)
		}
		values = append(values, result)
	}

	if a.ctx != nil && a.ctx.Stats != nil {
		a.ctx.Stats.RowsReturned++
	}

	return &Row{Values: values}, nil
}

// hashGroupKey computes a hash for the group key.
func (a *AggregateOperator) hashGroupKey(key []types.Value) uint64 {
	hasher := fnv.New64()

	for _, val := range key {
		writeValueToHasher(hasher, val)
	}

	return hasher.Sum64()
}

// Close cleans up the aggregate operator.
func (a *AggregateOperator) Close() error {
	a.groups = nil
	a.groupIter = nil
	return a.child.Close()
}

// getFunctionName returns a string name for an aggregate function.
func getFunctionName(fn AggregateFunction) string {
	switch fn.(type) {
	case *CountFunction:
		return "count"
	case *SumFunction:
		return "sum"
	case *AvgFunction:
		return "avg"
	case *MinFunction:
		return "min"
	case *MaxFunction:
		return "max"
	case *StdDevFunction:
		return "stddev"
	default:
		return "unknown"
	}
}

// Aggregate Functions

// CountFunction implements COUNT aggregate.
type CountFunction struct {
	CountStar bool // COUNT(*) vs COUNT(expr)
}

type countState struct {
	count int64
}

func (f *CountFunction) Initialize() AggregateState {
	return &countState{count: 0}
}

func (f *CountFunction) Accumulate(state AggregateState, value types.Value) error {
	s := state.(*countState)
	if f.CountStar || !value.IsNull() {
		s.count++
	}
	return nil
}

func (f *CountFunction) Finalize(state AggregateState) (types.Value, error) {
	s := state.(*countState)
	return types.NewValue(s.count), nil
}

func (f *CountFunction) ResultType() types.DataType {
	return types.BigInt
}

// SumFunction implements SUM aggregate.
type SumFunction struct{}

type sumState struct {
	sum    float64
	isNull bool
}

func (f *SumFunction) Initialize() AggregateState {
	return &sumState{sum: 0, isNull: true}
}

func (f *SumFunction) Accumulate(state AggregateState, value types.Value) error {
	s := state.(*sumState)
	if !value.IsNull() {
		s.isNull = false
		switch v := value.Data.(type) {
		case int32:
			s.sum += float64(v)
		case int64:
			s.sum += float64(v)
		case float32:
			s.sum += float64(v)
		case float64:
			s.sum += v
		default:
			return fmt.Errorf("SUM requires numeric value, got %T", value.Data)
		}
	}
	return nil
}

func (f *SumFunction) Finalize(state AggregateState) (types.Value, error) {
	s := state.(*sumState)
	if s.isNull {
		return types.NewNullValue(), nil
	}
	return types.NewValue(s.sum), nil
}

func (f *SumFunction) ResultType() types.DataType {
	return types.Decimal(20, 6)
}

// AvgFunction implements AVG aggregate.
type AvgFunction struct{}

type avgState struct {
	sum   float64
	count int64
}

func (f *AvgFunction) Initialize() AggregateState {
	return &avgState{sum: 0, count: 0}
}

func (f *AvgFunction) Accumulate(state AggregateState, value types.Value) error {
	s := state.(*avgState)
	if !value.IsNull() {
		switch v := value.Data.(type) {
		case int32:
			s.sum += float64(v)
			s.count++
		case int64:
			s.sum += float64(v)
			s.count++
		case float32:
			s.sum += float64(v)
			s.count++
		case float64:
			s.sum += v
			s.count++
		default:
			return fmt.Errorf("AVG requires numeric value, got %T", value.Data)
		}
	}
	return nil
}

func (f *AvgFunction) Finalize(state AggregateState) (types.Value, error) {
	s := state.(*avgState)
	if s.count == 0 {
		return types.NewNullValue(), nil
	}
	return types.NewValue(s.sum / float64(s.count)), nil
}

func (f *AvgFunction) ResultType() types.DataType {
	return types.Decimal(20, 6)
}

// MinFunction implements MIN aggregate.
type MinFunction struct{}

type minState struct {
	min    types.Value
	hasVal bool
}

func (f *MinFunction) Initialize() AggregateState {
	return &minState{hasVal: false}
}

func (f *MinFunction) Accumulate(state AggregateState, value types.Value) error {
	s := state.(*minState)
	if !value.IsNull() {
		if !s.hasVal || compareValues(value, s.min) < 0 {
			s.min = value
			s.hasVal = true
		}
	}
	return nil
}

func (f *MinFunction) Finalize(state AggregateState) (types.Value, error) {
	s := state.(*minState)
	if !s.hasVal {
		return types.NewNullValue(), nil
	}
	return s.min, nil
}

func (f *MinFunction) ResultType() types.DataType {
	return types.Unknown // Depends on input type
}

// MaxFunction implements MAX aggregate.
type MaxFunction struct{}

type maxState struct {
	max    types.Value
	hasVal bool
}

func (f *MaxFunction) Initialize() AggregateState {
	return &maxState{hasVal: false}
}

func (f *MaxFunction) Accumulate(state AggregateState, value types.Value) error {
	s := state.(*maxState)
	if !value.IsNull() {
		if !s.hasVal || compareValues(value, s.max) > 0 {
			s.max = value
			s.hasVal = true
		}
	}
	return nil
}

func (f *MaxFunction) Finalize(state AggregateState) (types.Value, error) {
	s := state.(*maxState)
	if !s.hasVal {
		return types.NewNullValue(), nil
	}
	return s.max, nil
}

func (f *MaxFunction) ResultType() types.DataType {
	return types.Unknown // Depends on input type
}

// StdDevFunction implements STDDEV aggregate (population standard deviation).
type StdDevFunction struct{}

type stdDevState struct {
	sum   float64
	sumSq float64 // sum of squares
	count int64
}

func (f *StdDevFunction) Initialize() AggregateState {
	return &stdDevState{sum: 0, sumSq: 0, count: 0}
}

func (f *StdDevFunction) Accumulate(state AggregateState, value types.Value) error {
	s := state.(*stdDevState)
	if !value.IsNull() {
		var floatVal float64
		switch v := value.Data.(type) {
		case int32:
			floatVal = float64(v)
		case int64:
			floatVal = float64(v)
		case float32:
			floatVal = float64(v)
		case float64:
			floatVal = v
		default:
			return fmt.Errorf("STDDEV requires numeric value, got %T", value.Data)
		}
		s.sum += floatVal
		s.sumSq += floatVal * floatVal
		s.count++
	}
	return nil
}

func (f *StdDevFunction) Finalize(state AggregateState) (types.Value, error) {
	s := state.(*stdDevState)
	if s.count == 0 {
		return types.NewNullValue(), nil
	}
	// Population standard deviation formula: sqrt((sumSq/count) - (sum/count)^2)
	mean := s.sum / float64(s.count)
	variance := (s.sumSq / float64(s.count)) - (mean * mean)
	// Handle floating point errors that might make variance slightly negative
	if variance < 0 {
		variance = 0
	}
	stddev := math.Sqrt(variance)
	return types.NewValue(stddev), nil
}

func (f *StdDevFunction) ResultType() types.DataType {
	return types.Decimal(20, 6)
}

// CreateAggregateFunction creates an aggregate function by name.
func CreateAggregateFunction(name string, args []planner.Expression) (AggregateFunction, error) {
	switch name {
	case "COUNT":
		// Check if COUNT(*)
		if len(args) == 1 {
			if _, isStar := args[0].(*planner.Star); isStar {
				return &CountFunction{CountStar: true}, nil
			}
		}
		return &CountFunction{CountStar: false}, nil
	case "SUM":
		return &SumFunction{}, nil
	case "AVG":
		return &AvgFunction{}, nil
	case "MIN":
		return &MinFunction{}, nil
	case "MAX":
		return &MaxFunction{}, nil
	case "STDDEV":
		return &StdDevFunction{}, nil
	default:
		return nil, fmt.Errorf("unknown aggregate function: %s", name)
	}
}
