package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Executor executes query plans.
type Executor interface {
	Execute(plan planner.Plan, ctx *ExecContext) (Result, error)
}

// Result represents the result of query execution.
type Result interface {
	// Next returns the next row or nil when done.
	Next() (*Row, error)
	// Close releases resources.
	Close() error
	// Schema returns the result schema.
	Schema() *Schema
}

// ExecContext provides context for query execution.
type ExecContext struct {
	// Catalog for metadata lookups
	Catalog catalog.Catalog
	// Storage engine for data access
	Engine engine.Engine
	// Transaction context (optional)
	Txn engine.Transaction
	// Parameters for prepared statements
	Params []types.Value
	// Statistics collector
	Stats *ExecStats
}

// Row represents a row of data.
type Row struct {
	Values []types.Value
}

// Schema represents the schema of a result set.
type Schema struct {
	Columns []Column
}

// Column represents a column in a schema.
type Column struct {
	Name     string
	Type     types.DataType
	Nullable bool
}

// ExecStats collects execution statistics.
type ExecStats struct {
	RowsRead     int64
	RowsReturned int64
	BytesRead    int64
}

// BasicExecutor is a basic query executor.
type BasicExecutor struct {
	catalog catalog.Catalog
	engine  engine.Engine
}

// NewBasicExecutor creates a new basic executor.
func NewBasicExecutor(catalog catalog.Catalog, engine engine.Engine) *BasicExecutor {
	return &BasicExecutor{
		catalog: catalog,
		engine:  engine,
	}
}

// Execute executes a query plan.
func (e *BasicExecutor) Execute(plan planner.Plan, ctx *ExecContext) (Result, error) {
	// Set default context values if not provided
	if ctx == nil {
		ctx = &ExecContext{
			Catalog: e.catalog,
			Engine:  e.engine,
			Stats:   &ExecStats{},
		}
	}
	
	// Convert logical plan to physical operators
	operator, err := e.buildOperator(plan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build operator: %w", err)
	}
	
	// Open the operator
	if err := operator.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open operator: %w", err)
	}
	
	// Return a result wrapper
	return &operatorResult{
		operator: operator,
		schema:   convertSchema(plan.Schema()),
	}, nil
}

// buildOperator converts a plan node to an operator.
func (e *BasicExecutor) buildOperator(plan planner.Plan, ctx *ExecContext) (Operator, error) {
	switch p := plan.(type) {
	case *planner.LogicalScan:
		return e.buildScanOperator(p, ctx)
	case *planner.LogicalFilter:
		return e.buildFilterOperator(p, ctx)
	case *planner.LogicalProject:
		return e.buildProjectOperator(p, ctx)
	case *planner.LogicalSort:
		return e.buildSortOperator(p, ctx)
	case *planner.LogicalLimit:
		return e.buildLimitOperator(p, ctx)
	default:
		return nil, fmt.Errorf("unsupported plan node: %T", plan)
	}
}

// buildScanOperator builds a scan operator.
func (e *BasicExecutor) buildScanOperator(plan *planner.LogicalScan, ctx *ExecContext) (Operator, error) {
	// Get table from catalog
	table, err := ctx.Catalog.GetTable("public", plan.TableName)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}
	
	return NewScanOperator(table, ctx), nil
}

// buildFilterOperator builds a filter operator.
func (e *BasicExecutor) buildFilterOperator(plan *planner.LogicalFilter, ctx *ExecContext) (Operator, error) {
	// Build child operator
	child, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}
	
	// Build predicate evaluator
	predicate, err := buildExprEvaluator(plan.Predicate)
	if err != nil {
		return nil, fmt.Errorf("failed to build predicate: %w", err)
	}
	
	return NewFilterOperator(child, predicate), nil
}

// buildProjectOperator builds a projection operator.
func (e *BasicExecutor) buildProjectOperator(plan *planner.LogicalProject, ctx *ExecContext) (Operator, error) {
	// Build child operator
	child, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}
	
	// Check if this is a star projection
	if len(plan.Projections) == 1 {
		if _, isStar := plan.Projections[0].(*planner.Star); isStar {
			// For star projections, return the child operator directly
			// This preserves the input schema
			return child, nil
		}
	}
	
	// Build projection evaluators
	projections := make([]ExprEvaluator, len(plan.Projections))
	childSchema := child.Schema()
	for i, expr := range plan.Projections {
		eval, err := buildExprEvaluatorWithSchema(expr, childSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to build projection %d: %w", i, err)
		}
		projections[i] = eval
	}
	
	// Build output schema
	schema := convertSchema(plan.Schema())
	if schema == nil {
		// If no schema from plan, use child schema
		schema = child.Schema()
	}
	
	return NewProjectOperator(child, projections, schema), nil
}

// buildSortOperator builds a sort operator.
func (e *BasicExecutor) buildSortOperator(plan *planner.LogicalSort, ctx *ExecContext) (Operator, error) {
	// Build child operator
	_, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}
	
	// TODO: Implement sort operator
	return nil, fmt.Errorf("sort operator not implemented")
}

// buildLimitOperator builds a limit operator.
func (e *BasicExecutor) buildLimitOperator(plan *planner.LogicalLimit, ctx *ExecContext) (Operator, error) {
	// Build child operator
	child, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}
	
	return NewLimitOperator(child, plan.Limit, plan.Offset), nil
}

// convertSchema converts a planner schema to executor schema.
func convertSchema(planSchema *planner.Schema) *Schema {
	if planSchema == nil {
		return nil
	}
	
	schema := &Schema{
		Columns: make([]Column, len(planSchema.Columns)),
	}
	
	for i, col := range planSchema.Columns {
		schema.Columns[i] = Column{
			Name:     col.Name,
			Type:     col.DataType,
			Nullable: col.Nullable,
		}
	}
	
	return schema
}

// operatorResult wraps an operator as a Result.
type operatorResult struct {
	operator Operator
	schema   *Schema
}

func (r *operatorResult) Next() (*Row, error) {
	return r.operator.Next()
}

func (r *operatorResult) Close() error {
	return r.operator.Close()
}

func (r *operatorResult) Schema() *Schema {
	return r.schema
}