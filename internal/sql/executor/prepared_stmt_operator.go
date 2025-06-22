package executor

import (
	"fmt"
	"io"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// PrepareOperator handles PREPARE statements.
type PrepareOperator struct {
	baseOperator
	plan *planner.LogicalPrepare
	done bool
}

// NewPrepareOperator creates a new PREPARE operator.
func NewPrepareOperator(plan *planner.LogicalPrepare) *PrepareOperator {
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &PrepareOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		plan: plan,
	}
}

// Open initializes the operator.
func (op *PrepareOperator) Open(ctx *ExecContext) error {
	op.ctx = ctx
	op.done = false
	return nil
}

// Next executes the PREPARE statement.
func (op *PrepareOperator) Next() (*Row, error) {
	if op.done {
		return nil, io.EOF
	}

	op.done = true

	// Store the prepared statement in the context
	if op.ctx.PreparedStatements == nil {
		op.ctx.PreparedStatements = make(map[string]*PreparedStatement)
	}

	// Create the prepared statement
	ps := &PreparedStatement{
		Name:       op.plan.StatementName,
		ParamTypes: op.plan.ParamTypes,
		Query:      op.plan.Query,
	}

	// Store it
	op.ctx.PreparedStatements[op.plan.StatementName] = ps

	// Return success message
	message := fmt.Sprintf("PREPARE %s", op.plan.StatementName)
	return &Row{
		Values: []types.Value{
			types.NewTextValue(message),
		},
	}, nil
}

// Close cleans up resources.
func (op *PrepareOperator) Close() error {
	return nil
}

// ExecuteOperator handles EXECUTE statements.
type ExecuteOperator struct {
	baseOperator
	plan  *planner.LogicalExecute
	subOp Operator
}

// NewExecuteOperator creates a new EXECUTE operator.
func NewExecuteOperator(plan *planner.LogicalExecute) *ExecuteOperator {
	// The schema will be determined when we look up the prepared statement
	return &ExecuteOperator{
		baseOperator: baseOperator{},
		plan:         plan,
	}
}

// Open initializes the operator.
func (op *ExecuteOperator) Open(ctx *ExecContext) error {
	op.ctx = ctx

	// Look up the prepared statement
	if ctx.PreparedStatements == nil {
		return fmt.Errorf("prepared statement %s does not exist", op.plan.StatementName)
	}

	ps, exists := ctx.PreparedStatements[op.plan.StatementName]
	if !exists {
		return fmt.Errorf("prepared statement %s does not exist", op.plan.StatementName)
	}

	// Substitute parameters in the query
	// For now, we'll need to implement parameter substitution
	// This is a simplified version - in reality we'd need to properly substitute parameters

	// Plan the prepared statement's query
	_, err := ctx.Planner.Plan(ps.Query)
	if err != nil {
		return fmt.Errorf("failed to plan prepared statement: %w", err)
	}

	// For now, we'll return an error as we need to implement the full execution pipeline
	// In a complete implementation, we would:
	// 1. Substitute the parameters into the query
	// 2. Re-plan the query with the substituted values
	// 3. Execute the resulting plan
	return fmt.Errorf("EXECUTE not fully implemented yet")
}

// Next returns the next row from the executed statement.
func (op *ExecuteOperator) Next() (*Row, error) {
	if op.subOp == nil {
		return nil, fmt.Errorf("EXECUTE not properly initialized")
	}

	return op.subOp.Next()
}

// Close cleans up resources.
func (op *ExecuteOperator) Close() error {
	if op.subOp != nil {
		return op.subOp.Close()
	}
	return nil
}

// DeallocateOperator handles DEALLOCATE statements.
type DeallocateOperator struct {
	baseOperator
	plan *planner.LogicalDeallocate
	done bool
}

// NewDeallocateOperator creates a new DEALLOCATE operator.
func NewDeallocateOperator(plan *planner.LogicalDeallocate) *DeallocateOperator {
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &DeallocateOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		plan: plan,
	}
}

// Open initializes the operator.
func (op *DeallocateOperator) Open(ctx *ExecContext) error {
	op.ctx = ctx
	op.done = false
	return nil
}

// Next executes the DEALLOCATE statement.
func (op *DeallocateOperator) Next() (*Row, error) {
	if op.done {
		return nil, io.EOF
	}

	op.done = true

	// Remove the prepared statement from the context
	if op.ctx.PreparedStatements == nil {
		return nil, fmt.Errorf("prepared statement %s does not exist", op.plan.StatementName)
	}

	if _, exists := op.ctx.PreparedStatements[op.plan.StatementName]; !exists {
		return nil, fmt.Errorf("prepared statement %s does not exist", op.plan.StatementName)
	}

	delete(op.ctx.PreparedStatements, op.plan.StatementName)

	// Return success message
	message := fmt.Sprintf("DEALLOCATE %s", op.plan.StatementName)
	return &Row{
		Values: []types.Value{
			types.NewTextValue(message),
		},
	}, nil
}

// Close cleans up resources.
func (op *DeallocateOperator) Close() error {
	return nil
}
