package planner

import (
	"fmt"
)

// AggregateRewriter rewrites expressions containing aggregates.
// It extracts aggregate expressions and replaces them with column references.
type AggregateRewriter struct {
	aggregates   []AggregateExpr
	aggregateMap map[string]int // Maps aggregate string to index
}

// NewAggregateRewriter creates a new aggregate rewriter.
func NewAggregateRewriter() *AggregateRewriter {
	return &AggregateRewriter{
		aggregateMap: make(map[string]int),
	}
}

// RewriteExpression rewrites an expression, extracting aggregates and replacing them with column refs.
func (r *AggregateRewriter) RewriteExpression(expr Expression) (Expression, error) {
	return r.rewriteExpr(expr)
}

// GetAggregates returns the extracted aggregate expressions.
func (r *AggregateRewriter) GetAggregates() []AggregateExpr {
	return r.aggregates
}

// rewriteExpr recursively rewrites an expression.
func (r *AggregateRewriter) rewriteExpr(expr Expression) (Expression, error) {
	switch e := expr.(type) {
	case *AggregateExpr:
		// Found an aggregate - add it to our list and return a column ref
		key := e.String()
		if idx, exists := r.aggregateMap[key]; exists {
			// Already have this aggregate
			return &ColumnRef{
				ColumnName: r.getAggregateColumnName(idx),
				ColumnType: r.aggregates[idx].Type,
			}, nil
		}

		// New aggregate - set the alias to match what we'll reference
		idx := len(r.aggregates)
		aggCopy := *e
		aggCopy.Alias = r.generateAggregateColumnName(idx, e.Function)
		r.aggregates = append(r.aggregates, aggCopy)
		r.aggregateMap[key] = idx

		return &ColumnRef{
			ColumnName: aggCopy.Alias,
			ColumnType: e.Type,
		}, nil

	case *BinaryOp:
		// Recursively rewrite left and right
		left, err := r.rewriteExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := r.rewriteExpr(e.Right)
		if err != nil {
			return nil, err
		}

		return &BinaryOp{
			Left:     left,
			Right:    right,
			Operator: e.Operator,
			Type:     e.Type,
		}, nil

	case *UnaryOp:
		// Recursively rewrite operand
		operand, err := r.rewriteExpr(e.Expr)
		if err != nil {
			return nil, err
		}

		return &UnaryOp{
			Expr:     operand,
			Operator: e.Operator,
			Type:     e.Type,
		}, nil

	case *CaseExpr:
		// Rewrite CASE expression
		var caseExpr Expression
		if e.Expr != nil {
			var err error
			caseExpr, err = r.rewriteExpr(e.Expr)
			if err != nil {
				return nil, err
			}
		}

		// Rewrite WHEN clauses
		whenList := make([]WhenClause, len(e.WhenList))
		for i, when := range e.WhenList {
			condition, err := r.rewriteExpr(when.Condition)
			if err != nil {
				return nil, err
			}
			result, err := r.rewriteExpr(when.Result)
			if err != nil {
				return nil, err
			}
			whenList[i] = WhenClause{
				Condition: condition,
				Result:    result,
			}
		}

		// Rewrite ELSE
		var elseExpr Expression
		if e.Else != nil {
			var err error
			elseExpr, err = r.rewriteExpr(e.Else)
			if err != nil {
				return nil, err
			}
		}

		return &CaseExpr{
			Expr:     caseExpr,
			WhenList: whenList,
			Else:     elseExpr,
			Type:     e.Type,
		}, nil

	case *FunctionCall:
		// Rewrite function arguments
		args := make([]Expression, len(e.Args))
		for i, arg := range e.Args {
			rewritten, err := r.rewriteExpr(arg)
			if err != nil {
				return nil, err
			}
			args[i] = rewritten
		}

		return &FunctionCall{
			Name: e.Name,
			Args: args,
			Type: e.Type,
		}, nil

	default:
		// For other expressions (literals, column refs, etc.), return as-is
		return expr, nil
	}
}

// getAggregateColumnName returns the column name for an aggregate at the given index.
func (r *AggregateRewriter) getAggregateColumnName(idx int) string {
	return fmt.Sprintf("agg_%d_%s", idx, r.aggregates[idx].Function.String())
}

// generateAggregateColumnName generates a column name for a new aggregate.
func (r *AggregateRewriter) generateAggregateColumnName(idx int, function AggregateFunc) string {
	return fmt.Sprintf("agg_%d_%s", idx, function.String())
}
