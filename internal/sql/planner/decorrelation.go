package planner

import (
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// SubqueryDecorrelation transforms subqueries into semi/anti joins for better performance.
type SubqueryDecorrelation struct{}

// Apply transforms EXISTS/IN subqueries to semi/anti joins.
func (d *SubqueryDecorrelation) Apply(plan LogicalPlan) (LogicalPlan, bool) {
	return d.decorrelateSubqueries(plan)
}

// decorrelateSubqueries recursively transforms subqueries in a plan.
func (d *SubqueryDecorrelation) decorrelateSubqueries(plan LogicalPlan) (LogicalPlan, bool) {
	switch p := plan.(type) {
	case *LogicalFilter:
		// Check if the filter predicate contains subqueries that can be decorrelated
		newPredicate, newJoin, transformed := d.transformFilterPredicate(p.Predicate, p.Children()[0].(LogicalPlan))
		if transformed {
			if newPredicate != nil {
				// Add a new filter on top of the join
				return NewLogicalFilter(newJoin, newPredicate), true
			}
			// Just return the join (predicate was fully absorbed)
			return newJoin, true
		}

		// Recursively apply to children
		newChild, childChanged := d.decorrelateSubqueries(p.Children()[0].(LogicalPlan))
		if childChanged {
			return NewLogicalFilter(newChild, p.Predicate), true
		}

	case *LogicalProject:
		// Apply to children
		if len(p.Children()) > 0 {
			newChild, changed := d.decorrelateSubqueries(p.Children()[0].(LogicalPlan))
			if changed {
				return NewLogicalProject(newChild, p.Projections, p.Aliases, p.schema), true
			}
		}

	case *LogicalJoin:
		// Apply to both children
		leftChild, leftChanged := d.decorrelateSubqueries(p.Children()[0].(LogicalPlan))
		rightChild, rightChanged := d.decorrelateSubqueries(p.Children()[1].(LogicalPlan))

		if leftChanged || rightChanged {
			return NewLogicalJoin(leftChild, rightChild, p.JoinType, p.Condition, p.schema), true
		}

	default:
		// For other node types, recursively apply to children
		children := plan.Children()
		newChildren := make([]Plan, len(children))
		changed := false

		for i, child := range children {
			if childLogical, ok := child.(LogicalPlan); ok {
				newChild, childChanged := d.decorrelateSubqueries(childLogical)
				newChildren[i] = newChild
				if childChanged {
					changed = true
				}
			} else {
				newChildren[i] = child
			}
		}

		if changed {
			return d.rebuildWithChildren(plan, newChildren), true
		}
	}

	return plan, false
}

// transformFilterPredicate transforms subquery predicates into joins.
func (d *SubqueryDecorrelation) transformFilterPredicate(predicate Expression, child LogicalPlan) (Expression, LogicalPlan, bool) {
	switch pred := predicate.(type) {
	case *ExistsExpr:
		// Transform EXISTS to SEMI JOIN
		return d.transformExists(pred, child)

	case *InExpr:
		if pred.Subquery != nil {
			// Transform IN (subquery) to SEMI JOIN
			return d.transformInSubquery(pred, child)
		}

	case *BinaryOp:
		// Handle AND/OR combinations
		if pred.Operator == OpAnd {
			// Try to transform left side
			leftPred, leftJoin, leftTransformed := d.transformFilterPredicate(pred.Left, child)
			if leftTransformed {
				// Try to transform right side on the new join
				rightPred, finalJoin, rightTransformed := d.transformFilterPredicate(pred.Right, leftJoin)
				if rightTransformed {
					// Both sides transformed
					if leftPred != nil && rightPred != nil {
						return &BinaryOp{
							Left:     leftPred,
							Right:    rightPred,
							Operator: OpAnd,
							Type:     pred.Type,
						}, finalJoin, true
					}
					if leftPred != nil {
						return leftPred, finalJoin, true
					}
					if rightPred != nil {
						return rightPred, finalJoin, true
					}
					return nil, finalJoin, true
				}
				// Only left side transformed
				if leftPred != nil {
					return &BinaryOp{
						Left:     leftPred,
						Right:    pred.Right,
						Operator: OpAnd,
						Type:     pred.Type,
					}, leftJoin, true
				}
				return pred.Right, leftJoin, true
			}
			// Try to transform right side
			rightPred, rightJoin, rightTransformed := d.transformFilterPredicate(pred.Right, child)
			if rightTransformed {
				if rightPred != nil {
					return &BinaryOp{
						Left:     pred.Left,
						Right:    rightPred,
						Operator: OpAnd,
						Type:     pred.Type,
					}, rightJoin, true
				}
				return pred.Left, rightJoin, true
			}
		}
		// OR is more complex and not implemented yet
	}

	return predicate, child, false
}

// transformExists transforms EXISTS predicates to SEMI/ANTI joins.
func (d *SubqueryDecorrelation) transformExists(exists *ExistsExpr, leftPlan LogicalPlan) (Expression, LogicalPlan, bool) {
	// Get the subquery plan
	subqueryPlan := exists.Subquery.Subplan

	// Determine join type
	joinType := SemiJoin
	if exists.Not {
		joinType = AntiJoin
	}

	// Extract correlation predicates from the subquery
	correlationPreds, remainingSubquery := d.extractCorrelationPredicates(subqueryPlan, leftPlan.Schema())
	
	// Use correlation predicates as join condition, or TRUE if none found
	var joinCondition Expression
	if len(correlationPreds) == 0 {
		// No correlation found - use TRUE condition
		joinCondition = &Literal{
			Value: NewTrueValue(),
			Type:  types.Boolean,
		}
	} else if len(correlationPreds) == 1 {
		joinCondition = correlationPreds[0]
	} else {
		// Combine multiple correlation predicates with AND
		joinCondition = correlationPreds[0]
		for i := 1; i < len(correlationPreds); i++ {
			joinCondition = &BinaryOp{
				Left:     joinCondition,
				Right:    correlationPreds[i],
				Operator: OpAnd,
				Type:     types.Boolean,
			}
		}
	}

	// Create the semi/anti join
	join := NewLogicalJoin(leftPlan, remainingSubquery, joinType, joinCondition, leftPlan.Schema())

	// Return no remaining predicate (the EXISTS is fully absorbed into the join)
	return nil, join, true
}

// transformInSubquery transforms IN (subquery) predicates to SEMI/ANTI joins.
func (d *SubqueryDecorrelation) transformInSubquery(inExpr *InExpr, leftPlan LogicalPlan) (Expression, LogicalPlan, bool) {
	// Get the subquery plan
	subqueryPlan := inExpr.Subquery.Subplan
	if subqueryPlan == nil {
		return nil, leftPlan, false
	}

	// Determine join type
	joinType := SemiJoin
	if inExpr.Not {
		joinType = AntiJoin
	}

	// Create join condition: leftExpr = subquery.column
	// Get the first column from the subquery's schema
	var rightColumn string
	if subquerySchema := subqueryPlan.Schema(); subquerySchema != nil && len(subquerySchema.Columns) > 0 {
		rightColumn = subquerySchema.Columns[0].Name
	} else {
		// Fallback column name
		rightColumn = "subquery_col"
	}

	joinCondition := &BinaryOp{
		Left:     inExpr.Expr,
		Right:    &ColumnRef{ColumnName: rightColumn, TableAlias: ""},
		Operator: OpEqual,
		Type:     types.Boolean,
	}

	// Create the semi/anti join
	join := NewLogicalJoin(leftPlan, subqueryPlan, joinType, joinCondition, leftPlan.Schema())

	// Return no remaining predicate (the IN is fully absorbed into the join)
	return nil, join, true
}

// rebuildWithChildren creates a new plan node with updated children.
func (d *SubqueryDecorrelation) rebuildWithChildren(plan LogicalPlan, children []Plan) LogicalPlan {
	switch p := plan.(type) {
	case *LogicalFilter:
		if len(children) > 0 {
			return NewLogicalFilter(children[0].(LogicalPlan), p.Predicate)
		}
	case *LogicalProject:
		if len(children) > 0 {
			return NewLogicalProject(children[0].(LogicalPlan), p.Projections, p.Aliases, p.schema)
		}
	case *LogicalSort:
		if len(children) > 0 {
			return NewLogicalSort(children[0].(LogicalPlan), p.OrderBy)
		}
	case *LogicalLimit:
		if len(children) > 0 {
			return NewLogicalLimit(children[0].(LogicalPlan), p.Limit, p.Offset)
		}
	case *LogicalJoin:
		if len(children) >= 2 {
			return NewLogicalJoin(children[0].(LogicalPlan), children[1].(LogicalPlan),
				p.JoinType, p.Condition, p.schema)
		}
	case *LogicalAggregate:
		if len(children) > 0 {
			return NewLogicalAggregate(children[0].(LogicalPlan), p.GroupBy, p.Aggregates, p.schema)
		}
	}

	// Return original plan if we can't rebuild
	return plan
}

// extractCorrelationPredicates analyzes a subquery plan to extract predicates that reference outer columns.
// Returns the correlation predicates and the subquery plan with those predicates removed.
func (d *SubqueryDecorrelation) extractCorrelationPredicates(subqueryPlan LogicalPlan, outerSchema *Schema) ([]Expression, LogicalPlan) {
	// Look for a filter node in the subquery
	if filter, ok := subqueryPlan.(*LogicalFilter); ok {
		// Extract correlation predicates from the filter
		correlationPreds, remainingPred := d.splitCorrelationPredicates(filter.Predicate, outerSchema, filter.Children()[0].(LogicalPlan).Schema())
		
		if len(correlationPreds) > 0 {
			// We found correlation predicates
			childPlan := filter.Children()[0].(LogicalPlan)
			
			if remainingPred != nil {
				// Create new filter with remaining predicates
				return correlationPreds, NewLogicalFilter(childPlan, remainingPred)
			} else {
				// All predicates were correlation predicates, return child directly
				return correlationPreds, childPlan
			}
		}
	}
	
	// If the plan has children, recursively check them
	if len(subqueryPlan.Children()) > 0 {
		// For now, only handle the first child (simple case)
		if childLogical, ok := subqueryPlan.Children()[0].(LogicalPlan); ok {
			correlationPreds, newChild := d.extractCorrelationPredicates(childLogical, outerSchema)
			if len(correlationPreds) > 0 {
				// Rebuild the current node with the new child
				newChildren := []Plan{newChild}
				for i := 1; i < len(subqueryPlan.Children()); i++ {
					newChildren = append(newChildren, subqueryPlan.Children()[i])
				}
				return correlationPreds, d.rebuildWithChildren(subqueryPlan, newChildren)
			}
		}
	}
	
	// No correlation predicates found
	return nil, subqueryPlan
}

// splitCorrelationPredicates splits a predicate into correlation and non-correlation parts.
func (d *SubqueryDecorrelation) splitCorrelationPredicates(predicate Expression, outerSchema, innerSchema *Schema) ([]Expression, Expression) {
	switch pred := predicate.(type) {
	case *BinaryOp:
		if pred.Operator == OpAnd {
			// Split AND predicates
			leftCorr, leftRemaining := d.splitCorrelationPredicates(pred.Left, outerSchema, innerSchema)
			rightCorr, rightRemaining := d.splitCorrelationPredicates(pred.Right, outerSchema, innerSchema)
			
			// Combine correlation predicates
			correlationPreds := append(leftCorr, rightCorr...)
			
			// Combine remaining predicates
			var remaining Expression
			if leftRemaining != nil && rightRemaining != nil {
				remaining = &BinaryOp{
					Left:     leftRemaining,
					Right:    rightRemaining,
					Operator: OpAnd,
					Type:     types.Boolean,
				}
			} else if leftRemaining != nil {
				remaining = leftRemaining
			} else if rightRemaining != nil {
				remaining = rightRemaining
			}
			
			return correlationPreds, remaining
		} else if d.isCorrelationPredicate(pred, outerSchema, innerSchema) {
			// This is a correlation predicate
			return []Expression{pred}, nil
		}
	default:
		// Check if this expression contains correlation
		if d.isCorrelationPredicate(predicate, outerSchema, innerSchema) {
			return []Expression{predicate}, nil
		}
	}
	
	// Not a correlation predicate
	return nil, predicate
}

// isCorrelationPredicate checks if a predicate references both outer and inner columns.
func (d *SubqueryDecorrelation) isCorrelationPredicate(expr Expression, outerSchema, innerSchema *Schema) bool {
	hasOuter := false
	hasInner := false
	
	// Check if expression references columns from both schemas
	d.walkExpression(expr, func(e Expression) {
		if col, ok := e.(*ColumnRef); ok {
			if d.columnInSchema(col, outerSchema) {
				hasOuter = true
			}
			if d.columnInSchema(col, innerSchema) {
				hasInner = true
			}
		}
	})
	
	return hasOuter && hasInner
}

// columnInSchema checks if a column reference belongs to a schema.
func (d *SubqueryDecorrelation) columnInSchema(col *ColumnRef, schema *Schema) bool {
	for _, schemaCol := range schema.Columns {
		// Check by column name and optional table alias
		if schemaCol.Name == col.ColumnName {
			if col.TableAlias == "" || col.TableAlias == schemaCol.TableAlias {
				return true
			}
		}
	}
	return false
}

// walkExpression recursively visits all expressions in a tree.
func (d *SubqueryDecorrelation) walkExpression(expr Expression, visitor func(Expression)) {
	if expr == nil {
		return
	}
	
	visitor(expr)
	
	switch e := expr.(type) {
	case *BinaryOp:
		d.walkExpression(e.Left, visitor)
		d.walkExpression(e.Right, visitor)
	case *UnaryOp:
		d.walkExpression(e.Expr, visitor)
	case *FunctionCall:
		for _, arg := range e.Args {
			d.walkExpression(arg, visitor)
		}
	case *CaseExpr:
		for _, when := range e.WhenList {
			d.walkExpression(when.Condition, visitor)
			d.walkExpression(when.Result, visitor)
		}
		d.walkExpression(e.Else, visitor)
	case *InExpr:
		d.walkExpression(e.Expr, visitor)
		for _, val := range e.Values {
			d.walkExpression(val, visitor)
		}
	case *ExistsExpr:
		// Don't walk into subqueries
	case *SubqueryExpr:
		// Don't walk into subqueries
	}
}

// Helper function to create a TRUE literal value
func NewTrueValue() types.Value {
	return types.Value{
		Data: true,
		Null: false,
	}
}
