package planner

import (
	"github.com/dshills/QuantaDB/internal/catalog"
)

// IndexConditionPushdown optimizes index scans by pushing additional filters
// that can be evaluated during the index scan, reducing unnecessary row fetches.
type IndexConditionPushdown struct {
	catalog catalog.Catalog
}

// NewIndexConditionPushdown creates a new index condition pushdown optimizer.
func NewIndexConditionPushdown(catalog catalog.Catalog) *IndexConditionPushdown {
	return &IndexConditionPushdown{
		catalog: catalog,
	}
}

// Apply pushes filter conditions into index scan operators when beneficial.
func (icp *IndexConditionPushdown) Apply(plan LogicalPlan) (LogicalPlan, bool) {
	// Look for Filter over IndexScan patterns
	switch p := plan.(type) {
	case *LogicalFilter:
		// Check if child is an index scan
		if len(p.Children()) > 0 {
			switch child := p.Children()[0].(type) {
			case *IndexScan:
				return icp.pushToIndexScan(p, child)
			case *CompositeIndexScan:
				return icp.pushToCompositeIndexScan(p, child)
			case *BitmapHeapScan:
				// For bitmap scans, we keep the filter at heap level
				// since bitmap operations already filtered by index
				return plan, false
			}
		}

		// Recursively apply to children
		if len(p.Children()) > 0 {
			newChild, changed := icp.Apply(p.Children()[0].(LogicalPlan))
			if changed {
				return NewLogicalFilter(newChild, p.Predicate), true
			}
		}

	default:
		// Recursively apply to all children
		changed := false
		newChildren := make([]Plan, len(plan.Children()))
		for i, child := range plan.Children() {
			if logicalChild, ok := child.(LogicalPlan); ok {
				newChild, childChanged := icp.Apply(logicalChild)
				newChildren[i] = newChild
				changed = changed || childChanged
			} else {
				newChildren[i] = child
			}
		}

		if changed {
			return icp.rebuildPlanWithChildren(plan, newChildren), true
		}
	}

	return plan, false
}

// pushToIndexScan pushes predicates into a single-column index scan.
func (icp *IndexConditionPushdown) pushToIndexScan(filter *LogicalFilter, scan *IndexScan) (LogicalPlan, bool) {
	// Extract predicates that can be pushed
	indexCol := scan.Index.Columns[0].Column.Name
	pushable, remaining := icp.splitPredicates(filter.Predicate, indexCol)

	if pushable == nil {
		// Nothing to push
		return filter, false
	}

	// Create new index scan with pushed predicates
	newScan := &IndexScan{
		basePlan:         scan.basePlan,
		TableName:        scan.TableName,
		Index:            scan.Index,
		StartKey:         scan.StartKey,
		EndKey:           scan.EndKey,
		PushedPredicates: pushable,
	}

	// If all predicates were pushed, remove the filter
	if remaining == nil {
		return newScan, true
	}

	// Otherwise, keep filter with remaining predicates
	return NewLogicalFilter(newScan, remaining), true
}

// pushToCompositeIndexScan pushes predicates into a composite index scan.
func (icp *IndexConditionPushdown) pushToCompositeIndexScan(filter *LogicalFilter, scan *CompositeIndexScan) (LogicalPlan, bool) {
	// Get all index columns
	indexCols := make(map[string]bool)
	for _, col := range scan.Index.Columns {
		indexCols[col.Column.Name] = true
	}

	// Split predicates based on whether they reference only index columns
	var pushable, remaining []Expression
	conjuncts := icp.extractConjuncts(filter.Predicate)

	for _, pred := range conjuncts {
		if icp.canPushToIndex(pred, indexCols) {
			pushable = append(pushable, pred)
		} else {
			remaining = append(remaining, pred)
		}
	}

	if len(pushable) == 0 {
		// Nothing to push
		return filter, false
	}

	// Create new composite scan with pushed predicates
	newScan := &CompositeIndexScan{
		basePlan:         scan.basePlan,
		TableName:        scan.TableName,
		Index:            scan.Index,
		StartValues:      scan.StartValues,
		EndValues:        scan.EndValues,
		PushedPredicates: icp.combinePredicates(pushable),
	}

	// If all predicates were pushed, remove the filter
	if len(remaining) == 0 {
		return newScan, true
	}

	// Otherwise, keep filter with remaining predicates
	return NewLogicalFilter(newScan, icp.combinePredicates(remaining)), true
}

// splitPredicates splits predicates into those that can be pushed and those that can't.
func (icp *IndexConditionPushdown) splitPredicates(expr Expression, indexCol string) (pushable, remaining Expression) {
	// For simple cases, check if the entire expression references only the index column
	if icp.referencesOnlyColumn(expr, indexCol) {
		return expr, nil
	}

	// For AND expressions, split each conjunct
	if binOp, ok := expr.(*BinaryOp); ok && binOp.Operator == OpAnd {
		leftPush, leftRemain := icp.splitPredicates(binOp.Left, indexCol)
		rightPush, rightRemain := icp.splitPredicates(binOp.Right, indexCol)

		// Combine pushable predicates
		if leftPush != nil && rightPush != nil {
			pushable = &BinaryOp{Left: leftPush, Right: rightPush, Operator: OpAnd}
		} else if leftPush != nil {
			pushable = leftPush
		} else if rightPush != nil {
			pushable = rightPush
		}

		// Combine remaining predicates
		if leftRemain != nil && rightRemain != nil {
			remaining = &BinaryOp{Left: leftRemain, Right: rightRemain, Operator: OpAnd}
		} else if leftRemain != nil {
			remaining = leftRemain
		} else if rightRemain != nil {
			remaining = rightRemain
		}

		return pushable, remaining
	}

	// Can't push this predicate
	return nil, expr
}

// referencesOnlyColumn checks if an expression only references the given column.
func (icp *IndexConditionPushdown) referencesOnlyColumn(expr Expression, colName string) bool {
	switch e := expr.(type) {
	case *ColumnRef:
		return e.ColumnName == colName
	case *BinaryOp:
		return icp.referencesOnlyColumn(e.Left, colName) && icp.referencesOnlyColumn(e.Right, colName)
	case *UnaryOp:
		return icp.referencesOnlyColumn(e.Expr, colName)
	case *Literal:
		return true // Literals don't reference any column
	default:
		return false
	}
}

// canPushToIndex checks if a predicate can be pushed to an index scan.
func (icp *IndexConditionPushdown) canPushToIndex(expr Expression, indexCols map[string]bool) bool {
	switch e := expr.(type) {
	case *ColumnRef:
		return indexCols[e.ColumnName]
	case *BinaryOp:
		return icp.canPushToIndex(e.Left, indexCols) && icp.canPushToIndex(e.Right, indexCols)
	case *UnaryOp:
		return icp.canPushToIndex(e.Expr, indexCols)
	case *Literal:
		return true
	default:
		return false
	}
}

// extractConjuncts extracts individual predicates from AND expressions.
func (icp *IndexConditionPushdown) extractConjuncts(expr Expression) []Expression {
	var conjuncts []Expression

	switch e := expr.(type) {
	case *BinaryOp:
		if e.Operator == OpAnd {
			conjuncts = append(conjuncts, icp.extractConjuncts(e.Left)...)
			conjuncts = append(conjuncts, icp.extractConjuncts(e.Right)...)
		} else {
			conjuncts = append(conjuncts, expr)
		}
	default:
		conjuncts = append(conjuncts, expr)
	}

	return conjuncts
}

// combinePredicates combines multiple predicates with AND.
func (icp *IndexConditionPushdown) combinePredicates(predicates []Expression) Expression {
	if len(predicates) == 0 {
		return nil
	}
	if len(predicates) == 1 {
		return predicates[0]
	}

	result := predicates[0]
	for i := 1; i < len(predicates); i++ {
		result = &BinaryOp{
			Left:     result,
			Right:    predicates[i],
			Operator: OpAnd,
		}
	}

	return result
}

// rebuildPlanWithChildren creates a new plan node with updated children.
func (icp *IndexConditionPushdown) rebuildPlanWithChildren(plan LogicalPlan, children []Plan) LogicalPlan {
	// Convert children to LogicalPlan slice
	logicalChildren := make([]LogicalPlan, len(children))
	for i, child := range children {
		logicalChildren[i] = child.(LogicalPlan)
	}

	switch p := plan.(type) {
	case *LogicalFilter:
		return NewLogicalFilter(logicalChildren[0], p.Predicate)
	case *LogicalProject:
		return NewLogicalProject(logicalChildren[0], p.Projections, p.Aliases, p.schema)
	case *LogicalSort:
		return NewLogicalSort(logicalChildren[0], p.OrderBy)
	case *LogicalLimit:
		return NewLogicalLimit(logicalChildren[0], p.Limit, p.Offset)
	case *LogicalJoin:
		return NewLogicalJoin(logicalChildren[0], logicalChildren[1], p.JoinType, p.Condition, p.schema)
	case *LogicalAggregate:
		return NewLogicalAggregate(logicalChildren[0], p.GroupBy, p.Aggregates, p.schema)
	default:
		// Return original plan if we don't know how to rebuild it
		return plan
	}
}
