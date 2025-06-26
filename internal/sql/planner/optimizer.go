package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// OptimizationRule represents a rule that transforms logical plans.
type OptimizationRule interface {
	// Apply attempts to apply this rule to the given plan.
	// Returns the transformed plan and true if the rule was applied.
	Apply(plan LogicalPlan) (LogicalPlan, bool)
}

// Optimizer applies optimization rules to logical plans.
type Optimizer struct {
	rules   []OptimizationRule
	catalog catalog.Catalog
}

// NewOptimizer creates a new optimizer with default rules.
func NewOptimizer() *Optimizer {
	return &Optimizer{
		rules: []OptimizationRule{
			&SubqueryDecorrelation{}, // Apply early to convert subqueries to joins
			&PredicatePushdown{},
			&ProjectionPushdown{},
			&ConstantFolding{},
		},
	}
}

// NewOptimizerWithCatalog creates a new optimizer with catalog for index selection.
func NewOptimizerWithCatalog(cat catalog.Catalog) *Optimizer {
	indexSelection := &IndexSelection{
		catalog:             cat,
		costEstimator:       NewCostEstimator(cat),
		vectorizedBatchSize: 1024, // Default batch size
	}
	indexConditionPushdown := NewIndexConditionPushdown(cat)
	return &Optimizer{
		catalog: cat,
		rules: []OptimizationRule{
			&SubqueryDecorrelation{}, // Apply early to convert subqueries to joins
			&PredicatePushdown{},
			&ProjectionPushdown{},
			&ConstantFolding{},
			indexSelection,
			indexConditionPushdown,
		},
	}
}

// Optimize applies all optimization rules to a plan until no more changes occur.
func (o *Optimizer) Optimize(plan LogicalPlan) LogicalPlan {
	maxIterations := 20 // Allow more iterations for complex queries

	var prevPlanStr string
	var seenPlans = make(map[string]bool) // Track seen plan structures to detect cycles

	for i := 0; i < maxIterations; i++ {
		currentPlanStr := o.planFingerprint(plan)

		// Check if we've seen this exact plan structure before (cycle detection)
		if seenPlans[currentPlanStr] {
			break
		}
		seenPlans[currentPlanStr] = true

		changed := false

		for _, rule := range o.rules {
			newPlan, applied := rule.Apply(plan)
			if applied {
				// Use fingerprint comparison instead of String()
				if o.planFingerprint(newPlan) != o.planFingerprint(plan) {
					plan = newPlan
					changed = true
				}
			}
		}

		// Additional check: if plan fingerprint is the same as previous iteration, stop
		if currentPlanStr == prevPlanStr {
			break
		}

		if !changed {
			break
		}

		prevPlanStr = currentPlanStr
	}

	return plan
}

// planFingerprint generates a unique string representation of the entire plan tree.
// Unlike String(), this includes all child nodes to properly detect structural changes.
func (o *Optimizer) planFingerprint(plan Plan) string {
	if plan == nil {
		return "nil"
	}

	// Start with the node's own string representation
	result := fmt.Sprintf("%T:%s", plan, plan.String())

	// Add child fingerprints
	children := plan.Children()
	if len(children) > 0 {
		result += "["
		for i, child := range children {
			if i > 0 {
				result += ","
			}
			result += o.planFingerprint(child)
		}
		result += "]"
	}

	return result
}

// PredicatePushdown pushes filter predicates closer to table scans.
type PredicatePushdown struct{}

// Apply pushes predicates down through the plan tree.
func (r *PredicatePushdown) Apply(plan LogicalPlan) (LogicalPlan, bool) {
	switch p := plan.(type) {
	case *LogicalFilter:
		// Try to push the filter down through its child
		child := p.Children()[0]

		switch c := child.(type) {
		case *LogicalProject:
			// Push filter below projection if possible
			if r.canPushThroughProject(p.Predicate, c) {
				// Create new filter below projection
				newFilter := NewLogicalFilter(c.Children()[0].(LogicalPlan), p.Predicate)
				// Create new projection on top of filter
				newProject := NewLogicalProject(newFilter, c.Projections, c.Aliases, c.schema)
				return newProject, true
			}

		case *LogicalJoin:
			// Split predicate and push parts to appropriate sides
			// TODO: Implement join predicate pushdown
			return plan, false
		}

	case *LogicalProject:
		// Recursively apply to children
		if len(p.Children()) > 0 {
			newChild, changed := r.Apply(p.Children()[0].(LogicalPlan))
			if changed {
				return NewLogicalProject(newChild, p.Projections, p.Aliases, p.schema), true
			}
		}

	case *LogicalJoin:
		// Recursively apply to children
		left, leftChanged := r.Apply(p.Children()[0].(LogicalPlan))
		right, rightChanged := r.Apply(p.Children()[1].(LogicalPlan))

		if leftChanged || rightChanged {
			return NewLogicalJoin(left, right, p.JoinType, p.Condition, p.schema), true
		}
	}

	return plan, false
}

// canPushThroughProject checks if a predicate can be pushed through a projection.
func (r *PredicatePushdown) canPushThroughProject(predicate Expression, project *LogicalProject) bool {
	// For now, always allow pushing through projections
	// TODO: Check if predicate references only columns available before projection
	return true
}

// ProjectionPushdown pushes projections down to reduce data flow.
type ProjectionPushdown struct{}

// Apply pushes projections down through the plan tree.
func (p *ProjectionPushdown) Apply(plan LogicalPlan) (LogicalPlan, bool) {
	// For projection pushdown, we want to push projections through operators
	// to reduce data flow as early as possible

	switch node := plan.(type) {
	case *LogicalProject:
		// Try to push this projection down through its child
		if len(node.Children()) == 0 {
			return plan, false
		}

		child := node.Children()[0].(LogicalPlan)
		requiredCols := node.RequiredColumns()

		// If projection uses star (*), don't push down
		if requiredCols.HasStar() {
			return plan, false
		}

		// Try to push through different operators
		switch c := child.(type) {
		case *LogicalFilter:
			// Push projection through filter
			// Need to include columns used by filter predicate
			filterCols := c.RequiredColumns()
			combinedCols := requiredCols.Clone()
			combinedCols.AddAll(filterCols)

			// Create projection below filter
			if len(c.Children()) > 0 {
				filterChild := c.Children()[0].(LogicalPlan)

				// Don't add projection if child is already a projection with same or fewer columns
				if existingProj, ok := filterChild.(*LogicalProject); ok {
					existingCols := existingProj.RequiredColumns()
					if !combinedCols.HasStar() && existingCols.Size() <= combinedCols.Size() {
						// Child projection already limits columns sufficiently
						return plan, false
					}
				}

				// Only add projection if it reduces columns significantly
				if !p.shouldInsertProjection(filterChild, combinedCols) {
					return plan, false
				}

				newProj := p.createProjection(filterChild, combinedCols)
				newFilter := NewLogicalFilter(newProj, c.Predicate)
				return NewLogicalProject(newFilter, node.Projections, node.Aliases, node.schema), true
			}

		case *LogicalSort:
			// Push projection through sort
			// Need to include columns used by sort
			sortCols := c.RequiredColumns()
			combinedCols := requiredCols.Clone()
			combinedCols.AddAll(sortCols)

			// Create projection below sort
			if len(c.Children()) > 0 {
				sortChild := c.Children()[0].(LogicalPlan)
				// Don't add projection if child is already a projection with same or fewer columns
				if existingProj, ok := sortChild.(*LogicalProject); ok {
					existingCols := existingProj.RequiredColumns()
					if !combinedCols.HasStar() && existingCols.Size() <= combinedCols.Size() {
						return plan, false
					}
				}

				// Only add projection if it reduces columns significantly
				if !p.shouldInsertProjection(sortChild, combinedCols) {
					return plan, false
				}

				newProj := p.createProjection(sortChild, combinedCols)
				newSort := NewLogicalSort(newProj, c.OrderBy)
				return NewLogicalProject(newSort, node.Projections, node.Aliases, node.schema), true
			}

		case *LogicalJoin:
			// Push projections to both sides of join
			// Need columns for join condition plus output columns
			joinCols := c.RequiredColumns()

			// Verify that we have at least two children
			children := c.Children()
			if len(children) < 2 {
				return plan, false
			}

			// Determine which columns are needed from each side
			leftSchema := children[0].Schema()
			rightSchema := children[1].Schema()

			// Check for nil schemas (defensive programming)
			if leftSchema == nil || rightSchema == nil {
				return plan, false
			}

			leftRequired := NewColumnSet()
			rightRequired := NewColumnSet()

			// Add columns needed for join condition
			for col := range joinCols.columns {
				if columnExistsInSchema(col.ColumnName, leftSchema) {
					leftRequired.Add(col)
				}
				if columnExistsInSchema(col.ColumnName, rightSchema) {
					rightRequired.Add(col)
				}
			}

			// Add columns needed for final projection
			for col := range requiredCols.columns {
				if col.TableAlias == "" {
					// Unqualified column - check both schemas
					if columnExistsInSchema(col.ColumnName, leftSchema) {
						leftRequired.Add(col)
					}
					if columnExistsInSchema(col.ColumnName, rightSchema) {
						rightRequired.Add(col)
					}
				} else {
					// Qualified column - add to appropriate side
					// This is simplified - in practice we'd track table aliases better
					if columnExistsInSchema(col.ColumnName, leftSchema) {
						leftRequired.Add(col)
					}
					if columnExistsInSchema(col.ColumnName, rightSchema) {
						rightRequired.Add(col)
					}
				}
			}

			// Create projections for both sides if beneficial
			leftChild := children[0].(LogicalPlan)
			rightChild := children[1].(LogicalPlan)

			modified := false
			if !leftRequired.HasStar() && leftRequired.Size() < len(leftSchema.Columns) {
				leftChild = p.createProjection(leftChild, leftRequired)
				modified = true
			}
			if !rightRequired.HasStar() && rightRequired.Size() < len(rightSchema.Columns) {
				rightChild = p.createProjection(rightChild, rightRequired)
				modified = true
			}

			if modified {
				newJoin := NewLogicalJoin(leftChild, rightChild, c.JoinType, c.Condition, c.schema)
				return NewLogicalProject(newJoin, node.Projections, node.Aliases, node.schema), true
			}
		}

		// Recursively apply to children
		newChild, changed := p.Apply(child)
		if changed {
			return NewLogicalProject(newChild, node.Projections, node.Aliases, node.schema), true
		}

	case *LogicalAggregate:
		// For aggregates, push projection below to only read needed columns
		if len(node.Children()) > 0 {
			requiredCols := node.RequiredColumns()
			child := node.Children()[0].(LogicalPlan)

			if !requiredCols.HasStar() && p.shouldInsertProjection(child, requiredCols) {
				newChild := p.createProjection(child, requiredCols)
				return NewLogicalAggregate(newChild, node.GroupBy, node.Aggregates, node.schema), true
			}
		}

	default:
		// For other nodes, recursively apply to children
		children := plan.Children()
		newChildren := make([]Plan, len(children))
		changed := false

		for i, child := range children {
			if childLogical, ok := child.(LogicalPlan); ok {
				newChild, childChanged := p.Apply(childLogical)
				newChildren[i] = newChild
				if childChanged {
					changed = true
				}
			} else {
				newChildren[i] = child
			}
		}

		if changed {
			return p.rebuildWithChildren(plan, newChildren), true
		}
	}

	return plan, false
}

// shouldInsertProjection determines if projection would be beneficial
func (p *ProjectionPushdown) shouldInsertProjection(node LogicalPlan, required *ColumnSet) bool {
	// Don't project if we need all columns
	if required.HasStar() {
		return false
	}

	// Don't add projection immediately after another projection
	if _, ok := node.(*LogicalProject); ok {
		return false
	}

	// Don't project after operations that already limit columns
	switch node.(type) {
	case *LogicalAggregate:
		// Aggregate already limits columns to GROUP BY + aggregates
		return false
	case *LogicalValues:
		// Values generates specific columns
		return false
	}

	// Get schema to see how many columns are available
	schema := node.Schema()
	if schema == nil || len(schema.Columns) == 0 {
		return false
	}

	// Insert projection if we're eliminating significant columns
	requiredCount := required.Size()
	availableCount := len(schema.Columns)

	// Project if we're using less than 80% of available columns
	// OR eliminating at least 1 column
	if requiredCount < int(float64(availableCount)*0.8) ||
		availableCount-requiredCount >= 1 {
		return true
	}

	// Always project after joins to eliminate join columns
	if _, ok := node.(*LogicalJoin); ok && !required.HasStar() {
		return true
	}

	// Project after table scans if eliminating many columns
	if _, ok := node.(*LogicalScan); ok {
		if requiredCount < availableCount/2 {
			return true
		}
	}

	return false
}

// createProjection creates a new projection node with required columns
func (p *ProjectionPushdown) createProjection(child LogicalPlan, required *ColumnSet) LogicalPlan {
	schema := child.Schema()
	if schema == nil {
		return child
	}

	// Build projection expressions for required columns
	projExprs := make([]Expression, 0)
	projAliases := make([]string, 0)

	// Get required columns in deterministic order
	requiredCols := required.ToSlice()

	// Map to track which schema columns we've included
	includedCols := make(map[string]bool)

	// Add required columns
	for _, reqCol := range requiredCols {
		// Find matching column in schema
		for _, schemaCol := range schema.Columns {
			if schemaCol.Name == reqCol.ColumnName {
				if !includedCols[schemaCol.Name] {
					projExprs = append(projExprs, &ColumnRef{
						TableAlias: reqCol.TableAlias,
						ColumnName: reqCol.ColumnName,
					})
					projAliases = append(projAliases, "")
					includedCols[schemaCol.Name] = true
				}
				break
			}
		}
	}

	// Build output schema
	newSchema := &Schema{
		Columns: make([]Column, len(projExprs)),
	}
	for i, expr := range projExprs {
		if colRef, ok := expr.(*ColumnRef); ok {
			// Find type from original schema
			for _, col := range schema.Columns {
				if col.Name == colRef.ColumnName {
					newSchema.Columns[i] = Column{
						Name:     col.Name,
						DataType: col.DataType,
						Nullable: col.Nullable,
					}
					break
				}
			}
		}
	}

	return NewLogicalProject(child, projExprs, projAliases, newSchema)
}

// rebuildWithChildren creates a new node with updated children
func (p *ProjectionPushdown) rebuildWithChildren(node LogicalPlan, children []Plan) LogicalPlan {
	switch n := node.(type) {
	case *LogicalFilter:
		if len(children) > 0 {
			return NewLogicalFilter(children[0].(LogicalPlan), n.Predicate)
		}
	case *LogicalProject:
		if len(children) > 0 {
			return NewLogicalProject(children[0].(LogicalPlan), n.Projections, n.Aliases, n.schema)
		}
	case *LogicalSort:
		if len(children) > 0 {
			return NewLogicalSort(children[0].(LogicalPlan), n.OrderBy)
		}
	case *LogicalLimit:
		if len(children) > 0 {
			return NewLogicalLimit(children[0].(LogicalPlan), n.Limit, n.Offset)
		}
	case *LogicalJoin:
		if len(children) >= 2 {
			return NewLogicalJoin(children[0].(LogicalPlan), children[1].(LogicalPlan),
				n.JoinType, n.Condition, n.schema)
		}
	case *LogicalAggregate:
		if len(children) > 0 {
			return NewLogicalAggregate(children[0].(LogicalPlan), n.GroupBy, n.Aggregates, n.schema)
		}
	}

	// Return original node if we can't rebuild
	return node
}

// ConstantFolding evaluates constant expressions at planning time.
type ConstantFolding struct{}

// Apply folds constant expressions.
func (c *ConstantFolding) Apply(plan LogicalPlan) (LogicalPlan, bool) {
	changed := false

	// Use a visitor pattern to traverse and modify expressions
	visitor := &constantFoldingVisitor{changed: &changed}

	switch p := plan.(type) {
	case *LogicalFilter:
		newPredicate := c.foldExpression(p.Predicate, visitor)
		if *visitor.changed {
			return NewLogicalFilter(p.Children()[0].(LogicalPlan), newPredicate), true
		}

	case *LogicalProject:
		var newProjections []Expression
		for _, proj := range p.Projections {
			newProj := c.foldExpression(proj, visitor)
			newProjections = append(newProjections, newProj)
		}

		if *visitor.changed {
			return NewLogicalProject(p.Children()[0].(LogicalPlan), newProjections, p.Aliases, p.schema), true
		}
	}

	// Recursively apply to children
	if len(plan.Children()) > 0 {
		var newChildren []LogicalPlan
		childChanged := false

		for _, child := range plan.Children() {
			newChild, changed := c.Apply(child.(LogicalPlan))
			newChildren = append(newChildren, newChild)
			if changed {
				childChanged = true
			}
		}

		if childChanged {
			return c.rebuildPlanWithChildren(plan, newChildren), true
		}
	}

	return plan, false
}

// foldExpression attempts to evaluate constant expressions.
func (c *ConstantFolding) foldExpression(expr Expression, visitor *constantFoldingVisitor) Expression {
	// For now, just return the expression as-is
	// TODO: Implement actual constant folding
	return expr
}

// rebuildPlanWithChildren creates a new plan node with updated children.
func (c *ConstantFolding) rebuildPlanWithChildren(plan LogicalPlan, children []LogicalPlan) LogicalPlan {
	switch p := plan.(type) {
	case *LogicalFilter:
		return NewLogicalFilter(children[0], p.Predicate)
	case *LogicalProject:
		return NewLogicalProject(children[0], p.Projections, p.Aliases, p.schema)
	case *LogicalSort:
		return NewLogicalSort(children[0], p.OrderBy)
	case *LogicalLimit:
		return NewLogicalLimit(children[0], p.Limit, p.Offset)
	case *LogicalJoin:
		return NewLogicalJoin(children[0], children[1], p.JoinType, p.Condition, p.schema)
	case *LogicalAggregate:
		return NewLogicalAggregate(children[0], p.GroupBy, p.Aggregates, p.schema)
	default:
		// Return original plan if we don't know how to rebuild it
		return plan
	}
}

// constantFoldingVisitor helps with constant folding.
type constantFoldingVisitor struct {
	changed *bool
}

// PlanOptimizer integrates optimization into the planner.
type PlanOptimizer struct {
	optimizer *Optimizer
}

// NewPlanOptimizer creates a new plan optimizer.
func NewPlanOptimizer() *PlanOptimizer {
	return &PlanOptimizer{
		optimizer: NewOptimizer(),
	}
}

// OptimizePlan applies all optimization rules to a logical plan.
func (o *PlanOptimizer) OptimizePlan(plan LogicalPlan) LogicalPlan {
	return o.optimizer.Optimize(plan)
}

// explainPlan generates a string representation of a plan for debugging.
func explainPlan(plan Plan, indent string) string {
	result := indent + plan.String() + "\n"

	for _, child := range plan.Children() {
		result += explainPlan(child, indent+"  ")
	}

	return result
}

// ExplainPlan returns a string representation of the plan tree.
func ExplainPlan(plan Plan) string {
	return explainPlan(plan, "")
}

// IndexSelection selects appropriate indexes for table scans.
type IndexSelection struct {
	catalog             catalog.Catalog
	costEstimator       *CostEstimator
	vectorizedCostModel *VectorizedCostModel
	vectorizedBatchSize int
}

// SetCatalog sets the catalog for index selection.
func (is *IndexSelection) SetCatalog(cat catalog.Catalog) {
	is.catalog = cat
	is.costEstimator = NewCostEstimator(cat)
	if is.vectorizedBatchSize == 0 {
		is.vectorizedBatchSize = 1024 // Default batch size
	}
}

// SetVectorizedModel sets the vectorized cost model and batch size
func (is *IndexSelection) SetVectorizedModel(model *VectorizedCostModel, batchSize int) {
	is.vectorizedCostModel = model
	is.vectorizedBatchSize = batchSize
	if is.costEstimator != nil {
		is.costEstimator.SetVectorizedModel(model)
	}
}

// tryCoveringIndexScan attempts to find a covering index that can satisfy the query entirely
// without needing to access the base table. This is the most efficient type of index scan.
func (is *IndexSelection) tryCoveringIndexScan(scan *LogicalScan, filter *LogicalFilter) LogicalPlan {
	if is.catalog == nil {
		return nil
	}

	// Skip covering index optimization for OR predicates as they typically don't benefit from indexes
	if filter != nil && filter.Predicate != nil && is.containsOrPredicate(filter.Predicate) {
		return nil
	}

	// Get table to access its indexes
	table, err := is.catalog.GetTable("public", scan.TableName) // Assuming public schema
	if err != nil || table == nil {
		return nil
	}

	// Extract required columns from the scan and filter
	requiredColumns := is.extractRequiredColumns(scan, filter)
	if len(requiredColumns) == 0 {
		return nil
	}

	// Find the best covering index
	var bestIndex *catalog.Index
	var bestCost float64

	for _, idx := range table.Indexes {
		// Check if this index covers all required columns
		if IsCoveringIndex(idx, requiredColumns) {
			// Estimate cost for this covering index
			cost := is.estimateCoveringIndexCost(table, idx, filter, len(requiredColumns))
			
			if bestIndex == nil || cost < bestCost {
				bestIndex = idx
				bestCost = cost
			}
		}
	}

	if bestIndex == nil {
		return nil
	}

	// Check if we can extract range conditions for the index
	startValues, endValues := is.extractIndexRangeConditions(bestIndex, filter.Predicate)

	// Create IndexOnlyScan plan
	schema := scan.Schema()
	return NewIndexOnlyScan(
		scan.TableName,
		bestIndex.Name,
		bestIndex,
		schema,
		startValues,
		endValues,
		requiredColumns,
	)
}

// extractRequiredColumns determines which columns are needed by the query
func (is *IndexSelection) extractRequiredColumns(scan *LogicalScan, filter *LogicalFilter) []string {
	columnsSet := make(map[string]bool)
	
	// Add columns from the filter predicate
	if filter != nil && filter.Predicate != nil {
		is.extractColumnsFromExpression(filter.Predicate, columnsSet)
	}
	
	// For now, if we can't determine specific columns, assume we need all table columns
	// In a more sophisticated implementation, we would walk up the plan tree to find projections
	if len(columnsSet) == 0 {
		// Get all table columns
		if table, err := is.catalog.GetTable("public", scan.TableName); err == nil && table != nil {
			for _, col := range table.Columns {
				columnsSet[col.Name] = true
			}
		}
	}
	
	// Convert to slice
	var columns []string
	for col := range columnsSet {
		columns = append(columns, col)
	}
	
	return columns
}

// extractColumnsFromExpression recursively extracts column names from expressions
func (is *IndexSelection) extractColumnsFromExpression(expr Expression, columns map[string]bool) {
	switch e := expr.(type) {
	case *ColumnRef:
		columns[e.ColumnName] = true
	case *BinaryOp:
		is.extractColumnsFromExpression(e.Left, columns)
		is.extractColumnsFromExpression(e.Right, columns)
	case *UnaryOp:
		is.extractColumnsFromExpression(e.Expr, columns)
	case *FunctionCall:
		for _, arg := range e.Args {
			is.extractColumnsFromExpression(arg, columns)
		}
	// Add more expression types as needed
	}
}

// estimateCoveringIndexCost estimates the cost of using a covering index
func (is *IndexSelection) estimateCoveringIndexCost(table *catalog.Table, index *catalog.Index, filter *LogicalFilter, numColumns int) float64 {
	if is.costEstimator != nil {
		// Use the cost estimator if available
		baseIndexCost := is.costEstimator.EstimateIndexScanCost(table, index, 0.5) // Assume 50% selectivity
		
		// Index-only scans are cheaper than index scans with heap lookups
		// Reduce cost by 30-50% since we avoid heap access
		coveringBonus := 0.6
		
		// Wider indexes (more columns) have slightly higher cost
		columnPenalty := float64(numColumns) * 0.05
		
		return baseIndexCost.TotalCost * coveringBonus * (1.0 + columnPenalty)
	}
	
	// Simple heuristic: base cost + column penalty
	baseCost := 100.0
	columnCost := float64(numColumns) * 10.0
	return baseCost + columnCost
}

// extractIndexRangeConditions extracts start and end values for index range scans
func (is *IndexSelection) extractIndexRangeConditions(index *catalog.Index, predicate Expression) ([]Expression, []Expression) {
	// For now, return nil to indicate no specific range conditions
	// A full implementation would analyze the predicate to extract range conditions
	// that match the index key columns
	return nil, nil
}

// containsOrPredicate checks if the predicate contains OR operations
func (is *IndexSelection) containsOrPredicate(expr Expression) bool {
	switch e := expr.(type) {
	case *BinaryOp:
		if e.Operator == OpOr {
			return true
		}
		// Check recursively in child expressions
		return is.containsOrPredicate(e.Left) || is.containsOrPredicate(e.Right)
	case *UnaryOp:
		return is.containsOrPredicate(e.Expr)
	case *FunctionCall:
		for _, arg := range e.Args {
			if is.containsOrPredicate(arg) {
				return true
			}
		}
	}
	return false
}

// tryIndexIntersection attempts to use multiple indexes with bitmap operations.
func (is *IndexSelection) tryIndexIntersection(scan *LogicalScan, filter *LogicalFilter) LogicalPlan {
	// Get table metadata
	// Try multiple schemas like in other methods
	var table *catalog.Table
	var err error

	// Try "public" schema first, then "test", then empty
	table, err = is.catalog.GetTable("public", scan.TableName)
	if err != nil {
		table, err = is.catalog.GetTable("test", scan.TableName)
		if err != nil {
			table, err = is.catalog.GetTable("", scan.TableName)
			if err != nil {
				return nil
			}
		}
	}

	// Create intersection planner
	planner := NewIndexIntersectionPlanner(is.catalog, is.costEstimator)

	// Try to create an intersection plan
	intersectionPlan, err := planner.PlanIndexIntersection(table, filter.Predicate)
	if err != nil || intersectionPlan == nil {
		return nil
	}

	// Convert to logical plan nodes
	var bitmapScans []Plan
	for _, group := range intersectionPlan.IndexGroups {
		for _, indexPred := range group.Indexes {
			bitmapScan := &BitmapIndexScan{
				basePlan:  basePlan{schema: scan.schema},
				TableName: scan.TableName,
				Index:     indexPred.Index,
				StartKey:  indexPred.StartValue,
				EndKey:    indexPred.EndValue,
			}
			bitmapScans = append(bitmapScans, bitmapScan)
		}
	}

	// Create bitmap AND operation
	bitmapAnd := &BitmapAnd{
		basePlan:       basePlan{schema: scan.schema},
		BitmapChildren: bitmapScans,
	}

	// Create bitmap heap scan
	heapScan := &BitmapHeapScan{
		basePlan:     basePlan{schema: scan.schema},
		TableName:    scan.TableName,
		BitmapSource: bitmapAnd,
	}

	return heapScan
}

// Apply attempts to replace scan+filter combinations with index scans.
func (is *IndexSelection) Apply(plan LogicalPlan) (LogicalPlan, bool) {
	if is.catalog == nil {
		return plan, false
	}

	// Look for Filter over Scan patterns
	switch p := plan.(type) {
	case *LogicalFilter:
		// Check if the child is a table scan
		if len(p.Children()) > 0 {
			if scan, ok := p.Children()[0].(*LogicalScan); ok {
				// Check if vectorized execution should be used
				if is.vectorizedCostModel != nil && is.vectorizedBatchSize > 0 {
					// Get table stats for row count estimation
					table, err := is.catalog.GetTable("public", scan.TableName)
					if err == nil && table != nil {
						stats, _ := is.catalog.GetTableStats(table.SchemaName, table.TableName)
						estimatedRows := int64(1000) // Default estimate
						if stats != nil {
							estimatedRows = stats.RowCount
						}
						
						// Check if vectorized filter should be used
						memoryAvailable := int64(64 * 1024 * 1024) // 64MB default
						choice := is.costEstimator.GetVectorizedExecutionChoice(
							"filter",
							estimatedRows,
							0.5, // Default selectivity
							p.Predicate,
							memoryAvailable,
						)
						
						if choice.UseVectorized {
							// Create vectorized scan + filter combination
							vectorizedScan := NewVectorizedLogicalScan(scan.TableName, scan.Alias, scan.Schema(), is.vectorizedBatchSize)
							vectorizedFilter := NewVectorizedLogicalFilter(vectorizedScan, p.Predicate, is.vectorizedBatchSize)
							return vectorizedFilter, true
						}
					}
				}
				
				// Try covering index scan first - most efficient since it avoids heap access
				if coveringIndexScan := is.tryCoveringIndexScan(scan, p); coveringIndexScan != nil {
					return coveringIndexScan, true
				}

				// Try composite index scan second (enhanced multi-column index support)
				if is.costEstimator != nil {
					if compositeIndexScan := tryCompositeIndexScanWithCost(scan, p, is.catalog, is.costEstimator); compositeIndexScan != nil {
						// Composite index scan found - return it directly (filter is incorporated)
						return compositeIndexScan.(LogicalPlan), true
					}
				} else {
					// Fallback to simple composite index scan if no cost estimator
					if compositeIndexScan := tryCompositeIndexScan(scan, p, is.catalog); compositeIndexScan != nil {
						// Composite index scan found - return it directly (filter is incorporated)
						return compositeIndexScan.(LogicalPlan), true
					}
				}

				// Try index intersection for multiple predicates
				if is.costEstimator != nil {
					if intersectionPlan := is.tryIndexIntersection(scan, p); intersectionPlan != nil {
						return intersectionPlan, true
					}
				}

				// Fallback to original index scan logic for backward compatibility
				if is.costEstimator != nil {
					if indexScan := tryIndexScanWithCost(scan, p, is.catalog, is.costEstimator); indexScan != nil {
						// Index scan found - return it directly (filter is incorporated)
						return indexScan.(LogicalPlan), true
					}
				} else {
					// Fallback to simple index scan if no cost estimator
					if indexScan := tryIndexScan(scan, p, is.catalog); indexScan != nil {
						// Index scan found - return it directly (filter is incorporated)
						return indexScan.(LogicalPlan), true
					}
				}
			}
		}

		// Recursively apply to children
		if len(p.Children()) > 0 {
			newChild, changed := is.Apply(p.Children()[0].(LogicalPlan))
			if changed {
				return NewLogicalFilter(newChild, p.Predicate), true
			}
		}

	case *LogicalScan:
		// Check if vectorized scan should be used (without filter)
		if is.vectorizedCostModel != nil && is.vectorizedBatchSize > 0 {
			// Get table stats for row count estimation
			table, err := is.catalog.GetTable("public", p.TableName)
			if err == nil && table != nil {
				stats, _ := is.catalog.GetTableStats(table.SchemaName, table.TableName)
				estimatedRows := int64(1000) // Default estimate
				if stats != nil {
					estimatedRows = stats.RowCount
				}
				
				// Check if vectorized scan should be used
				memoryAvailable := int64(64 * 1024 * 1024) // 64MB default
				choice := is.costEstimator.GetVectorizedExecutionChoice(
					"scan",
					estimatedRows,
					1.0, // No selectivity for pure scan
					nil, // No predicate
					memoryAvailable,
				)
				
				if choice.UseVectorized {
					// Create vectorized scan
					return NewVectorizedLogicalScan(p.TableName, p.Alias, p.Schema(), is.vectorizedBatchSize), true
				}
			}
		}

	case *LogicalProject:
		// Apply to children
		if len(p.Children()) > 0 {
			newChild, changed := is.Apply(p.Children()[0].(LogicalPlan))
			if changed {
				return NewLogicalProject(newChild, p.Projections, p.Aliases, p.schema), true
			}
		}

	case *LogicalSort:
		// Apply to children
		if len(p.Children()) > 0 {
			newChild, changed := is.Apply(p.Children()[0].(LogicalPlan))
			if changed {
				return NewLogicalSort(newChild, p.OrderBy), true
			}
		}

	case *LogicalLimit:
		// Apply to children
		if len(p.Children()) > 0 {
			newChild, changed := is.Apply(p.Children()[0].(LogicalPlan))
			if changed {
				return NewLogicalLimit(newChild, p.Limit, p.Offset), true
			}
		}

	case *LogicalJoin:
		// Apply to both children
		leftChild, leftChanged := is.Apply(p.Children()[0].(LogicalPlan))
		rightChild, rightChanged := is.Apply(p.Children()[1].(LogicalPlan))

		if leftChanged || rightChanged {
			return NewLogicalJoin(leftChild, rightChild, p.JoinType, p.Condition, p.schema), true
		}

	case *LogicalAggregate:
		// Apply to children
		if len(p.Children()) > 0 {
			newChild, changed := is.Apply(p.Children()[0].(LogicalPlan))
			if changed {
				return NewLogicalAggregate(newChild, p.GroupBy, p.Aggregates, p.schema), true
			}
		}
	}

	return plan, false
}
