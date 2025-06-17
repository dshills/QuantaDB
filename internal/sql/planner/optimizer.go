package planner

// OptimizationRule represents a rule that transforms logical plans.
type OptimizationRule interface {
	// Apply attempts to apply this rule to the given plan.
	// Returns the transformed plan and true if the rule was applied.
	Apply(plan LogicalPlan) (LogicalPlan, bool)
}

// Optimizer applies optimization rules to logical plans.
type Optimizer struct {
	rules []OptimizationRule
}

// NewOptimizer creates a new optimizer with default rules.
func NewOptimizer() *Optimizer {
	return &Optimizer{
		rules: []OptimizationRule{
			&PredicatePushdown{},
			&ProjectionPushdown{},
			&ConstantFolding{},
		},
	}
}

// Optimize applies all optimization rules to a plan until no more changes occur.
func (o *Optimizer) Optimize(plan LogicalPlan) LogicalPlan {
	maxIterations := 100 // Prevent infinite loops
	
	for i := 0; i < maxIterations; i++ {
		changed := false
		
		for _, rule := range o.rules {
			newPlan, applied := rule.Apply(plan)
			if applied {
				plan = newPlan
				changed = true
			}
		}
		
		if !changed {
			break
		}
	}
	
	return plan
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
	// TODO: Implement projection pushdown
	// This would track which columns are actually needed and push
	// projections down to eliminate unnecessary columns early
	return plan, false
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