package planner

// LogicalDistinct represents a DISTINCT operation in the logical plan.
type LogicalDistinct struct {
	basePlan
	child Plan
}

// NewLogicalDistinct creates a new logical distinct plan.
func NewLogicalDistinct(child Plan) *LogicalDistinct {
	return &LogicalDistinct{
		basePlan: basePlan{
			schema: child.Schema(), // DISTINCT preserves the schema
		},
		child: child,
	}
}

// Children returns the child plans.
func (d *LogicalDistinct) Children() []Plan {
	return []Plan{d.child}
}

// WithChildren returns a copy of the plan with new children.
func (d *LogicalDistinct) WithChildren(children []Plan) Plan {
	if len(children) != 1 {
		panic("LogicalDistinct expects exactly 1 child")
	}
	return NewLogicalDistinct(children[0])
}

// String returns a string representation of the plan.
func (d *LogicalDistinct) String() string {
	return "Distinct"
}

// logicalNode marks this as a logical plan node.
func (d *LogicalDistinct) logicalNode() {}
