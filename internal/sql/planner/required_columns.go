package planner

// This file implements RequiredColumns analysis for all logical plan nodes

// LogicalScan - base tables provide all their columns
func (s *LogicalScan) RequiredColumns() *ColumnSet {
	// Scan nodes don't require columns from children (they have no children)
	// But they need to know what columns parent needs from them
	return NewColumnSet()
}

func (s *LogicalScan) RequiredInputColumns(childIndex int) *ColumnSet {
	// Scan has no children
	return NewColumnSet()
}

// LogicalFilter - needs columns for predicate evaluation plus what parent needs
func (f *LogicalFilter) RequiredColumns() *ColumnSet {
	cols := NewColumnSet()
	extractColumns(f.Predicate, cols)
	return cols
}

func (f *LogicalFilter) RequiredInputColumns(childIndex int) *ColumnSet {
	// Filter needs predicate columns plus all columns needed by parent
	cols := f.RequiredColumns()
	// Note: Parent's requirements will be added by the pushdown algorithm
	return cols
}

// LogicalProject - only needs columns referenced in projections
func (p *LogicalProject) RequiredColumns() *ColumnSet {
	cols := NewColumnSet()
	for _, expr := range p.Projections {
		extractColumns(expr, cols)
	}
	return cols
}

func (p *LogicalProject) RequiredInputColumns(childIndex int) *ColumnSet {
	// Project only needs columns referenced in its projection expressions
	return p.RequiredColumns()
}

// LogicalSort - needs sort columns plus what parent needs
func (s *LogicalSort) RequiredColumns() *ColumnSet {
	cols := NewColumnSet()
	for _, orderBy := range s.OrderBy {
		extractColumns(orderBy.Expr, cols)
	}
	return cols
}

func (s *LogicalSort) RequiredInputColumns(childIndex int) *ColumnSet {
	// Sort needs its sort columns plus all columns needed by parent
	return s.RequiredColumns()
}

// LogicalLimit - passes through parent requirements unchanged
func (l *LogicalLimit) RequiredColumns() *ColumnSet {
	// Limit doesn't add any column requirements
	return NewColumnSet()
}

func (l *LogicalLimit) RequiredInputColumns(childIndex int) *ColumnSet {
	// Limit passes through parent requirements unchanged
	return NewColumnSet()
}

// LogicalJoin - needs join condition columns
func (j *LogicalJoin) RequiredColumns() *ColumnSet {
	cols := NewColumnSet()
	if j.Condition != nil {
		extractColumns(j.Condition, cols)
	}
	return cols
}

func (j *LogicalJoin) RequiredInputColumns(childIndex int) *ColumnSet {
	// Start with columns needed for join condition
	cols := j.RequiredColumns()

	// Filter to only columns that come from the specified child
	// This requires schema information to know which columns come from which side
	if childIndex < len(j.children) && j.children[childIndex] != nil {
		childSchema := j.children[childIndex].Schema()
		if childSchema != nil {
			// For now, we'll need a helper to determine table alias for the child
			// This is a simplification - in practice we'd track this more carefully
			filtered := NewColumnSet()
			for col := range cols.columns {
				if columnExistsInSchema(col.ColumnName, childSchema) {
					filtered.Add(col)
				}
			}
			return filtered
		}
	}

	return cols
}

// LogicalAggregate - needs GROUP BY and aggregate expression columns
func (a *LogicalAggregate) RequiredColumns() *ColumnSet {
	cols := NewColumnSet()

	// GROUP BY columns
	for _, expr := range a.GroupBy {
		extractColumns(expr, cols)
	}

	// Aggregate expression arguments
	for _, agg := range a.Aggregates {
		for _, arg := range agg.Args {
			extractColumns(arg, cols)
		}
	}

	return cols
}

func (a *LogicalAggregate) RequiredInputColumns(childIndex int) *ColumnSet {
	// Aggregate needs all columns referenced in GROUP BY and aggregate expressions
	return a.RequiredColumns()
}

// LogicalValues - no column requirements (generates its own data)
func (v *LogicalValues) RequiredColumns() *ColumnSet {
	return NewColumnSet()
}

func (v *LogicalValues) RequiredInputColumns(childIndex int) *ColumnSet {
	// Values has no children
	return NewColumnSet()
}
