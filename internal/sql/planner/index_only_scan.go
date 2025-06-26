package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// IndexOnlyScan represents a scan that can be satisfied entirely from the index
// without needing to access the table data.
type IndexOnlyScan struct {
	basePlan
	TableName   string
	IndexName   string
	Index       *catalog.Index
	StartValues []Expression
	EndValues   []Expression
	// ProjectedColumns are the columns needed from the index
	ProjectedColumns []string
}

func (s *IndexOnlyScan) logicalNode() {}

func (s *IndexOnlyScan) String() string {
	return fmt.Sprintf("IndexOnlyScan(%s.%s)", s.TableName, s.IndexName)
}

// NewIndexOnlyScan creates a new index-only scan node.
func NewIndexOnlyScan(tableName, indexName string, index *catalog.Index, schema *Schema,
	startValues, endValues []Expression, projectedColumns []string) *IndexOnlyScan {
	return &IndexOnlyScan{
		basePlan: basePlan{
			children: []Plan{},
			schema:   schema,
		},
		TableName:        tableName,
		IndexName:        indexName,
		Index:            index,
		StartValues:      startValues,
		EndValues:        endValues,
		ProjectedColumns: projectedColumns,
	}
}

// IsCoveringIndex checks if the index contains all columns needed for the query.
func IsCoveringIndex(index *catalog.Index, requiredColumns []string) bool {
	// Build a set of columns in the index (both key columns and include columns)
	indexColumns := make(map[string]bool)
	
	// Add key columns
	for _, col := range index.Columns {
		indexColumns[col.Column.Name] = true
	}
	
	// Add include columns (for covering indexes)
	for _, col := range index.IncludeColumns {
		indexColumns[col.Column.Name] = true
	}

	// Check if all required columns are in the index
	for _, col := range requiredColumns {
		if !indexColumns[col] {
			return false
		}
	}

	return true
}

// tryIndexOnlyScan attempts to convert a projection + index scan into an index-only scan.
func tryIndexOnlyScan(project *LogicalProject, indexScan Plan) Plan {
	// Check if the child is a composite index scan
	compIndexScan, ok := indexScan.(*CompositeIndexScan)
	if !ok {
		// Also check for regular index scan
		regIndexScan, ok := indexScan.(*IndexScan)
		if !ok {
			return nil
		}
		// For regular index scan, check if it's covering
		return tryRegularIndexOnlyScan(project, regIndexScan)
	}

	// Extract the columns needed by the projection
	projectedColumns := extractProjectedColumns(project.Projections)
	if projectedColumns == nil {
		return nil // Can't determine columns or contains complex expressions
	}

	// Check if the index is a covering index
	if !IsCoveringIndex(compIndexScan.Index, projectedColumns) {
		return nil
	}

	// Create index-only scan
	// Convert composite values to expressions for consistency
	var startExprs, endExprs []Expression
	for _, val := range compIndexScan.StartValues {
		startExprs = append(startExprs, &Literal{Value: val})
	}
	for _, val := range compIndexScan.EndValues {
		endExprs = append(endExprs, &Literal{Value: val})
	}

	return NewIndexOnlyScan(
		compIndexScan.TableName,
		compIndexScan.IndexName,
		compIndexScan.Index,
		project.Schema(),
		startExprs,
		endExprs,
		projectedColumns,
	)
}

// tryRegularIndexOnlyScan handles regular (single-column) index scans.
func tryRegularIndexOnlyScan(project *LogicalProject, indexScan *IndexScan) Plan {
	// Extract the columns needed by the projection
	projectedColumns := extractProjectedColumns(project.Projections)
	if projectedColumns == nil {
		return nil
	}

	// Check if the index is a covering index
	if !IsCoveringIndex(indexScan.Index, projectedColumns) {
		return nil
	}

	// Create index-only scan
	var startExprs, endExprs []Expression
	if indexScan.StartKey != nil {
		startExprs = []Expression{indexScan.StartKey}
	}
	if indexScan.EndKey != nil {
		endExprs = []Expression{indexScan.EndKey}
	}

	return NewIndexOnlyScan(
		indexScan.TableName,
		indexScan.Index.Name,
		indexScan.Index,
		project.Schema(),
		startExprs,
		endExprs,
		projectedColumns,
	)
}

// extractProjectedColumns extracts column names from projection expressions.
// Returns nil if the projections contain anything other than simple column references.
func extractProjectedColumns(projections []Expression) []string {
	var columns []string

	for _, proj := range projections {
		switch p := proj.(type) {
		case *ColumnRef:
			columns = append(columns, p.ColumnName)
		default:
			// Complex expression, can't use index-only scan
			return nil
		}
	}

	return columns
}

// IndexOnlyScanOptimization is an optimization rule that converts eligible scans
// to index-only scans when possible.
type IndexOnlyScanOptimization struct{}

// Apply attempts to apply index-only scan optimization.
func (opt *IndexOnlyScanOptimization) Apply(plan Plan) (Plan, bool) {
	switch p := plan.(type) {
	case *LogicalProject:
		// Check if child is an index scan
		if len(p.Children()) == 1 {
			optimized := tryIndexOnlyScan(p, p.Children()[0])
			if optimized != nil {
				return optimized, true
			}
		}
	}

	// Recursively apply to children
	children := plan.Children()
	changed := false
	newChildren := make([]Plan, len(children))

	for i, child := range children {
		newChild, childChanged := opt.Apply(child)
		if childChanged {
			changed = true
			newChildren[i] = newChild
		} else {
			newChildren[i] = child
		}
	}

	// Return the original plan if no changes were made to children
	// In a real implementation, you'd need to properly reconstruct the plan
	// with new children while preserving other properties

	return plan, changed
}
