package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// IndexIntersectionPlanner analyzes predicates to find opportunities for index intersection.
type IndexIntersectionPlanner struct {
	catalog       catalog.Catalog
	costEstimator *CostEstimator
}

// NewIndexIntersectionPlanner creates a new index intersection planner.
func NewIndexIntersectionPlanner(catalog catalog.Catalog, costEstimator *CostEstimator) *IndexIntersectionPlanner {
	return &IndexIntersectionPlanner{
		catalog:       catalog,
		costEstimator: costEstimator,
	}
}

// IndexIntersectionPlan represents a plan using multiple indexes with bitmap operations.
type IndexIntersectionPlan struct {
	Table       string
	IndexGroups []IndexGroup // Groups of indexes to be combined
	Operation   BitmapOpType // AND or OR operation between groups
}

// IndexGroup represents a group of indexes to be combined.
type IndexGroup struct {
	Indexes   []IndexPredicate
	Operation BitmapOpType // AND or OR within the group
}

// IndexPredicate represents an index with its predicate.
type IndexPredicate struct {
	Index      *catalog.Index
	Predicate  Expression
	StartValue types.Value
	EndValue   types.Value
}

// BitmapOpType represents a bitmap operation type.
type BitmapOpType int

const (
	BitmapOpAnd BitmapOpType = iota
	BitmapOpOr
)

// PlanIndexIntersection analyzes predicates to create an index intersection plan.
func (p *IndexIntersectionPlanner) PlanIndexIntersection(
	table *catalog.Table,
	predicates Expression,
) (*IndexIntersectionPlan, error) {
	// Extract individual predicates from AND/OR expressions
	conjuncts := p.extractConjuncts(predicates)
	if len(conjuncts) < 2 {
		// Need at least 2 predicates for intersection
		return nil, nil //nolint:nilnil // Intentional - no error, no plan
	}

	// Find indexes that can satisfy each predicate
	indexMatches := make([]IndexPredicate, 0)
	for _, pred := range conjuncts {
		if match := p.findIndexForPredicate(table, pred); match != nil {
			indexMatches = append(indexMatches, *match)
		}
	}

	if len(indexMatches) < 2 {
		// Need at least 2 indexes for intersection
		return nil, nil //nolint:nilnil // Intentional - no error, no plan
	}

	// Check if intersection would be beneficial
	if !p.isIntersectionBeneficial(table, indexMatches) {
		return nil, nil //nolint:nilnil // Intentional - no error, no plan
	}

	// Create intersection plan
	plan := &IndexIntersectionPlan{
		Table:     table.TableName,
		Operation: BitmapOpAnd,
		IndexGroups: []IndexGroup{
			{
				Indexes:   indexMatches,
				Operation: BitmapOpAnd,
			},
		},
	}

	return plan, nil
}

// extractConjuncts extracts individual predicates from AND expressions.
func (p *IndexIntersectionPlanner) extractConjuncts(expr Expression) []Expression {
	var conjuncts []Expression

	switch e := expr.(type) {
	case *BinaryOp:
		if e.Operator == OpAnd {
			// Recursively extract from both sides
			conjuncts = append(conjuncts, p.extractConjuncts(e.Left)...)
			conjuncts = append(conjuncts, p.extractConjuncts(e.Right)...)
		} else {
			// Single predicate
			conjuncts = append(conjuncts, expr)
		}
	default:
		conjuncts = append(conjuncts, expr)
	}

	return conjuncts
}

// findIndexForPredicate finds an index that can satisfy a predicate.
func (p *IndexIntersectionPlanner) findIndexForPredicate(
	table *catalog.Table,
	predicate Expression,
) *IndexPredicate {
	// Look for simple column predicates
	binOp, ok := predicate.(*BinaryOp)
	if !ok {
		return nil
	}

	// Extract column reference
	var colRef *ColumnRef
	var value Expression

	if ref, ok := binOp.Left.(*ColumnRef); ok {
		colRef = ref
		value = binOp.Right
	} else if ref, ok := binOp.Right.(*ColumnRef); ok && binOp.Operator == OpEqual {
		colRef = ref
		value = binOp.Left
	} else {
		return nil
	}

	// Find single-column index on this column
	for _, idx := range table.Indexes {
		if len(idx.Columns) == 1 && idx.Columns[0].Column.Name == colRef.ColumnName {
			// Evaluate the value expression
			// For now, only handle literal values
			literal, ok := value.(*Literal)
			if !ok {
				continue
			}
			val := literal.Value

			// Create index predicate based on operator
			match := &IndexPredicate{
				Index:     idx,
				Predicate: predicate,
			}

			switch binOp.Operator {
			case OpEqual:
				match.StartValue = val
				match.EndValue = val
			case OpLess, OpLessEqual:
				match.EndValue = val
			case OpGreater, OpGreaterEqual:
				match.StartValue = val
			default:
				continue
			}

			return match
		}
	}

	return nil
}

// isIntersectionBeneficial determines if index intersection would be faster than single index.
func (p *IndexIntersectionPlanner) isIntersectionBeneficial(
	table *catalog.Table,
	matches []IndexPredicate,
) bool {
	// Simple heuristic: intersection is beneficial if each index is selective
	// and the combination would significantly reduce rows

	// Estimate selectivity of each index
	totalSelectivity := 1.0
	for _, match := range matches {
		// Get column stats
		colName := match.Index.Columns[0].Column.Name
		var stats *catalog.ColumnStats
		// Try to get column from table
		for _, col := range table.Columns {
			if col.Name == colName {
				stats = col.Stats
				break
			}
		}

		selectivity := 0.5 // Default if no stats
		if stats != nil {
			selectivity = p.estimatePredicateSelectivity(stats, match.Predicate)
		}

		totalSelectivity *= selectivity
	}

	// Intersection is beneficial if combined selectivity is low enough
	// and we have at least 2 highly selective indexes
	return totalSelectivity < 0.1 && len(matches) >= 2
}

// estimatePredicateSelectivity estimates selectivity for a predicate.
func (p *IndexIntersectionPlanner) estimatePredicateSelectivity(
	stats *catalog.ColumnStats,
	predicate Expression,
) float64 {
	binOp, ok := predicate.(*BinaryOp)
	if !ok {
		return 0.5
	}

	switch binOp.Operator {
	case OpEqual:
		if stats.DistinctCount > 0 {
			return 1.0 / float64(stats.DistinctCount)
		}
		return 0.1
	case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		// Range predicates are less selective
		return 0.3
	default:
		return 0.5
	}
}

// BitmapIndexScan represents a bitmap index scan plan node.
type BitmapIndexScan struct {
	basePlan
	TableName string
	Index     *catalog.Index
	StartKey  types.Value
	EndKey    types.Value
}

func (n *BitmapIndexScan) logicalNode() {}

// BitmapAnd represents a bitmap AND operation plan node.
type BitmapAnd struct {
	basePlan
	BitmapChildren []Plan
}

func (n *BitmapAnd) logicalNode() {}

// BitmapOr represents a bitmap OR operation plan node.
type BitmapOr struct {
	basePlan
	BitmapChildren []Plan
}

func (n *BitmapOr) logicalNode() {}

// BitmapHeapScan represents a bitmap heap scan plan node.
type BitmapHeapScan struct {
	basePlan
	TableName    string
	BitmapSource Plan // BitmapAnd, BitmapOr, or BitmapIndexScan
}

func (n *BitmapHeapScan) logicalNode() {}

// Children implements the Plan interface.
func (n *BitmapIndexScan) Children() []Plan { return nil }
func (n *BitmapAnd) Children() []Plan       { return n.BitmapChildren }
func (n *BitmapOr) Children() []Plan        { return n.BitmapChildren }
func (n *BitmapHeapScan) Children() []Plan  { return []Plan{n.BitmapSource} }

// String implements the Plan interface.
func (n *BitmapIndexScan) String() string {
	return fmt.Sprintf("BitmapIndexScan[%s.%s]", n.TableName, n.Index.Name)
}
func (n *BitmapAnd) String() string {
	return fmt.Sprintf("BitmapAnd[%d children]", len(n.BitmapChildren))
}
func (n *BitmapOr) String() string {
	return fmt.Sprintf("BitmapOr[%d children]", len(n.BitmapChildren))
}
func (n *BitmapHeapScan) String() string {
	return fmt.Sprintf("BitmapHeapScan[%s]", n.TableName)
}
