package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CompositeIndexMatch represents how well a composite index matches a set of predicates
type CompositeIndexMatch struct {
	// Index being evaluated
	Index *catalog.Index

	// Number of leading columns that can be used (leftmost prefix rule)
	MatchingColumns int

	// Column predicates in index order
	ColumnPredicates []*ColumnPredicate

	// Whether any columns use range predicates
	HasRangePredicates bool

	// Estimated selectivity (0.0 to 1.0)
	Selectivity float64

	// Start and end keys for the composite index scan
	StartKey CompositeKey
	EndKey   CompositeKey
}

// ColumnPredicate represents a predicate on a single column
type ColumnPredicate struct {
	ColumnName string
	Operator   BinaryOperator
	Value      Expression
	IsRange    bool // true for <, <=, >, >=
}

// CompositeKey represents a multi-column key for index bounds
type CompositeKey struct {
	Values []Expression
}

// CompositeIndexMatcher analyzes predicates and finds the best composite index matches
type CompositeIndexMatcher struct{}

// NewCompositeIndexMatcher creates a new composite index matcher
func NewCompositeIndexMatcher() *CompositeIndexMatcher {
	return &CompositeIndexMatcher{}
}

// FindBestIndexMatch finds the best composite index for the given predicate
func (m *CompositeIndexMatcher) FindBestIndexMatch(table *catalog.Table, predicate Expression) *CompositeIndexMatch {
	if table == nil || len(table.Indexes) == 0 {
		return nil
	}

	var bestMatch *CompositeIndexMatch
	bestScore := 0.0

	// Extract column predicates from the WHERE clause
	columnPredicates := m.extractColumnPredicates(predicate)
	if len(columnPredicates) == 0 {
		return nil
	}

	// Evaluate each index
	for _, index := range table.Indexes {
		match := m.evaluateIndexForPredicates(index, columnPredicates)
		if match != nil && match.MatchingColumns > 0 {
			// Calculate a score based on matching columns and selectivity
			score := float64(match.MatchingColumns) + (1.0 - match.Selectivity)
			if score > bestScore {
				bestScore = score
				bestMatch = match
			}
		}
	}

	return bestMatch
}

// extractColumnPredicates extracts individual column predicates from a complex expression
func (m *CompositeIndexMatcher) extractColumnPredicates(expr Expression) map[string]*ColumnPredicate {
	predicates := make(map[string]*ColumnPredicate)
	m.extractPredicatesRecursive(expr, predicates)
	return predicates
}

// extractPredicatesRecursive recursively extracts predicates from nested AND expressions
func (m *CompositeIndexMatcher) extractPredicatesRecursive(expr Expression, predicates map[string]*ColumnPredicate) {
	switch e := expr.(type) {
	case *BinaryOp:
		switch e.Operator {
		case OpAnd:
			// Recursively process both sides of AND
			m.extractPredicatesRecursive(e.Left, predicates)
			m.extractPredicatesRecursive(e.Right, predicates)

		case OpEqual, OpNotEqual, OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
			// Extract column predicate
			if colRef, ok := e.Left.(*ColumnRef); ok {
				predicates[colRef.ColumnName] = &ColumnPredicate{
					ColumnName: colRef.ColumnName,
					Operator:   e.Operator,
					Value:      e.Right,
					IsRange:    m.isRangeOperator(e.Operator),
				}
			} else if colRef, ok := e.Right.(*ColumnRef); ok && (e.Operator == OpEqual || e.Operator == OpNotEqual) {
				// Handle commutative operators where column is on right side
				predicates[colRef.ColumnName] = &ColumnPredicate{
					ColumnName: colRef.ColumnName,
					Operator:   e.Operator,
					Value:      e.Left,
					IsRange:    m.isRangeOperator(e.Operator),
				}
			}
		case OpOr, OpLike, OpNotLike, OpIn, OpNotIn, OpIs, OpIsNot,
			OpAdd, OpSubtract, OpMultiply, OpDivide, OpModulo, OpConcat:
			// These operators are not supported for index matching
			// Do nothing - they cannot be used for index bounds
		}
	}
}

// isRangeOperator returns true if the operator represents a range condition
func (m *CompositeIndexMatcher) isRangeOperator(op BinaryOperator) bool {
	switch op {
	case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		return true
	default:
		return false
	}
}

// evaluateIndexForPredicates evaluates how well an index matches the given predicates
func (m *CompositeIndexMatcher) evaluateIndexForPredicates(index *catalog.Index, columnPredicates map[string]*ColumnPredicate) *CompositeIndexMatch {
	if len(index.Columns) == 0 {
		return nil
	}

	match := &CompositeIndexMatch{
		Index:            index,
		ColumnPredicates: make([]*ColumnPredicate, 0, len(index.Columns)),
		Selectivity:      1.0,
	}

	// Apply leftmost prefix rule - check columns in index order
	for i, indexCol := range index.Columns {
		predicate, exists := columnPredicates[indexCol.Column.Name]
		if !exists {
			// Stop at first missing column (leftmost prefix rule)
			break
		}

		// Check if this is a supported predicate type
		if !m.isSupportedPredicate(predicate) {
			break
		}

		match.MatchingColumns = i + 1
		match.ColumnPredicates = append(match.ColumnPredicates, predicate)

		// Update selectivity estimation
		match.Selectivity *= m.estimatePredicateSelectivity(predicate)

		// Check for range predicates
		if predicate.IsRange {
			match.HasRangePredicates = true
			// After a range predicate, we can't use subsequent columns efficiently
			break
		}
	}

	// Only return matches that use at least one column
	if match.MatchingColumns == 0 {
		return nil
	}

	// Build composite keys for index bounds
	m.buildCompositeKeys(match)

	return match
}

// isSupportedPredicate checks if a predicate can be used with index scans
func (m *CompositeIndexMatcher) isSupportedPredicate(predicate *ColumnPredicate) bool {
	switch predicate.Operator {
	case OpEqual, OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		return true
	case OpNotEqual:
		// NOT EQUAL predicates are not efficiently supported by B+Tree range scans
		return false
	default:
		return false
	}
}

// estimatePredicateSelectivity provides a rough selectivity estimate for different predicate types
func (m *CompositeIndexMatcher) estimatePredicateSelectivity(predicate *ColumnPredicate) float64 {
	switch predicate.Operator {
	case OpEqual:
		return 0.1 // Assume equality is highly selective
	case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		return 0.3 // Range predicates are less selective
	default:
		return 1.0 // Unknown predicates assumed to be non-selective
	}
}

// buildCompositeKeys constructs start and end keys for the composite index scan
func (m *CompositeIndexMatcher) buildCompositeKeys(match *CompositeIndexMatch) {
	startValues := make([]Expression, 0, match.MatchingColumns)
	endValues := make([]Expression, 0, match.MatchingColumns)

	for _, predicate := range match.ColumnPredicates {
		switch predicate.Operator {
		case OpEqual:
			// For equality, both start and end keys use the same value
			startValues = append(startValues, predicate.Value)
			endValues = append(endValues, predicate.Value)

		case OpGreater:
			// For >, start is the value (exclusive), end is open
			startValues = append(startValues, predicate.Value)
			// Don't add to endValues - we want an open range

		case OpGreaterEqual:
			// For >=, start is the value (inclusive), end is open
			startValues = append(startValues, predicate.Value)
			// Don't add to endValues - we want an open range

		case OpLess:
			// For <, start is open, end is the value (exclusive)
			endValues = append(endValues, predicate.Value)
			// Don't add to startValues - we want an open range

		case OpLessEqual:
			// For <=, start is open, end is the value (inclusive)
			endValues = append(endValues, predicate.Value)
			// Don't add to startValues - we want an open range

		case OpNotEqual, OpOr, OpAnd, OpLike, OpNotLike, OpIn, OpNotIn, OpIs, OpIsNot,
			OpAdd, OpSubtract, OpMultiply, OpDivide, OpModulo, OpConcat:
			// These operators are not supported for composite key building
			// Skip them
		}

		// After any range predicate, we can't add more columns
		if predicate.IsRange {
			break
		}
	}

	// Only set keys if we have values
	if len(startValues) > 0 {
		match.StartKey = CompositeKey{Values: startValues}
	}
	if len(endValues) > 0 {
		match.EndKey = CompositeKey{Values: endValues}
	}
}

// CanUseIndexForFilter is an enhanced version that handles composite predicates
func (m *CompositeIndexMatcher) CanUseIndexForFilter(index *catalog.Index, filter Expression) bool {
	columnPredicates := m.extractColumnPredicates(filter)
	match := m.evaluateIndexForPredicates(index, columnPredicates)
	return match != nil && match.MatchingColumns > 0
}

// ExtractCompositeIndexBounds extracts start and end keys for composite index scans
func (m *CompositeIndexMatcher) ExtractCompositeIndexBounds(index *catalog.Index, filter Expression) (startValues, endValues []types.Value, canUse bool) {
	columnPredicates := m.extractColumnPredicates(filter)
	match := m.evaluateIndexForPredicates(index, columnPredicates)

	if match == nil || match.MatchingColumns == 0 {
		return nil, nil, false
	}

	// Convert expressions to values
	// Note: This is a simplified conversion - in a real implementation,
	// you'd need to evaluate the expressions with proper type conversion
	var startVals, endVals []types.Value

	if len(match.StartKey.Values) > 0 {
		startVals = make([]types.Value, len(match.StartKey.Values))
		for i, expr := range match.StartKey.Values {
			if lit, ok := expr.(*Literal); ok {
				startVals[i] = lit.Value
			} else {
				// For non-literal expressions, we'd need parameter evaluation
				// For now, return false to indicate we can't use the index
				return nil, nil, false
			}
		}
	}

	if len(match.EndKey.Values) > 0 {
		endVals = make([]types.Value, len(match.EndKey.Values))
		for i, expr := range match.EndKey.Values {
			if lit, ok := expr.(*Literal); ok {
				endVals[i] = lit.Value
			} else {
				// For non-literal expressions, we'd need parameter evaluation
				return nil, nil, false
			}
		}
	}

	return startVals, endVals, true
}

// IsExactMatch returns true if the index can be used for exact matching
func (match *CompositeIndexMatch) IsExactMatch() bool {
	return match.MatchingColumns == len(match.Index.Columns) && !match.HasRangePredicates
}

// IsPrefixMatch returns true if the index uses only some leading columns
func (match *CompositeIndexMatch) IsPrefixMatch() bool {
	return match.MatchingColumns > 0 && match.MatchingColumns < len(match.Index.Columns)
}

// String returns a string representation of the index match
func (match *CompositeIndexMatch) String() string {
	return fmt.Sprintf("IndexMatch(%s: %d/%d columns, selectivity=%.3f)",
		match.Index.Name, match.MatchingColumns, len(match.Index.Columns), match.Selectivity)
}
