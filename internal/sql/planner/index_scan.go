package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// IndexScan represents a scan operation using an index.
type IndexScan struct {
	basePlan
	TableName string
	IndexName string
	Index     *catalog.Index
	StartKey  Expression // Start of range (inclusive)
	EndKey    Expression // End of range (inclusive)
	Reverse   bool       // Scan in reverse order
}

func (s *IndexScan) logicalNode() {}

func (s *IndexScan) String() string {
	return fmt.Sprintf("IndexScan(%s.%s)", s.TableName, s.IndexName)
}

// canUseIndexForFilter checks if an index can be used for a filter expression.
func canUseIndexForFilter(index *catalog.Index, filter Expression) bool {
	// For now, only handle simple equality and range conditions on single columns
	switch expr := filter.(type) {
	case *BinaryOp:
		// First check for logical operators
		if expr.Operator == OpAnd {
			// For AND, both sides must be usable
			return canUseIndexForFilter(index, expr.Left) && canUseIndexForFilter(index, expr.Right)
		} else if expr.Operator == OpOr {
			// For OR, we can't use the index efficiently (would need multiple scans)
			return false
		}
		
		// Check if this is a comparison on an indexed column
		col, ok := expr.Left.(*ColumnRef)
		if !ok {
			// Try the other side for commutative operators
			if expr.Operator == OpEqual || expr.Operator == OpNotEqual {
				col, ok = expr.Right.(*ColumnRef)
				if !ok {
					return false
				}
			} else {
				return false
			}
		}
		
		// Check if the column is the first column in the index
		if len(index.Columns) == 0 {
			return false
		}
		
		firstIndexCol := index.Columns[0]
		if firstIndexCol.Column.Name != col.ColumnName {
			return false
		}
		
		// Check if the operator is supported
		switch expr.Operator {
		case OpEqual, OpNotEqual, OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
			return true
		default:
			return false
		}
		
	default:
		return false
	}
}

// extractIndexBounds extracts the start and end keys for an index scan from a filter.
func extractIndexBounds(index *catalog.Index, filter Expression) (startKey, endKey Expression, canUse bool) {
	// For now, only handle simple cases
	switch expr := filter.(type) {
	case *BinaryOp:
		col, ok := expr.Left.(*ColumnRef)
		if !ok {
			return nil, nil, false
		}
		
		// Make sure it's the first index column
		if len(index.Columns) == 0 || index.Columns[0].Column.Name != col.ColumnName {
			return nil, nil, false
		}
		
		switch expr.Operator {
		case OpEqual:
			// Exact match: start = end = value
			return expr.Right, expr.Right, true
			
		case OpGreater:
			// Range: (value, +inf)
			// For now, return the value as start, nil as end
			return expr.Right, nil, true
			
		case OpGreaterEqual:
			// Range: [value, +inf)
			return expr.Right, nil, true
			
		case OpLess:
			// Range: (-inf, value)
			return nil, expr.Right, true
			
		case OpLessEqual:
			// Range: (-inf, value]
			return nil, expr.Right, true
			
		default:
			return nil, nil, false
		}
		
		// Handle AND operators separately at the end
		if expr.Operator == OpAnd {
			// Try to combine bounds from both sides
			leftStart, leftEnd, leftOk := extractIndexBounds(index, expr.Left)
			rightStart, rightEnd, rightOk := extractIndexBounds(index, expr.Right)
			
			if !leftOk || !rightOk {
				return nil, nil, false
			}
			
			// Combine bounds (take the most restrictive)
			var start, end Expression
			if leftStart != nil && rightStart != nil {
				// Both have start bounds - would need to compare values
				// For now, just use left
				start = leftStart
			} else if leftStart != nil {
				start = leftStart
			} else {
				start = rightStart
			}
			
			if leftEnd != nil && rightEnd != nil {
				// Both have end bounds - would need to compare values
				// For now, just use left
				end = leftEnd
			} else if leftEnd != nil {
				end = leftEnd
			} else {
				end = rightEnd
			}
			
			return start, end, true
		}
		
	default:
		return nil, nil, false
	}
	
	return nil, nil, false
}

// tryIndexScan attempts to convert a scan+filter into an index scan.
func tryIndexScan(scan *LogicalScan, filter *LogicalFilter, catalog catalog.Catalog) Plan {
	// Get table metadata
	table, err := catalog.GetTable("", scan.TableName)
	if err != nil {
		return nil
	}
	
	// Check each index to see if it can be used
	for _, index := range table.Indexes {
		if canUseIndexForFilter(index, filter.Predicate) {
			startKey, endKey, ok := extractIndexBounds(index, filter.Predicate)
			if ok {
				// Create index scan
				indexScan := &IndexScan{
					TableName: scan.TableName,
					IndexName: index.Name,
					Index:     index,
					StartKey:  startKey,
					EndKey:    endKey,
				}
				indexScan.basePlan.children = []Plan{scan}
				
				// Check if we still need a filter (for conditions not fully covered by index)
				// For now, assume the index fully covers the filter
				return indexScan
			}
		}
	}
	
	// No suitable index found
	return nil
}