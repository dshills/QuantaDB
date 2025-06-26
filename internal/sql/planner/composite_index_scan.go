package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CompositeIndexScan represents a scan operation using a composite (multi-column) index.
type CompositeIndexScan struct {
	basePlan
	TableName        string
	IndexName        string
	Index            *catalog.Index
	StartValues      []types.Value        // Start values for composite key (inclusive)
	EndValues        []types.Value        // End values for composite key (inclusive)
	Reverse          bool                 // Scan in reverse order
	IndexMatch       *CompositeIndexMatch // Details about how the index matches predicates
	PushedPredicates Expression           // Additional predicates to evaluate during scan
}

func (s *CompositeIndexScan) logicalNode() {}

func (s *CompositeIndexScan) String() string {
	return fmt.Sprintf("CompositeIndexScan(%s.%s)", s.TableName, s.IndexName)
}

// NewCompositeIndexScan creates a new composite index scan node.
func NewCompositeIndexScan(tableName, indexName string, index *catalog.Index, schema *Schema,
	startValues, endValues []types.Value, indexMatch *CompositeIndexMatch) *CompositeIndexScan {
	return &CompositeIndexScan{
		basePlan: basePlan{
			children: []Plan{},
			schema:   schema,
		},
		TableName:   tableName,
		IndexName:   indexName,
		Index:       index,
		StartValues: startValues,
		EndValues:   endValues,
		Reverse:     false,
		IndexMatch:  indexMatch,
	}
}

// tryCompositeIndexScan attempts to convert a scan+filter into a composite index scan.
func tryCompositeIndexScan(scan *LogicalScan, filter *LogicalFilter, cat catalog.Catalog) Plan {
	// Try to get table metadata - try both common schemas
	var table *catalog.Table
	var err error

	// Try "public" schema first
	table, err = cat.GetTable("public", scan.TableName)
	if err != nil {
		// Try "test" schema
		table, err = cat.GetTable("test", scan.TableName)
		if err != nil {
			// Try empty schema (might default to public)
			table, err = cat.GetTable("", scan.TableName)
			if err != nil {
				return nil
			}
		}
	}

	// Use composite index matcher for enhanced index selection
	matcher := NewCompositeIndexMatcher()
	bestMatch := matcher.FindBestIndexMatch(table, filter.Predicate)

	if bestMatch != nil {
		// Extract composite key bounds using the matcher
		startValues, endValues, canUse := matcher.ExtractCompositeIndexBounds(bestMatch.Index, filter.Predicate)

		if canUse {
			// Create composite index scan
			indexScan := NewCompositeIndexScan(
				scan.TableName,
				bestMatch.Index.Name,
				bestMatch.Index,
				scan.Schema(),
				startValues,
				endValues,
				bestMatch,
			)
			return indexScan
		}
	}

	// No suitable composite index found
	return nil
}

// tryCompositeIndexScanWithCost attempts to create a composite index scan using cost-based optimization.
func tryCompositeIndexScanWithCost(scan *LogicalScan, filter *LogicalFilter, cat catalog.Catalog, costEstimator *CostEstimator) Plan {
	// Get table metadata - try multiple schemas like in the original function
	var table *catalog.Table
	var err error

	// Try "public" schema first, then "test", then empty
	table, err = cat.GetTable("public", scan.TableName)
	if err != nil {
		table, err = cat.GetTable("test", scan.TableName)
		if err != nil {
			table, err = cat.GetTable("", scan.TableName)
			if err != nil {
				return nil
			}
		}
	}

	// Use composite index matcher for enhanced index selection
	matcher := NewCompositeIndexMatcher()
	bestMatch := matcher.FindBestIndexMatch(table, filter.Predicate)

	if bestMatch == nil {
		return nil
	}

	// Use cost estimator to determine if this index is worth using
	if costEstimator != nil && costEstimator.ShouldUseIndex(table, bestMatch.Index, filter.Predicate) {
		// Calculate cost for the best matching index
		selectivity := costEstimator.EstimateSelectivity(table, filter.Predicate)
		indexCost := costEstimator.EstimateIndexScanCost(table, bestMatch.Index, selectivity)

		// Check if index scan is better than table scan
		tableCost := costEstimator.EstimateTableScanCost(table, 1.0)
		if indexCost.TotalCost < tableCost.TotalCost {
			// Extract composite key bounds using the matcher
			startValues, endValues, canUse := matcher.ExtractCompositeIndexBounds(bestMatch.Index, filter.Predicate)

			if canUse {
				// Create composite index scan
				indexScan := NewCompositeIndexScan(
					scan.TableName,
					bestMatch.Index.Name,
					bestMatch.Index,
					scan.Schema(),
					startValues,
					endValues,
					bestMatch,
				)
				return indexScan
			}
		}
	}

	// No cost-effective composite index found
	return nil
}

// IsCompositeIndexScan checks if a plan is a composite index scan or index-only scan
// Note: This function name is kept for backward compatibility with existing tests
func IsCompositeIndexScan(plan Plan) bool {
	_, isComposite := plan.(*CompositeIndexScan)
	_, isIndexOnly := plan.(*IndexOnlyScan)
	return isComposite || isIndexOnly
}

// IndexScanResult represents common properties for both CompositeIndexScan and IndexOnlyScan
type IndexScanResult struct {
	IndexName       string
	MatchingColumns int
	Plan            Plan
}

// GetCompositeIndexScan safely extracts index scan information from CompositeIndexScan or IndexOnlyScan
func GetCompositeIndexScan(plan Plan) (*IndexScanResult, bool) {
	switch scan := plan.(type) {
	case *CompositeIndexScan:
		matchingColumns := 0
		if scan.IndexMatch != nil {
			matchingColumns = scan.IndexMatch.MatchingColumns
		}
		return &IndexScanResult{
			IndexName:       scan.IndexName,
			MatchingColumns: matchingColumns,
			Plan:            plan,
		}, true
	case *IndexOnlyScan:
		// For IndexOnlyScan, we need to estimate matching columns
		// Since IndexOnlyScan is typically chosen when it's an optimization of a CompositeIndexScan,
		// we can make a reasonable estimate based on the index structure
		matchingColumns := len(scan.Index.Columns) // Assume the index is being used effectively
		
		// If we have specific start/end values, use that as a better estimate
		if len(scan.StartValues) > 0 || len(scan.EndValues) > 0 {
			startLen := len(scan.StartValues)
			endLen := len(scan.EndValues)
			maxRangeColumns := startLen
			if endLen > startLen {
				maxRangeColumns = endLen
			}
			if maxRangeColumns > 0 && maxRangeColumns <= len(scan.Index.Columns) {
				matchingColumns = maxRangeColumns
			}
		}
		
		return &IndexScanResult{
			IndexName:       scan.IndexName,
			MatchingColumns: matchingColumns,
			Plan:            plan,
		}, true
	default:
		return nil, false
	}
}
