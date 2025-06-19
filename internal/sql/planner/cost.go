package planner

import (
	"math"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// CostParams defines system-wide cost parameters for optimization decisions.
type CostParams struct {
	SequentialPageCost float64 // Cost of sequential page read (baseline: 1.0)
	RandomPageCost     float64 // Cost of random page read (typically 4.0)
	CPUTupleCost       float64 // Cost of processing one row (typically 0.01)
	CPUIndexTupleCost  float64 // Cost of processing index entry (typically 0.005)
	CPUOperatorCost    float64 // Cost of operator evaluation (typically 0.0025)
}

// DefaultCostParams returns standard cost parameters based on PostgreSQL defaults.
func DefaultCostParams() *CostParams {
	return &CostParams{
		SequentialPageCost: 1.0,
		RandomPageCost:     4.0,
		CPUTupleCost:       0.01,
		CPUIndexTupleCost:  0.005,
		CPUOperatorCost:    0.0025,
	}
}

// CostEstimator provides cost estimation for different access methods.
type CostEstimator struct {
	params  *CostParams
	catalog catalog.Catalog
}

// NewCostEstimator creates a new cost estimator.
func NewCostEstimator(catalog catalog.Catalog) *CostEstimator {
	return &CostEstimator{
		params:  DefaultCostParams(),
		catalog: catalog,
	}
}

// EstimateTableScanCost calculates the cost of a sequential table scan.
func (ce *CostEstimator) EstimateTableScanCost(table *catalog.Table, selectivity float64) Cost {
	// Get table statistics
	stats, err := ce.catalog.GetTableStats(table.SchemaName, table.TableName)
	if err != nil || stats == nil {
		// Fallback to default estimates
		stats = &catalog.TableStats{
			RowCount:   1000, // Default row count
			PageCount:  100,  // Default page count
			AvgRowSize: 100,  // Default row size in bytes
		}
	}

	// Ensure minimum values
	if stats.RowCount <= 0 {
		stats.RowCount = 1000
	}
	if stats.PageCount <= 0 {
		stats.PageCount = int64(math.Max(1, float64(stats.RowCount)/100)) // ~100 rows per page
	}

	// Sequential scan cost = (startup cost) + (page I/O cost) + (CPU cost)
	startupCost := 0.0 // No startup cost for table scan

	// I/O cost: read all pages sequentially
	ioCost := float64(stats.PageCount) * ce.params.SequentialPageCost

	// CPU cost: process all rows, return fraction based on selectivity
	cpuCost := float64(stats.RowCount) * ce.params.CPUTupleCost

	totalCost := startupCost + ioCost + cpuCost
	estimatedRows := float64(stats.RowCount) * selectivity

	return Cost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		Rows:        estimatedRows,
		Width:       stats.AvgRowSize,
	}
}

// EstimateIndexScanCost calculates the cost of an index scan.
func (ce *CostEstimator) EstimateIndexScanCost(table *catalog.Table, index *catalog.Index, selectivity float64) Cost {
	// Get table statistics
	tableStats, err := ce.catalog.GetTableStats(table.SchemaName, table.TableName)
	if err != nil || tableStats == nil {
		tableStats = &catalog.TableStats{
			RowCount:   1000,
			PageCount:  100,
			AvgRowSize: 100,
		}
	}

	// Ensure minimum values
	if tableStats.RowCount <= 0 {
		tableStats.RowCount = 1000
	}

	// Estimate index height (B+ tree levels)
	// Assume ~200 entries per index page for a rough estimate
	indexHeight := math.Max(1, math.Log(float64(tableStats.RowCount))/math.Log(200))

	// Estimated number of matching rows
	matchingRows := float64(tableStats.RowCount) * selectivity

	// Index scan cost components:

	// 1. Startup cost: Navigate to first matching key in B+ tree
	startupCost := indexHeight * ce.params.RandomPageCost

	// 2. Index I/O cost: Read index pages for matching entries
	// For range scans, estimate index pages needed
	indexPagesRead := math.Max(1, matchingRows/200) // ~200 entries per index page
	indexIOCost := indexPagesRead * ce.params.RandomPageCost

	// 3. Heap I/O cost: Random access to fetch actual rows
	// Assume some clustering - not every row requires a separate page read
	clusteringFactor := 0.1 // 10% of matching rows require new page reads
	heapPagesRead := math.Max(1, matchingRows*clusteringFactor)
	heapIOCost := heapPagesRead * ce.params.RandomPageCost

	// 4. CPU costs
	indexCPUCost := matchingRows * ce.params.CPUIndexTupleCost
	heapCPUCost := matchingRows * ce.params.CPUTupleCost

	totalCost := startupCost + indexIOCost + heapIOCost + indexCPUCost + heapCPUCost

	return Cost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		Rows:        matchingRows,
		Width:       tableStats.AvgRowSize,
	}
}

// EstimateSelectivity estimates the selectivity of a filter predicate.
func (ce *CostEstimator) EstimateSelectivity(table *catalog.Table, filter Expression) float64 {
	switch expr := filter.(type) {
	case *BinaryOp:
		return ce.estimateBinaryOpSelectivity(table, expr)
	default:
		// Default selectivity for unknown expressions
		return 0.1
	}
}

// estimateBinaryOpSelectivity estimates selectivity for binary operations.
func (ce *CostEstimator) estimateBinaryOpSelectivity(table *catalog.Table, expr *BinaryOp) float64 {
	switch expr.Operator {
	case OpAnd:
		// For AND: multiply selectivities (assuming independence)
		leftSel := ce.EstimateSelectivity(table, expr.Left)
		rightSel := ce.EstimateSelectivity(table, expr.Right)
		return leftSel * rightSel

	case OpOr:
		// For OR: add selectivities minus their intersection
		leftSel := ce.EstimateSelectivity(table, expr.Left)
		rightSel := ce.EstimateSelectivity(table, expr.Right)
		return leftSel + rightSel - (leftSel * rightSel)

	case OpEqual:
		// Equality: use column statistics if available
		if col, ok := expr.Left.(*ColumnRef); ok {
			return ce.estimateEqualitySelectivity(table, col.ColumnName)
		}
		return 0.05 // Default for equality predicates

	case OpNotEqual:
		// Not equal: complement of equality
		return 1.0 - ce.estimateBinaryOpSelectivity(table, &BinaryOp{
			Left:     expr.Left,
			Right:    expr.Right,
			Operator: OpEqual,
		})

	case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		// Range predicates: use histogram if available, otherwise default
		if col, ok := expr.Left.(*ColumnRef); ok {
			return ce.estimateRangeSelectivity(table, col.ColumnName, expr.Operator)
		}
		return 0.3 // Default for range predicates

	case OpAdd, OpSubtract, OpMultiply, OpDivide, OpModulo, OpConcat,
		OpLike, OpNotLike, OpIn, OpNotIn, OpIs, OpIsNot:
		// These operators are not typically used in WHERE clauses for selectivity
		return 0.1 // Conservative default

	default:
		return 0.1 // Conservative default
	}
}

// estimateEqualitySelectivity estimates selectivity for equality predicates.
func (ce *CostEstimator) estimateEqualitySelectivity(table *catalog.Table, columnName string) float64 {
	// Try to get column statistics
	if colStats := ce.getColumnStats(table, columnName); colStats != nil && colStats.DistinctCount > 0 {
		// If we have distinct count, use it for selectivity
		return 1.0 / float64(colStats.DistinctCount)
	}

	// Check if it's a primary key or unique column
	for _, constraint := range table.Constraints {
		switch c := constraint.(type) {
		case catalog.PrimaryKeyConstraint:
			for _, col := range c.Columns {
				if col == columnName {
					return 1.0 / 1000.0 // Very selective for PK
				}
			}
		case catalog.UniqueConstraint:
			for _, col := range c.Columns {
				if col == columnName {
					return 1.0 / 1000.0 // Very selective for unique
				}
			}
		}
	}

	// Check if there's a unique index on this column
	for _, index := range table.Indexes {
		if index.IsUnique && len(index.Columns) == 1 && index.Columns[0].Column.Name == columnName {
			return 1.0 / 1000.0 // Very selective for unique index
		}
	}

	// Default equality selectivity
	return 0.05
}

// estimateRangeSelectivity estimates selectivity for range predicates.
func (ce *CostEstimator) estimateRangeSelectivity(table *catalog.Table, columnName string, operator BinaryOperator) float64 {
	// Try to get column statistics with histogram
	if colStats := ce.getColumnStats(table, columnName); colStats != nil && colStats.Histogram != nil {
		// TODO: Implement histogram-based selectivity estimation
		// Will use bucket boundaries and frequencies for accurate range estimates
		_ = colStats // Placeholder to avoid empty branch warning
	}

	// Default range selectivity estimates
	switch operator {
	case OpLess, OpLessEqual:
		return 0.3 // 30% of rows are typically less than a random value
	case OpGreater, OpGreaterEqual:
		return 0.3 // 30% of rows are typically greater than a random value
	case OpAdd, OpSubtract, OpMultiply, OpDivide, OpModulo, OpEqual, OpNotEqual,
		OpAnd, OpOr, OpConcat, OpLike, OpNotLike, OpIn, OpNotIn, OpIs, OpIsNot:
		// Not range operators
		return 0.3
	default:
		return 0.3
	}
}

// getColumnStats retrieves column statistics if available.
func (ce *CostEstimator) getColumnStats(table *catalog.Table, columnName string) *catalog.ColumnStats {
	// Find the column in the table
	for _, col := range table.Columns {
		if col.Name == columnName && col.Stats != nil {
			return col.Stats
		}
	}
	return nil
}

// ShouldUseIndex determines whether an index scan is more cost-effective than a table scan.
func (ce *CostEstimator) ShouldUseIndex(table *catalog.Table, index *catalog.Index, filter Expression) bool {
	// Estimate selectivity of the filter
	selectivity := ce.EstimateSelectivity(table, filter)

	// Calculate costs for both access methods
	tableScanCost := ce.EstimateTableScanCost(table, selectivity)
	indexScanCost := ce.EstimateIndexScanCost(table, index, selectivity)

	// Compare total costs
	return indexScanCost.TotalCost < tableScanCost.TotalCost
}
