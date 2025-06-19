package executor

import (
	"fmt"
	"sort"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// AnalyzeOperator executes ANALYZE statements to collect table and column statistics.
type AnalyzeOperator struct {
	baseOperator
	plan        *planner.LogicalAnalyze
	catalog     catalog.Catalog
	storage     StorageBackend
	statsWriter catalog.StatsWriter
	done        bool
}

// NewAnalyzeOperator creates a new ANALYZE operator.
func NewAnalyzeOperator(plan *planner.LogicalAnalyze, cat catalog.Catalog, store StorageBackend) *AnalyzeOperator {
	// Try to get StatsWriter interface from catalog
	var statsWriter catalog.StatsWriter
	if sw, ok := cat.(catalog.StatsWriter); ok {
		statsWriter = sw
	}

	// Convert planner schema to executor schema
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "result",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &AnalyzeOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		plan:        plan,
		catalog:     cat,
		storage:     store,
		statsWriter: statsWriter,
		done:        false,
	}
}

// Open initializes the operator.
func (op *AnalyzeOperator) Open(ctx *ExecContext) error {
	op.ctx = ctx
	if op.statsWriter == nil {
		return fmt.Errorf("catalog does not support statistics updates")
	}
	return nil
}

// Next executes the ANALYZE operation and returns the result.
func (op *AnalyzeOperator) Next() (*Row, error) {
	// Only return one row
	if op.done {
		return nil, nil //nolint:nilnil // EOF - standard iterator pattern
	}
	op.done = true

	// Get table from catalog
	table, err := op.catalog.GetTable(op.plan.SchemaName, op.plan.TableName)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	// Collect table statistics
	tableStats, err := op.collectTableStats(table)
	if err != nil {
		return nil, fmt.Errorf("failed to collect table statistics: %w", err)
	}

	// Update table statistics in catalog
	if err := op.statsWriter.UpdateTableStatistics(table.ID, tableStats); err != nil {
		return nil, fmt.Errorf("failed to update table statistics: %w", err)
	}

	// Determine which columns to analyze
	columnsToAnalyze := op.plan.Columns
	if len(columnsToAnalyze) == 0 {
		// Analyze all columns
		columnsToAnalyze = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			columnsToAnalyze[i] = col.Name
		}
	}

	// Collect column statistics
	for _, colName := range columnsToAnalyze {
		// Find column in table
		var column *catalog.Column
		for _, col := range table.Columns {
			if col.Name == colName {
				column = col
				break
			}
		}
		if column == nil {
			continue // Skip if column not found
		}

		colStats, err := op.collectColumnStats(table, column, tableStats.RowCount)
		if err != nil {
			return nil, fmt.Errorf("failed to collect statistics for column %s: %w", colName, err)
		}

		// Update column statistics in catalog
		if err := op.statsWriter.UpdateColumnStatistics(table.ID, column.ID, colStats); err != nil {
			return nil, fmt.Errorf("failed to update statistics for column %s: %w", colName, err)
		}
	}

	// Return success message
	msg := fmt.Sprintf("ANALYZE completed for table %s.%s", op.plan.SchemaName, op.plan.TableName)
	return &Row{
		Values: []types.Value{types.NewValue(msg)},
	}, nil
}

// collectTableStats collects table-level statistics.
func (op *AnalyzeOperator) collectTableStats(table *catalog.Table) (*catalog.TableStats, error) {
	stats := &catalog.TableStats{
		LastAnalyzed: time.Now(),
	}

	// Create a scan operator to count rows
	scanOp := NewStorageScanOperator(table, op.storage)

	if err := scanOp.Open(op.ctx); err != nil {
		return nil, err
	}
	defer scanOp.Close()

	// Count rows and calculate average row size
	var totalSize int64
	for {
		row, err := scanOp.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			break
		}

		stats.RowCount++

		// Estimate row size
		rowSize := 0
		for _, val := range row.Values {
			rowSize += estimateValueSize(val)
		}
		totalSize += int64(rowSize)
	}

	if stats.RowCount > 0 {
		stats.AvgRowSize = int(totalSize / stats.RowCount)
	}

	// TODO: Get page count from storage engine when available
	// For now, estimate based on row count and average size
	const pageSize = 8192 // 8KB pages
	if stats.RowCount > 0 && stats.AvgRowSize > 0 {
		totalTableSize := stats.RowCount * int64(stats.AvgRowSize)
		stats.PageCount = (totalTableSize + pageSize - 1) / pageSize
	}

	return stats, nil
}

// collectColumnStats collects column-level statistics.
func (op *AnalyzeOperator) collectColumnStats(table *catalog.Table, column *catalog.Column, rowCount int64) (*catalog.ColumnStats, error) {
	stats := &catalog.ColumnStats{
		LastAnalyzed: time.Now(),
	}

	// Find column index
	colIndex := -1
	for i, col := range table.Columns {
		if col.ID == column.ID {
			colIndex = i
			break
		}
	}
	if colIndex < 0 {
		return nil, fmt.Errorf("column not found in table")
	}

	// Create scan operator
	scanOp := NewStorageScanOperator(table, op.storage)

	if err := scanOp.Open(op.ctx); err != nil {
		return nil, err
	}
	defer scanOp.Close()

	// Collect distinct values and null count
	distinctValues := make(map[string]struct{})
	var totalWidth int64
	var values []types.Value // For histogram generation

	// Use sampling for large tables
	sampleSize := int64(30000) // Default sample size
	sampleInterval := int64(1)
	if rowCount > sampleSize {
		sampleInterval = rowCount / sampleSize
	}

	var rowNum int64
	for {
		row, err := scanOp.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			break
		}

		rowNum++

		// Sample rows for large tables
		if sampleInterval > 1 && rowNum%sampleInterval != 0 {
			continue
		}

		if colIndex >= len(row.Values) {
			continue
		}

		val := row.Values[colIndex]

		// Count nulls
		if val.IsNull() {
			stats.NullCount++
			continue
		}

		// Track distinct values (up to a limit)
		if len(distinctValues) < 10000 { // Limit distinct tracking
			distinctValues[val.String()] = struct{}{}
		}

		// Calculate width
		totalWidth += int64(estimateValueSize(val))

		// Collect values for histogram
		values = append(values, val)

		// Update min/max
		if stats.MinValue.IsNull() || compareValuesForStats(val, stats.MinValue) < 0 {
			stats.MinValue = val
		}
		if stats.MaxValue.IsNull() || compareValuesForStats(val, stats.MaxValue) > 0 {
			stats.MaxValue = val
		}
	}

	// Adjust statistics for sampling
	if sampleInterval > 1 {
		stats.NullCount = stats.NullCount * sampleInterval
		// Estimate distinct count using simple formula
		// For better accuracy, could use HyperLogLog
		sampleDistinct := int64(len(distinctValues))
		stats.DistinctCount = estimateDistinctCount(sampleDistinct, rowCount, sampleSize)
	} else {
		stats.DistinctCount = int64(len(distinctValues))
	}

	// Calculate average width
	nonNullCount := int64(len(values))
	if nonNullCount > 0 {
		stats.AvgWidth = int(totalWidth / nonNullCount)
	}

	// Build histogram if we have values
	if len(values) > 0 {
		stats.Histogram = buildHistogram(values, 100) // 100 buckets
	}

	return stats, nil
}

// estimateValueSize estimates the size of a value in bytes.
func estimateValueSize(val types.Value) int {
	if val.IsNull() {
		return 1 // Null bitmap
	}

	switch v := val.Data.(type) {
	case bool:
		return 1
	case int64:
		return 8
	case float64:
		return 8
	case string:
		return len(v) + 4 // String length + overhead
	case time.Time:
		return 8
	default:
		return 8 // Default estimate
	}
}

// compareValuesForStats compares two values for ordering in statistics collection.
func compareValuesForStats(a, b types.Value) int {
	// Handle nulls
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

	// Compare based on type
	switch va := a.Data.(type) {
	case int64:
		if vb, ok := b.Data.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.Data.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.Data.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case time.Time:
		if vb, ok := b.Data.(time.Time); ok {
			if va.Before(vb) {
				return -1
			} else if va.After(vb) {
				return 1
			}
			return 0
		}
	}

	// Default: convert to string and compare
	sa := a.String()
	sb := b.String()
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

// estimateDistinctCount estimates the total distinct count based on sample.
func estimateDistinctCount(sampleDistinct, totalRows, sampleRows int64) int64 {
	if sampleRows >= totalRows {
		return sampleDistinct
	}

	// Simple estimation formula
	// For better accuracy, use statistical methods like Good-Turing or HyperLogLog
	ratio := float64(totalRows) / float64(sampleRows)
	estimated := float64(sampleDistinct) * ratio

	// Cap at total rows
	if estimated > float64(totalRows) {
		return totalRows
	}

	return int64(estimated)
}

// buildHistogram builds an equi-height histogram from values.
func buildHistogram(values []types.Value, numBuckets int) *catalog.Histogram {
	if len(values) == 0 || numBuckets <= 0 {
		return nil
	}

	// Sort values
	sort.Slice(values, func(i, j int) bool {
		return compareValuesForStats(values[i], values[j]) < 0
	})

	// Calculate bucket size
	bucketSize := len(values) / numBuckets
	if bucketSize == 0 {
		bucketSize = 1
		numBuckets = len(values)
	}

	hist := &catalog.Histogram{
		Type:    catalog.EquiHeightHistogram,
		Buckets: make([]catalog.HistogramBucket, 0, numBuckets),
	}

	for i := 0; i < numBuckets && i*bucketSize < len(values); i++ {
		start := i * bucketSize
		end := start + bucketSize
		if i == numBuckets-1 || end > len(values) {
			end = len(values)
		}

		// Count distinct values in bucket
		distinctInBucket := make(map[string]struct{})
		for j := start; j < end; j++ {
			distinctInBucket[values[j].String()] = struct{}{}
		}

		bucket := catalog.HistogramBucket{
			LowerBound:    values[start],
			UpperBound:    values[end-1],
			Frequency:     int64(end - start),
			DistinctCount: int64(len(distinctInBucket)),
		}
		hist.Buckets = append(hist.Buckets, bucket)
	}

	return hist
}

// Close cleans up the operator.
func (op *AnalyzeOperator) Close() error {
	return nil
}

// String returns a string representation.
func (op *AnalyzeOperator) String() string {
	return fmt.Sprintf("AnalyzeOperator(%s.%s)", op.plan.SchemaName, op.plan.TableName)
}

// Schema returns the output schema.
func (op *AnalyzeOperator) Schema() *Schema {
	return op.schema
}
