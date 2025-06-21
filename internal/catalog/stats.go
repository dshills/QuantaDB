package catalog

import (
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// TableStats holds table-level statistics.
type TableStats struct {
	RowCount     int64
	PageCount    int64
	AvgRowSize   int
	LastAnalyzed time.Time
}

// ColumnStats holds column-level statistics.
type ColumnStats struct {
	NullCount     int64
	DistinctCount int64
	AvgWidth      int
	MinValue      types.Value
	MaxValue      types.Value
	Histogram     *Histogram
	LastAnalyzed  time.Time
	TableRowCount int64 // Total rows in the table (for selectivity calculation)
}

// Histogram represents the distribution of values in a column.
type Histogram struct {
	Type    HistogramType
	Buckets []HistogramBucket
}

// HistogramType represents the type of histogram.
type HistogramType int

const (
	// EquiHeightHistogram has buckets with equal number of rows.
	EquiHeightHistogram HistogramType = iota
	// EquiWidthHistogram has buckets with equal value ranges.
	EquiWidthHistogram
)

// HistogramBucket represents a single bucket in a histogram.
type HistogramBucket struct {
	LowerBound    types.Value
	UpperBound    types.Value
	Frequency     int64
	DistinctCount int64
}

// StatsCollector analyzes tables and updates statistics.
type StatsCollector interface {
	// AnalyzeTable collects statistics for an entire table.
	AnalyzeTable(table *Table) (*TableStats, error)
	// AnalyzeColumn collects statistics for a specific column.
	AnalyzeColumn(table *Table, column *Column) (*ColumnStats, error)
	// BuildHistogram builds a histogram for a column.
	BuildHistogram(table *Table, column *Column, buckets int) (*Histogram, error)
}

// Selectivity estimates the fraction of rows that match a predicate.
type Selectivity float64

// EstimateSelectivity estimates the selectivity of a predicate using histogram-based estimation.
func EstimateSelectivity(stats *ColumnStats, op ComparisonOp, value types.Value) Selectivity {
	if stats == nil || stats.RowCount() == 0 {
		// Default selectivity when no statistics available
		return defaultSelectivity(op)
	}

	switch op {
	case OpEqual:
		return estimateEqualitySelectivity(stats, value)

	case OpLess:
		return estimateRangeSelectivity(stats, value, true, false)

	case OpLessEqual:
		return estimateRangeSelectivity(stats, value, true, true)

	case OpGreater:
		return estimateRangeSelectivity(stats, value, false, false)

	case OpGreaterEqual:
		return estimateRangeSelectivity(stats, value, false, true)

	case OpIsNull:
		if stats.NullCount > 0 {
			return Selectivity(float64(stats.NullCount) / float64(stats.RowCount()))
		}
		return 0.01 // Low default for IS NULL

	case OpIsNotNull:
		if stats.NullCount > 0 {
			return Selectivity(1.0 - float64(stats.NullCount)/float64(stats.RowCount()))
		}
		return 0.99 // High default for IS NOT NULL

	case OpNotEqual:
		equalSel := estimateEqualitySelectivity(stats, value)
		return Selectivity(1.0 - float64(equalSel))

	default:
		return 0.5 // Conservative default
	}
}

// estimateEqualitySelectivity estimates selectivity for equality predicates.
func estimateEqualitySelectivity(stats *ColumnStats, _ types.Value) Selectivity {
	if stats.DistinctCount > 0 {
		// Use uniform distribution assumption
		return Selectivity(1.0 / float64(stats.DistinctCount))
	}
	return 0.1 // Default for equality
}

// estimateRangeSelectivity estimates selectivity for range predicates using histograms.
func estimateRangeSelectivity(stats *ColumnStats, value types.Value, isLower bool, inclusive bool) Selectivity {
	// If no histogram, fall back to simple range estimation
	if stats.Histogram == nil || len(stats.Histogram.Buckets) == 0 {
		return estimateRangeSelectivitySimple(stats, value, isLower)
	}

	totalRows := stats.RowCount()
	if totalRows == 0 {
		return 0.3 // Default
	}

	var selectedRows int64 = 0

	for _, bucket := range stats.Histogram.Buckets {
		bucketSelectivity := calculateBucketSelectivity(bucket, value, isLower, inclusive)
		selectedRows += int64(float64(bucket.Frequency) * float64(bucketSelectivity))
	}

	selectivity := Selectivity(float64(selectedRows) / float64(totalRows))

	// Ensure selectivity is within bounds
	if selectivity < 0.0 {
		return 0.0
	}
	if selectivity > 1.0 {
		return 1.0
	}

	return selectivity
}

// calculateBucketSelectivity calculates what fraction of a bucket satisfies the predicate.
func calculateBucketSelectivity(bucket HistogramBucket, value types.Value, isLower bool, inclusive bool) Selectivity {
	lowerBound := bucket.LowerBound
	upperBound := bucket.UpperBound

	// Compare value with bucket bounds
	cmpLower := compareValues(value, lowerBound)
	cmpUpper := compareValues(value, upperBound)

	if isLower {
		// For < or <= predicates, we want values less than 'value'
		if cmpLower <= 0 {
			// value <= lowerBound, so no rows in this bucket satisfy predicate
			return 0.0
		}
		if cmpUpper >= 0 {
			// value >= upperBound, so all rows in this bucket satisfy predicate
			return 1.0
		}
		// value is within bucket range, estimate fraction
		return estimateFractionInRange(lowerBound, upperBound, value, true, inclusive)
	} else {
		// For > or >= predicates, we want values greater than 'value'
		if cmpUpper <= 0 {
			// value >= upperBound, so no rows in this bucket satisfy predicate
			return 0.0
		}
		if cmpLower >= 0 {
			// value <= lowerBound, so all rows in this bucket satisfy predicate
			return 1.0
		}
		// value is within bucket range, estimate fraction
		return estimateFractionInRange(lowerBound, upperBound, value, false, inclusive)
	}
}

// estimateFractionInRange estimates what fraction of values in [lower, upper] satisfy the predicate.
func estimateFractionInRange(lower, upper, value types.Value, isLower bool, inclusive bool) Selectivity {
	// For simplicity, assume uniform distribution within bucket
	// More sophisticated implementations could use interpolation

	totalRange := calculateRange(lower, upper)
	if totalRange <= 0 {
		return 0.5 // Can't determine, use conservative estimate
	}

	if isLower {
		// Calculate fraction less than value
		valueRange := calculateRange(lower, value)
		fraction := float64(valueRange) / float64(totalRange)
		if !inclusive && fraction > 0 {
			// Adjust for non-inclusive comparison
			fraction -= 1.0 / float64(totalRange)
		}
		return Selectivity(fraction)
	} else {
		// Calculate fraction greater than value
		valueRange := calculateRange(value, upper)
		fraction := float64(valueRange) / float64(totalRange)
		if !inclusive && fraction > 0 {
			// Adjust for non-inclusive comparison
			fraction -= 1.0 / float64(totalRange)
		}
		return Selectivity(fraction)
	}
}

// calculateRange calculates the "distance" between two values for range estimation.
func calculateRange(lower, upper types.Value) int64 {
	if lower.IsNull() || upper.IsNull() {
		return 0
	}

	switch lv := lower.Data.(type) {
	case int64:
		if uv, ok := upper.Data.(int64); ok {
			if uv > lv {
				return uv - lv
			}
		}
	case float64:
		if uv, ok := upper.Data.(float64); ok {
			if uv > lv {
				return int64((uv - lv) * 1000) // Scale for precision
			}
		}
	case string:
		// For strings, use lexicographic ordering approximation
		if uv, ok := upper.Data.(string); ok {
			if len(lv) > 0 && len(uv) > 0 {
				return int64(uv[0] - lv[0]) // Simple first-character difference
			}
		}
	}

	return 1 // Default minimal range
}

// compareValues compares two values and returns -1, 0, or 1.
func compareValues(a, b types.Value) int {
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

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
	}

	// Default string comparison
	sa := a.String()
	sb := b.String()
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

// estimateRangeSelectivitySimple provides fallback range estimation without histogram.
func estimateRangeSelectivitySimple(stats *ColumnStats, value types.Value, isLower bool) Selectivity {
	if stats.MinValue.IsNull() || stats.MaxValue.IsNull() {
		return 0.3 // Default for range
	}

	// Simple linear interpolation between min and max
	totalRange := calculateRange(stats.MinValue, stats.MaxValue)
	if totalRange <= 0 {
		return 0.3
	}

	if isLower {
		valueRange := calculateRange(stats.MinValue, value)
		fraction := float64(valueRange) / float64(totalRange)
		if fraction < 0 {
			return 0.0
		}
		if fraction > 1 {
			return 1.0
		}
		return Selectivity(fraction)
	} else {
		valueRange := calculateRange(value, stats.MaxValue)
		fraction := float64(valueRange) / float64(totalRange)
		if fraction < 0 {
			return 0.0
		}
		if fraction > 1 {
			return 1.0
		}
		return Selectivity(fraction)
	}
}

// ComparisonOp represents a comparison operator for selectivity estimation.
type ComparisonOp int

const (
	OpEqual ComparisonOp = iota
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
	OpIsNull
	OpIsNotNull
)

// defaultSelectivity returns default selectivity for operators.
func defaultSelectivity(op ComparisonOp) Selectivity {
	switch op {
	case OpEqual:
		return 0.1
	case OpNotEqual:
		return 0.9
	case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		return 0.3
	case OpIsNull:
		return 0.05
	case OpIsNotNull:
		return 0.95
	default:
		return 0.5
	}
}

// RowCount returns the total row count from table stats.
func (c *ColumnStats) RowCount() int64 {
	return c.TableRowCount
}

// CombineSelectivity combines multiple selectivities.
func CombineSelectivity(op LogicalOp, selectivities ...Selectivity) Selectivity {
	if len(selectivities) == 0 {
		return 1.0
	}

	result := selectivities[0]
	for i := 1; i < len(selectivities); i++ {
		switch op {
		case LogicalAnd:
			// For AND, multiply selectivities (assuming independence)
			result = result * selectivities[i]
		case LogicalOr:
			// For OR, use inclusion-exclusion principle
			result = result + selectivities[i] - (result * selectivities[i])
		}
	}

	return result
}

// LogicalOp represents a logical operator for combining selectivities.
type LogicalOp int

const (
	LogicalAnd LogicalOp = iota
	LogicalOr
)
