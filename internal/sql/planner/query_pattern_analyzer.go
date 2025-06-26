package planner

import (
	"crypto/md5"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// QueryPatternAnalyzer tracks and analyzes SQL query execution patterns.
type QueryPatternAnalyzer struct {
	patterns map[string]*QueryPattern // Key is query fingerprint
}

// NewQueryPatternAnalyzer creates a new query pattern analyzer.
func NewQueryPatternAnalyzer() *QueryPatternAnalyzer {
	return &QueryPatternAnalyzer{
		patterns: make(map[string]*QueryPattern),
	}
}

// QueryPattern represents a normalized query pattern with execution statistics.
type QueryPattern struct {
	// Query identification
	Fingerprint   string    // Normalized query hash
	QueryType     QueryType // SELECT, INSERT, UPDATE, DELETE
	NormalizedSQL string    // SQL with parameters normalized

	// Table and column access
	PrimaryTable    string   // Main table being queried
	JoinedTables    []string // Additional tables in joins
	AccessedColumns []string // All columns referenced
	FilterColumns   []string // Columns used in WHERE clauses
	OrderByColumns  []string // Columns used in ORDER BY
	GroupByColumns  []string // Columns used in GROUP BY
	ProjectColumns  []string // Columns in SELECT list

	// Predicate analysis
	EqualityFilters map[string][]types.Value // column -> values for = predicates
	RangeFilters    map[string]RangeFilter   // column -> range conditions
	LikeFilters     []string                 // columns with LIKE predicates
	InFilters       map[string]int           // column -> number of values in IN clauses

	// Execution statistics
	ExecutionCount  int           // Number of times this pattern was executed
	TotalDuration   time.Duration // Total execution time
	AverageDuration time.Duration // Average execution time
	MinDuration     time.Duration // Fastest execution
	MaxDuration     time.Duration // Slowest execution

	// Access patterns
	RowsExamined int64   // Average rows examined
	RowsReturned int64   // Average rows returned
	Selectivity  float64 // RowsReturned / RowsExamined

	// Temporal data
	FirstSeen time.Time // When this pattern was first observed
	LastSeen  time.Time // Most recent execution

	// Cost analysis
	EstimatedCost float64 // Current estimated execution cost
	ActualCost    float64 // Measured execution cost
}

// QueryType represents the type of SQL query.
type QueryType int

const (
	QueryTypeSelect QueryType = iota
	QueryTypeInsert
	QueryTypeUpdate
	QueryTypeDelete
	QueryTypeUnknown
)

func (qt QueryType) String() string {
	switch qt {
	case QueryTypeSelect:
		return "SELECT"
	case QueryTypeInsert:
		return "INSERT"
	case QueryTypeUpdate:
		return "UPDATE"
	case QueryTypeDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// RangeFilter represents a range condition on a column.
type RangeFilter struct {
	HasLowerBound bool
	HasUpperBound bool
	LowerBound    types.Value
	UpperBound    types.Value
	Operator      string // <, <=, >, >=, BETWEEN
}

// RecordQuery analyzes and records a query execution for pattern analysis.
func (analyzer *QueryPatternAnalyzer) RecordQuery(
	sql string,
	executionTime time.Duration,
	rowsExamined int64,
	rowsReturned int64,
	plan LogicalPlan,
) error {
	// Normalize the query to create a fingerprint
	normalized := analyzer.normalizeQuery(sql)
	fingerprint := analyzer.createFingerprint(normalized)

	// Get or create pattern
	pattern, exists := analyzer.patterns[fingerprint]
	if !exists {
		pattern = &QueryPattern{
			Fingerprint:     fingerprint,
			NormalizedSQL:   normalized,
			EqualityFilters: make(map[string][]types.Value),
			RangeFilters:    make(map[string]RangeFilter),
			InFilters:       make(map[string]int),
			FirstSeen:       time.Now(),
			QueryType:       analyzer.detectQueryType(sql),
		}
		analyzer.patterns[fingerprint] = pattern

		// Analyze the plan to extract access patterns
		analyzer.analyzePlan(plan, pattern)
	}

	// Update execution statistics
	pattern.ExecutionCount++
	pattern.TotalDuration += executionTime
	pattern.AverageDuration = pattern.TotalDuration / time.Duration(pattern.ExecutionCount)
	pattern.LastSeen = time.Now()

	if pattern.ExecutionCount == 1 || executionTime < pattern.MinDuration {
		pattern.MinDuration = executionTime
	}
	if pattern.ExecutionCount == 1 || executionTime > pattern.MaxDuration {
		pattern.MaxDuration = executionTime
	}

	// Update access statistics
	pattern.RowsExamined = rowsExamined
	pattern.RowsReturned = rowsReturned
	if rowsExamined > 0 {
		pattern.Selectivity = float64(rowsReturned) / float64(rowsExamined)
	}

	return nil
}

// GetPatterns returns all recorded query patterns.
func (analyzer *QueryPatternAnalyzer) GetPatterns() []QueryPattern {
	patterns := make([]QueryPattern, 0, len(analyzer.patterns))
	for _, pattern := range analyzer.patterns {
		patterns = append(patterns, *pattern)
	}

	// Sort by execution count (most frequent first)
	sort.Slice(patterns, func(i, j int) bool {
		return patterns[i].ExecutionCount > patterns[j].ExecutionCount
	})

	return patterns
}

// GetFrequentPatterns returns patterns executed more than the specified threshold.
func (analyzer *QueryPatternAnalyzer) GetFrequentPatterns(minExecutions int) []QueryPattern {
	patterns := analyzer.GetPatterns()
	filtered := make([]QueryPattern, 0)

	for _, pattern := range patterns {
		if pattern.ExecutionCount >= minExecutions {
			filtered = append(filtered, pattern)
		}
	}

	return filtered
}

// GetSlowPatterns returns patterns with average execution time above the threshold.
func (analyzer *QueryPatternAnalyzer) GetSlowPatterns(threshold time.Duration) []QueryPattern {
	patterns := analyzer.GetPatterns()
	filtered := make([]QueryPattern, 0)

	for _, pattern := range patterns {
		if pattern.AverageDuration > threshold {
			filtered = append(filtered, pattern)
		}
	}

	// Sort by average duration (slowest first)
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].AverageDuration > filtered[j].AverageDuration
	})

	return filtered
}

// normalizeQuery removes literals and standardizes the query structure.
func (analyzer *QueryPatternAnalyzer) normalizeQuery(sql string) string {
	// Simple normalization - in practice, this would be more sophisticated
	normalized := strings.ToUpper(sql)

	// Replace common patterns with placeholders
	// Numbers
	normalized = replacePattern(normalized, `\b\d+\b`, "?")
	// Quoted strings
	normalized = replacePattern(normalized, `'[^']*'`, "?")
	// Multiple whitespace with single space
	normalized = replacePattern(normalized, `\s+`, " ")

	return strings.TrimSpace(normalized)
}

// createFingerprint creates a hash fingerprint for the normalized query.
func (analyzer *QueryPatternAnalyzer) createFingerprint(normalized string) string {
	hash := md5.Sum([]byte(normalized))
	return fmt.Sprintf("%x", hash)
}

// detectQueryType determines the type of SQL query.
func (analyzer *QueryPatternAnalyzer) detectQueryType(sql string) QueryType {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	if strings.HasPrefix(upper, "SELECT") {
		return QueryTypeSelect
	} else if strings.HasPrefix(upper, "INSERT") {
		return QueryTypeInsert
	} else if strings.HasPrefix(upper, "UPDATE") {
		return QueryTypeUpdate
	} else if strings.HasPrefix(upper, "DELETE") {
		return QueryTypeDelete
	}

	return QueryTypeUnknown
}

// analyzePlan extracts table and column access patterns from the logical plan.
func (analyzer *QueryPatternAnalyzer) analyzePlan(plan LogicalPlan, pattern *QueryPattern) {
	if plan == nil {
		return
	}

	switch p := plan.(type) {
	case *LogicalScan:
		if pattern.PrimaryTable == "" {
			pattern.PrimaryTable = p.TableName
		}

	case *LogicalFilter:
		// Extract filter columns and conditions
		analyzer.analyzeFilterExpression(p.Predicate, pattern)

	case *LogicalProject:
		// Extract projected columns
		for _, proj := range p.Projections {
			analyzer.extractColumnsFromExpression(proj, &pattern.ProjectColumns)
		}

	case *LogicalSort:
		// Extract ORDER BY columns
		for _, orderBy := range p.OrderBy {
			analyzer.extractColumnsFromExpression(orderBy.Expr, &pattern.OrderByColumns)
		}

	case *LogicalJoin:
		// Extract join condition columns
		if p.Condition != nil {
			analyzer.extractColumnsFromExpression(p.Condition, &pattern.AccessedColumns)
		}

		// Track joined tables from children
		for _, child := range p.Children() {
			if scan, ok := child.(*LogicalScan); ok {
				if scan.TableName != pattern.PrimaryTable {
					pattern.JoinedTables = appendUnique(pattern.JoinedTables, scan.TableName)
				}
			}
		}
	}

	// Recursively analyze children
	for _, child := range plan.Children() {
		if logicalChild, ok := child.(LogicalPlan); ok {
			analyzer.analyzePlan(logicalChild, pattern)
		}
	}
}

// analyzeFilterExpression extracts filter conditions from a predicate expression.
func (analyzer *QueryPatternAnalyzer) analyzeFilterExpression(expr Expression, pattern *QueryPattern) {
	switch e := expr.(type) {
	case *BinaryOp:
		switch e.Operator {
		case OpEqual:
			// Extract equality filters
			if col, ok := e.Left.(*ColumnRef); ok {
				if lit, ok := e.Right.(*Literal); ok {
					pattern.FilterColumns = appendUnique(pattern.FilterColumns, col.ColumnName)
					pattern.EqualityFilters[col.ColumnName] = append(
						pattern.EqualityFilters[col.ColumnName], lit.Value)
				}
			}

		case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
			// Extract range filters
			if col, ok := e.Left.(*ColumnRef); ok {
				if lit, ok := e.Right.(*Literal); ok {
					pattern.FilterColumns = appendUnique(pattern.FilterColumns, col.ColumnName)

					rangeFilter := pattern.RangeFilters[col.ColumnName]
					rangeFilter.Operator = e.Operator.String()

					if e.Operator == OpLess || e.Operator == OpLessEqual {
						rangeFilter.HasUpperBound = true
						rangeFilter.UpperBound = lit.Value
					} else {
						rangeFilter.HasLowerBound = true
						rangeFilter.LowerBound = lit.Value
					}

					pattern.RangeFilters[col.ColumnName] = rangeFilter
				}
			}

		case OpLike:
			// Extract LIKE filters
			if col, ok := e.Left.(*ColumnRef); ok {
				pattern.LikeFilters = appendUnique(pattern.LikeFilters, col.ColumnName)
				pattern.FilterColumns = appendUnique(pattern.FilterColumns, col.ColumnName)
			}

		case OpAnd, OpOr:
			// Recursively analyze compound conditions
			analyzer.analyzeFilterExpression(e.Left, pattern)
			analyzer.analyzeFilterExpression(e.Right, pattern)
		}

	case *FunctionCall:
		// Handle IN clauses and other functions
		if strings.ToUpper(e.Name) == "IN" && len(e.Args) > 0 {
			if col, ok := e.Args[0].(*ColumnRef); ok {
				pattern.InFilters[col.ColumnName] = len(e.Args) - 1
				pattern.FilterColumns = appendUnique(pattern.FilterColumns, col.ColumnName)
			}
		}
	}
}

// extractColumnsFromExpression extracts column references from any expression.
func (analyzer *QueryPatternAnalyzer) extractColumnsFromExpression(expr Expression, columns *[]string) {
	switch e := expr.(type) {
	case *ColumnRef:
		*columns = appendUnique(*columns, e.ColumnName)

	case *BinaryOp:
		analyzer.extractColumnsFromExpression(e.Left, columns)
		analyzer.extractColumnsFromExpression(e.Right, columns)

	case *FunctionCall:
		for _, arg := range e.Args {
			analyzer.extractColumnsFromExpression(arg, columns)
		}
	}
}

// Helper functions

// replacePattern is a simplified regex replacement (would use regexp in practice).
func replacePattern(text, pattern, replacement string) string {
	// Simplified implementation - would use regexp.ReplaceAllString in practice
	return text
}

// appendUnique adds a string to a slice if it's not already present.
func appendUnique(slice []string, item string) []string {
	for _, existing := range slice {
		if existing == item {
			return slice
		}
	}
	return append(slice, item)
}
