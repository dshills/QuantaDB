package planner

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// analyzeTableQueries analyzes query patterns for a specific table and generates recommendations.
func (advisor *IndexAdvisor) analyzeTableQueries(tableName string, patterns []QueryPattern) ([]IndexRecommendation, error) {
	// Get table metadata
	table, err := advisor.catalog.GetTable("public", tableName)
	if err != nil {
		// Try other schemas
		table, err = advisor.catalog.GetTable("", tableName)
		if err != nil {
			return nil, fmt.Errorf("table %s not found: %w", tableName, err)
		}
	}
	
	recommendations := make([]IndexRecommendation, 0)
	
	// Analyze patterns for different types of recommendations
	compositeRecs := advisor.analyzeCompositeIndexOpportunities(table, patterns)
	coveringRecs := advisor.analyzeCoveringIndexOpportunities(table, patterns)
	partialRecs := advisor.analyzePartialIndexOpportunities(table, patterns)
	singleRecs := advisor.analyzeSingleColumnOpportunities(table, patterns)
	
	recommendations = append(recommendations, compositeRecs...)
	recommendations = append(recommendations, coveringRecs...)
	recommendations = append(recommendations, partialRecs...)
	recommendations = append(recommendations, singleRecs...)
	
	// Remove duplicates and conflicts
	recommendations = advisor.deduplicateRecommendations(recommendations)
	
	// Calculate priorities and final metrics
	for i := range recommendations {
		advisor.calculateRecommendationPriority(&recommendations[i])
		advisor.estimateImplementationCost(&recommendations[i], table)
	}
	
	return recommendations, nil
}

// analyzeCompositeIndexOpportunities identifies opportunities for multi-column indexes.
func (advisor *IndexAdvisor) analyzeCompositeIndexOpportunities(table *catalog.Table, patterns []QueryPattern) []IndexRecommendation {
	recommendations := make([]IndexRecommendation, 0)
	
	// Group patterns by their filter column combinations
	columnCombinations := make(map[string][]QueryPattern)
	
	for _, pattern := range patterns {
		if len(pattern.FilterColumns) >= 2 {
			// Create a sorted key for the column combination
			columns := make([]string, len(pattern.FilterColumns))
			copy(columns, pattern.FilterColumns)
			sort.Strings(columns)
			key := strings.Join(columns, ",")
			
			columnCombinations[key] = append(columnCombinations[key], pattern)
		}
	}
	
	// Analyze each combination
	for columnKey, relatedPatterns := range columnCombinations {
		columns := strings.Split(columnKey, ",")
		
		// Calculate benefit for this combination
		totalQueryCount := 0
		totalCurrentCost := 0.0
		combinedPatterns := make([]QueryPattern, 0)
		
		for _, pattern := range relatedPatterns {
			totalQueryCount += pattern.ExecutionCount
			totalCurrentCost += pattern.ActualCost * float64(pattern.ExecutionCount)
			combinedPatterns = append(combinedPatterns, pattern)
		}
		
		if totalQueryCount < advisor.config.MinQueryCount {
			continue
		}
		
		// Estimate cost with composite index
		projectedCost := advisor.estimateCompositeIndexCost(table, columns, relatedPatterns)
		costReduction := totalCurrentCost - projectedCost
		
		if costReduction < advisor.config.MinCostReduction {
			continue
		}
		
		// Order columns optimally (equality filters first, then range filters)
		optimizedColumns := advisor.optimizeColumnOrder(columns, relatedPatterns)
		
		rec := IndexRecommendation{
			SchemaName:       table.SchemaName,
			TableName:        table.TableName,
			IndexName:        fmt.Sprintf("idx_%s_%s", table.TableName, strings.Join(optimizedColumns, "_")),
			IndexType:        IndexTypeComposite,
			Columns:          optimizedColumns,
			EstimatedBenefit: totalCurrentCost / projectedCost,
			CostReduction:    costReduction,
			AffectedQueries:  combinedPatterns,
			CurrentCost:      totalCurrentCost,
			ProjectedCost:    projectedCost,
			QueryCount:       totalQueryCount,
			CreatedAt:        time.Now(),
		}
		
		recommendations = append(recommendations, rec)
	}
	
	return recommendations
}

// analyzeCoveringIndexOpportunities identifies opportunities for covering indexes.
func (advisor *IndexAdvisor) analyzeCoveringIndexOpportunities(table *catalog.Table, patterns []QueryPattern) []IndexRecommendation {
	recommendations := make([]IndexRecommendation, 0)
	
	for _, pattern := range patterns {
		if pattern.ExecutionCount < advisor.config.MinQueryCount {
			continue
		}
		
		// Skip if query doesn't have both filter and project columns
		if len(pattern.FilterColumns) == 0 || len(pattern.ProjectColumns) == 0 {
			continue
		}
		
		// Check if this would benefit from a covering index
		allColumns := make([]string, 0)
		allColumns = append(allColumns, pattern.FilterColumns...)
		allColumns = append(allColumns, pattern.ProjectColumns...)
		allColumns = removeDuplicates(allColumns)
		
		// Skip if too many columns (covering indexes become inefficient)
		if len(allColumns) > 10 {
			continue
		}
		
		// Separate key columns (used in WHERE) from include columns (SELECT only)
		keyColumns := pattern.FilterColumns
		includeColumns := make([]string, 0)
		
		for _, col := range pattern.ProjectColumns {
			if !contains(keyColumns, col) {
				includeColumns = append(includeColumns, col)
			}
		}
		
		// Must have at least one include column to be worth it
		if len(includeColumns) == 0 {
			continue
		}
		
		// Estimate benefit
		currentCost := pattern.ActualCost * float64(pattern.ExecutionCount)
		projectedCost := advisor.estimateCoveringIndexCost(table, keyColumns, includeColumns, pattern)
		costReduction := currentCost - projectedCost
		
		if costReduction < advisor.config.MinCostReduction {
			continue
		}
		
		rec := IndexRecommendation{
			SchemaName:       table.SchemaName,
			TableName:        table.TableName,
			IndexName:        fmt.Sprintf("idx_%s_%s_covering", table.TableName, strings.Join(keyColumns, "_")),
			IndexType:        IndexTypeCovering,
			Columns:          keyColumns,
			IncludeColumns:   includeColumns,
			EstimatedBenefit: currentCost / projectedCost,
			CostReduction:    costReduction,
			AffectedQueries:  []QueryPattern{pattern},
			CurrentCost:      currentCost,
			ProjectedCost:    projectedCost,
			QueryCount:       pattern.ExecutionCount,
			CreatedAt:        time.Now(),
		}
		
		recommendations = append(recommendations, rec)
	}
	
	return recommendations
}

// analyzePartialIndexOpportunities identifies opportunities for partial indexes.
func (advisor *IndexAdvisor) analyzePartialIndexOpportunities(table *catalog.Table, patterns []QueryPattern) []IndexRecommendation {
	recommendations := make([]IndexRecommendation, 0)
	
	// Look for patterns with highly selective constant predicates
	for _, pattern := range patterns {
		if pattern.ExecutionCount < advisor.config.MinQueryCount {
			continue
		}
		
		// Check for highly selective equality filters
		for column, values := range pattern.EqualityFilters {
			// Skip if too many different values (not selective enough)
			if len(values) > 3 {
				continue
			}
			
			// Estimate selectivity
			if pattern.Selectivity < 0.1 { // Very selective
				whereClause := advisor.buildWhereClause(column, values)
				
				currentCost := pattern.ActualCost * float64(pattern.ExecutionCount)
				projectedCost := currentCost * 0.3 // Assume 70% reduction
				
				rec := IndexRecommendation{
					SchemaName:       table.SchemaName,
					TableName:        table.TableName,
					IndexName:        fmt.Sprintf("idx_%s_%s_partial", table.TableName, column),
					IndexType:        IndexTypePartial,
					Columns:          []string{column},
					WhereClause:      whereClause,
					EstimatedBenefit: currentCost / projectedCost,
					CostReduction:    currentCost - projectedCost,
					AffectedQueries:  []QueryPattern{pattern},
					CurrentCost:      currentCost,
					ProjectedCost:    projectedCost,
					QueryCount:       pattern.ExecutionCount,
					CreatedAt:        time.Now(),
				}
				
				recommendations = append(recommendations, rec)
			}
		}
	}
	
	return recommendations
}

// analyzeSingleColumnOpportunities identifies opportunities for simple single-column indexes.
func (advisor *IndexAdvisor) analyzeSingleColumnOpportunities(table *catalog.Table, patterns []QueryPattern) []IndexRecommendation {
	recommendations := make([]IndexRecommendation, 0)
	
	// Collect column usage statistics
	columnStats := make(map[string]*ColumnUsageStats)
	
	for _, pattern := range patterns {
		for _, column := range pattern.FilterColumns {
			if columnStats[column] == nil {
				columnStats[column] = &ColumnUsageStats{}
			}
			stats := columnStats[column]
			stats.QueryCount += pattern.ExecutionCount
			stats.TotalCost += pattern.ActualCost * float64(pattern.ExecutionCount)
			stats.Patterns = append(stats.Patterns, pattern)
		}
	}
	
	// Analyze each column
	for column, stats := range columnStats {
		if stats.QueryCount < advisor.config.MinQueryCount {
			continue
		}
		
		// Check if column already has an index
		if advisor.hasIndexOnColumn(table, column) {
			continue
		}
		
		// Estimate benefit
		projectedCost := stats.TotalCost * 0.5 // Assume 50% improvement
		costReduction := stats.TotalCost - projectedCost
		
		if costReduction < advisor.config.MinCostReduction {
			continue
		}
		
		rec := IndexRecommendation{
			SchemaName:       table.SchemaName,
			TableName:        table.TableName,
			IndexName:        fmt.Sprintf("idx_%s_%s", table.TableName, column),
			IndexType:        IndexTypeSingle,
			Columns:          []string{column},
			EstimatedBenefit: stats.TotalCost / projectedCost,
			CostReduction:    costReduction,
			AffectedQueries:  stats.Patterns,
			CurrentCost:      stats.TotalCost,
			ProjectedCost:    projectedCost,
			QueryCount:       stats.QueryCount,
			CreatedAt:        time.Now(),
		}
		
		recommendations = append(recommendations, rec)
	}
	
	return recommendations
}

// ColumnUsageStats tracks how a column is used across queries.
type ColumnUsageStats struct {
	QueryCount int
	TotalCost  float64
	Patterns   []QueryPattern
}

// Helper methods for cost estimation and analysis

func (advisor *IndexAdvisor) estimateCompositeIndexCost(table *catalog.Table, columns []string, patterns []QueryPattern) float64 {
	// Simplified cost estimation - would use the cost estimator in practice
	baseCost := 100.0
	for _, pattern := range patterns {
		baseCost += pattern.ActualCost * 0.2 // Assume 80% improvement
	}
	return baseCost
}

func (advisor *IndexAdvisor) estimateCoveringIndexCost(table *catalog.Table, keyColumns, includeColumns []string, pattern QueryPattern) float64 {
	// Covering index eliminates heap access
	return pattern.ActualCost * 0.15 // Assume 85% improvement
}

func (advisor *IndexAdvisor) optimizeColumnOrder(columns []string, patterns []QueryPattern) []string {
	// Put equality filters first, then range filters
	// This is a simplified version - would analyze actual predicate patterns
	return columns
}

func (advisor *IndexAdvisor) buildWhereClause(column string, values []types.Value) string {
	if len(values) == 1 {
		return fmt.Sprintf("%s = ?", column)
	}
	return fmt.Sprintf("%s IN (?)", column)
}

func (advisor *IndexAdvisor) hasIndexOnColumn(table *catalog.Table, column string) bool {
	if table.Indexes == nil {
		return false
	}
	for _, index := range table.Indexes {
		if len(index.Columns) >= 1 && index.Columns[0].Column.Name == column {
			return true
		}
	}
	return false
}

func (advisor *IndexAdvisor) calculateRecommendationPriority(rec *IndexRecommendation) {
	if rec.EstimatedBenefit >= 5.0 && rec.QueryCount >= 100 {
		rec.Priority = PriorityCritical
	} else if rec.EstimatedBenefit >= 3.0 && rec.QueryCount >= 50 {
		rec.Priority = PriorityHigh
	} else if rec.EstimatedBenefit >= 2.0 && rec.QueryCount >= 20 {
		rec.Priority = PriorityMedium
	} else {
		rec.Priority = PriorityLow
	}
}

func (advisor *IndexAdvisor) estimateImplementationCost(rec *IndexRecommendation, table *catalog.Table) {
	// Estimate index size and maintenance cost
	columnCount := len(rec.Columns) + len(rec.IncludeColumns)
	rec.EstimatedSize = int64(columnCount * 1000) // Simplified estimate
	rec.MaintenanceCost = float64(columnCount) * 0.1 // Simplified maintenance cost
}

func (advisor *IndexAdvisor) deduplicateRecommendations(recommendations []IndexRecommendation) []IndexRecommendation {
	// Remove duplicate or conflicting recommendations
	seen := make(map[string]bool)
	unique := make([]IndexRecommendation, 0)
	
	for _, rec := range recommendations {
		key := fmt.Sprintf("%s.%s:%s", rec.SchemaName, rec.TableName, strings.Join(rec.Columns, ","))
		if !seen[key] {
			seen[key] = true
			unique = append(unique, rec)
		}
	}
	
	return unique
}

// Utility functions

func removeDuplicates(slice []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0)
	
	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	
	return result
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}