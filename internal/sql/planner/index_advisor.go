package planner

import (
	"fmt"
	"sort"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// IndexAdvisor analyzes query patterns and recommends optimal indexes.
type IndexAdvisor struct {
	catalog         catalog.Catalog
	costEstimator   *CostEstimator
	queryAnalyzer   *QueryPatternAnalyzer
	recommendations []IndexRecommendation
	config          *IndexAdvisorConfig
}

// IndexAdvisorConfig contains configuration for the index advisor.
type IndexAdvisorConfig struct {
	// Minimum number of queries that must use a potential index
	MinQueryCount int
	// Minimum performance improvement required to recommend an index
	MinPerformanceGain float64
	// Maximum number of indexes to recommend per table
	MaxIndexesPerTable int
	// Consider indexes only for queries executed within this timeframe
	AnalysisTimeWindow time.Duration
	// Minimum cost reduction to justify creating an index
	MinCostReduction float64
}

// DefaultIndexAdvisorConfig returns sensible defaults for the index advisor.
func DefaultIndexAdvisorConfig() *IndexAdvisorConfig {
	return &IndexAdvisorConfig{
		MinQueryCount:      5,
		MinPerformanceGain: 2.0, // 2x improvement
		MaxIndexesPerTable: 10,
		AnalysisTimeWindow: 24 * time.Hour,
		MinCostReduction:   50.0, // Lower threshold for testing
	}
}

// NewIndexAdvisor creates a new index advisor.
func NewIndexAdvisor(catalog catalog.Catalog, costEstimator *CostEstimator) *IndexAdvisor {
	return &IndexAdvisor{
		catalog:         catalog,
		costEstimator:   costEstimator,
		queryAnalyzer:   NewQueryPatternAnalyzer(),
		recommendations: make([]IndexRecommendation, 0),
		config:          DefaultIndexAdvisorConfig(),
	}
}

// IndexRecommendation represents a recommended index with justification.
type IndexRecommendation struct {
	// Index definition
	SchemaName     string
	TableName      string
	IndexName      string
	IndexType      IndexRecommendationType
	Columns        []string // Key columns in order
	IncludeColumns []string // Non-key columns for covering indexes
	WhereClause    string   // For partial indexes

	// Performance analysis
	EstimatedBenefit float64        // Estimated performance improvement
	CostReduction    float64        // Estimated cost reduction
	AffectedQueries  []QueryPattern // Queries that would benefit
	CurrentCost      float64        // Current execution cost for affected queries
	ProjectedCost    float64        // Projected cost with the new index

	// Usage statistics
	QueryCount int // Number of queries that would use this index
	Priority   RecommendationPriority
	CreatedAt  time.Time

	// Implementation details
	EstimatedSize   int64   // Estimated index size in bytes
	MaintenanceCost float64 // Estimated maintenance overhead
}

// IndexRecommendationType defines the type of index being recommended.
type IndexRecommendationType int

const (
	IndexTypeComposite IndexRecommendationType = iota // Multi-column index
	IndexTypeCovering                                 // Index with INCLUDE columns
	IndexTypePartial                                  // Index with WHERE clause
	IndexTypeSingle                                   // Single-column index
)

func (t IndexRecommendationType) String() string {
	switch t {
	case IndexTypeComposite:
		return "Composite"
	case IndexTypeCovering:
		return "Covering"
	case IndexTypePartial:
		return "Partial"
	case IndexTypeSingle:
		return "Single"
	default:
		return "Unknown"
	}
}

// RecommendationPriority indicates the urgency of implementing an index recommendation.
type RecommendationPriority int

const (
	PriorityLow RecommendationPriority = iota
	PriorityMedium
	PriorityHigh
	PriorityCritical
)

func (p RecommendationPriority) String() string {
	switch p {
	case PriorityLow:
		return "Low"
	case PriorityMedium:
		return "Medium"
	case PriorityHigh:
		return "High"
	case PriorityCritical:
		return "Critical"
	default:
		return "Unknown"
	}
}

// String returns a detailed string representation of the recommendation.
func (r *IndexRecommendation) String() string {
	includeClause := ""
	if len(r.IncludeColumns) > 0 {
		includeClause = fmt.Sprintf(" INCLUDE (%s)", joinStrings(r.IncludeColumns, ", "))
	}

	whereClause := ""
	if r.WhereClause != "" {
		whereClause = fmt.Sprintf(" WHERE %s", r.WhereClause)
	}

	return fmt.Sprintf(
		"CREATE INDEX %s ON %s.%s (%s)%s%s -- %s priority, %.1fx speedup, %d queries affected",
		r.IndexName,
		r.SchemaName,
		r.TableName,
		joinStrings(r.Columns, ", "),
		includeClause,
		whereClause,
		r.Priority.String(),
		r.EstimatedBenefit,
		r.QueryCount,
	)
}

// AnalyzeQueries processes a batch of query patterns and generates index recommendations.
func (advisor *IndexAdvisor) AnalyzeQueries(queries []QueryPattern) error {
	// Clear previous recommendations
	advisor.recommendations = make([]IndexRecommendation, 0)

	// Group queries by table
	tableQueries := advisor.groupQueriesByTable(queries)

	// Analyze each table's query patterns
	for tableName, patterns := range tableQueries {
		recommendations, err := advisor.analyzeTableQueries(tableName, patterns)
		if err != nil {
			return fmt.Errorf("failed to analyze queries for table %s: %w", tableName, err)
		}
		advisor.recommendations = append(advisor.recommendations, recommendations...)
	}

	// Sort recommendations by priority and benefit
	advisor.sortRecommendations()

	// Apply configuration limits
	advisor.applyLimits()

	return nil
}

// GetRecommendations returns the current set of index recommendations.
func (advisor *IndexAdvisor) GetRecommendations() []IndexRecommendation {
	return advisor.recommendations
}

// GetTopRecommendations returns the highest priority recommendations up to the specified limit.
func (advisor *IndexAdvisor) GetTopRecommendations(limit int) []IndexRecommendation {
	if limit <= 0 || limit > len(advisor.recommendations) {
		return advisor.recommendations
	}
	return advisor.recommendations[:limit]
}

// groupQueriesByTable organizes query patterns by the primary table they access.
func (advisor *IndexAdvisor) groupQueriesByTable(queries []QueryPattern) map[string][]QueryPattern {
	tableGroups := make(map[string][]QueryPattern)

	for _, query := range queries {
		// Filter queries within the analysis time window
		if time.Since(query.LastSeen) > advisor.config.AnalysisTimeWindow {
			continue
		}

		// Skip queries that don't meet minimum execution count
		if query.ExecutionCount < advisor.config.MinQueryCount {
			continue
		}

		tableName := query.PrimaryTable
		if tableName == "" {
			continue // Skip queries without a clear primary table
		}

		tableGroups[tableName] = append(tableGroups[tableName], query)
	}

	return tableGroups
}

// sortRecommendations orders recommendations by priority and estimated benefit.
func (advisor *IndexAdvisor) sortRecommendations() {
	sort.Slice(advisor.recommendations, func(i, j int) bool {
		rec1, rec2 := advisor.recommendations[i], advisor.recommendations[j]

		// First sort by priority (higher priority first)
		if rec1.Priority != rec2.Priority {
			return rec1.Priority > rec2.Priority
		}

		// Then by estimated benefit (higher benefit first)
		if rec1.EstimatedBenefit != rec2.EstimatedBenefit {
			return rec1.EstimatedBenefit > rec2.EstimatedBenefit
		}

		// Finally by cost reduction (higher reduction first)
		return rec1.CostReduction > rec2.CostReduction
	})
}

// applyLimits applies configuration limits to the recommendations.
func (advisor *IndexAdvisor) applyLimits() {
	// Group by table and limit per table
	tableIndexCounts := make(map[string]int)
	filteredRecs := make([]IndexRecommendation, 0)

	for _, rec := range advisor.recommendations {
		tableKey := rec.SchemaName + "." + rec.TableName
		if tableIndexCounts[tableKey] < advisor.config.MaxIndexesPerTable {
			filteredRecs = append(filteredRecs, rec)
			tableIndexCounts[tableKey]++
		}
	}

	advisor.recommendations = filteredRecs
}

// joinStrings is a helper function to join string slices.
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
