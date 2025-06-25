package executor

import (
	"github.com/dshills/QuantaDB/internal/sql/planner"
)

// PopulatePlanCacheStats populates plan cache statistics in the execution context
// from a caching planner. This function should be called by the SQL interface layer
// when executing EXPLAIN ANALYZE queries with a caching planner.
func PopulatePlanCacheStats(ctx *ExecContext, plannr planner.Planner) {
	if ctx == nil {
		return
	}

	// Check if the planner supports cache statistics
	if cachingPlanner, ok := plannr.(interface {
		GetCacheStats() *planner.PlanCacheStats
	}); ok {
		if plannerStats := cachingPlanner.GetCacheStats(); plannerStats != nil {
			// Use the planner cache stats directly (no conversion needed)
			ctx.PlanCacheStats = plannerStats
		}
	}
}

// CreateSystemView creates a system view operator for monitoring database internals
func CreateSystemView(viewName string, plannr planner.Planner) Operator {
	switch viewName {
	case "plan_cache_stats":
		var cacheStats interface{}
		if cachingPlanner, ok := plannr.(interface {
			GetCacheStats() *planner.PlanCacheStats
		}); ok {
			cacheStats = cachingPlanner.GetCacheStats()
		}
		return NewSystemViewOperator("plan_cache_stats", cacheStats)
	default:
		return NewSystemViewOperator(viewName, nil)
	}
}
