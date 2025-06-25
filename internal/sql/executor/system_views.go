package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// SystemViewOperator implements system tables/views for database monitoring
type SystemViewOperator struct {
	baseOperator
	viewType    string
	rows        [][]types.Value
	currentRow  int
	cacheStats  interface{} // Plan cache statistics from the planner
}

// NewSystemViewOperator creates a new system view operator
func NewSystemViewOperator(viewType string, cacheStats interface{}) *SystemViewOperator {
	var schema *Schema
	var rows [][]types.Value

	switch viewType {
	case "plan_cache_stats":
		schema = &Schema{
			Columns: []Column{
				{Name: "cache_hits", Type: types.BigInt, Nullable: false},
				{Name: "cache_misses", Type: types.BigInt, Nullable: false},
				{Name: "cache_size", Type: types.Integer, Nullable: false},
				{Name: "cache_evictions", Type: types.BigInt, Nullable: false},
				{Name: "hit_rate_percent", Type: types.Float, Nullable: false},
				{Name: "max_size", Type: types.Integer, Nullable: false},
			},
		}
		rows = buildPlanCacheStatsRows(cacheStats)

	default:
		// Unknown view type
		schema = &Schema{
			Columns: []Column{
				{Name: "error", Type: types.Text, Nullable: false},
			},
		}
		rows = [][]types.Value{
			{types.NewValue(fmt.Sprintf("Unknown system view: %s", viewType))},
		}
	}

	return &SystemViewOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		viewType:   viewType,
		rows:       rows,
		currentRow: 0,
		cacheStats: cacheStats,
	}
}

// buildPlanCacheStatsRows builds rows for the plan_cache_stats view
func buildPlanCacheStatsRows(cacheStats interface{}) [][]types.Value {
	// Default empty stats if no cache stats provided
	var hitCount, missCount, evictionCount int64 = 0, 0, 0
	var currentSize, maxSize int = 0, 0
	var hitRate float64 = 0.0

	// Try to extract cache statistics using reflection or type assertion
	// This approach avoids import cycles
	if cacheStats != nil {
		// For now, return default values
		// In a real implementation, this would use reflection or a common interface
		// to extract the actual statistics from the planner's cache
	}

	if hitCount+missCount > 0 {
		hitRate = float64(hitCount) / float64(hitCount+missCount) * 100.0
	}

	return [][]types.Value{
		{
			types.NewValue(hitCount),
			types.NewValue(missCount),
			types.NewValue(int64(currentSize)),
			types.NewValue(evictionCount),
			types.NewValue(hitRate),
			types.NewValue(int64(maxSize)),
		},
	}
}

// Open initializes the system view operator
func (sv *SystemViewOperator) Open(ctx *ExecContext) error {
	sv.ctx = ctx
	sv.currentRow = 0
	return nil
}

// Next returns the next row from the system view
func (sv *SystemViewOperator) Next() (*Row, error) {
	if sv.currentRow >= len(sv.rows) {
		return nil, nil // EOF
	}

	row := &Row{
		Values: sv.rows[sv.currentRow],
	}
	sv.currentRow++

	return row, nil
}

// Close cleans up the system view operator
func (sv *SystemViewOperator) Close() error {
	return nil
}

// GetSystemViewSchema returns the schema for a system view
func GetSystemViewSchema(viewType string) *Schema {
	switch viewType {
	case "plan_cache_stats":
		return &Schema{
			Columns: []Column{
				{Name: "cache_hits", Type: types.BigInt, Nullable: false},
				{Name: "cache_misses", Type: types.BigInt, Nullable: false},
				{Name: "cache_size", Type: types.Integer, Nullable: false},
				{Name: "cache_evictions", Type: types.BigInt, Nullable: false},
				{Name: "hit_rate_percent", Type: types.Float, Nullable: false},
				{Name: "max_size", Type: types.Integer, Nullable: false},
			},
		}
	default:
		return &Schema{
			Columns: []Column{
				{Name: "error", Type: types.Text, Nullable: false},
			},
		}
	}
}