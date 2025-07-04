package executor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// ExplainOperator executes EXPLAIN statements
type ExplainOperator struct {
	baseOperator
	plan     Operator
	analyze  bool
	verbose  bool
	format   string
	planText string
	executed bool
	storage  StorageBackend // For accessing buffer pool statistics
}

// NewExplainOperator creates a new EXPLAIN operator
func NewExplainOperator(plan Operator, analyze, verbose bool, format string, storage StorageBackend) *ExplainOperator {
	// Create single column schema for explain output
	schema := &Schema{
		Columns: []Column{
			{
				Name:     "QUERY PLAN",
				Type:     types.Text,
				Nullable: false,
			},
		},
	}

	return &ExplainOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		plan:    plan,
		analyze: analyze,
		verbose: verbose,
		format:  format,
		storage: storage,
	}
}

// Open initializes the explain operator
func (e *ExplainOperator) Open(ctx *ExecContext) error {
	e.ctx = ctx
	e.executed = false

	if e.analyze {
		// Enable statistics collection for EXPLAIN ANALYZE
		ctx.CollectStats = true
		ctx.StartTime = time.Now()
		ctx.OperatorStats = make(map[Operator]*OperatorStats)

		// Set up stats collector callback
		ctx.StatsCollector = func(op Operator, stats *OperatorStats) {
			ctx.OperatorStats[op] = stats
		}

		// Initialize buffer pool statistics collection
		if ctx.BufferStats == nil {
			ctx.BufferStats = &BufferPoolStats{}
		}

		// Get starting buffer pool statistics if available
		if e.storage != nil {
			if startStats := e.storage.GetBufferPoolStats(); startStats != nil {
				ctx.BufferStats.StartHitCount = startStats.PagesHit
				ctx.BufferStats.StartMissCount = startStats.PagesRead
			}
		}

		// Plan cache statistics will be populated by the calling layer
		// if a caching planner is being used

		// Execute the plan to collect runtime statistics
		if err := e.plan.Open(ctx); err != nil {
			return fmt.Errorf("error opening plan for EXPLAIN ANALYZE: %w", err)
		}

		// Consume all rows to get accurate statistics
		rowCount := int64(0)
		for {
			row, err := e.plan.Next()
			if err != nil {
				return fmt.Errorf("error executing plan for EXPLAIN ANALYZE: %w", err)
			}
			if row == nil {
				break // EOF
			}
			rowCount++
		}

		// Close the plan
		if err := e.plan.Close(); err != nil {
			return fmt.Errorf("error closing plan for EXPLAIN ANALYZE: %w", err)
		}

		// Calculate execution time and final buffer pool statistics
		ctx.ExecutionTime = time.Since(ctx.StartTime)

		// Get final buffer pool statistics and calculate deltas
		if e.storage != nil {
			if finalStats := e.storage.GetBufferPoolStats(); finalStats != nil {
				ctx.BufferStats.HitCount = finalStats.PagesHit
				ctx.BufferStats.MissCount = finalStats.PagesRead
				ctx.BufferStats.EvictionCount = finalStats.PagesEvicted
				ctx.BufferStats.DirtyPages = finalStats.PagesDirtied

				// Calculate deltas for this query
				ctx.BufferStats.Hits = ctx.BufferStats.HitCount - ctx.BufferStats.StartHitCount
				ctx.BufferStats.Misses = ctx.BufferStats.MissCount - ctx.BufferStats.StartMissCount
				ctx.BufferStats.PagesRead = ctx.BufferStats.Misses
				ctx.BufferStats.PagesWritten = ctx.BufferStats.DirtyPages // Approximate for now
			}
		}
	}

	// Generate the explain output
	e.planText = e.generateExplainOutput()

	return nil
}

// Next returns the explain output
func (e *ExplainOperator) Next() (*Row, error) {
	if e.executed {
		return nil, nil // EOF
	}

	e.executed = true

	// Return the explain output as a single row
	row := &Row{
		Values: []types.Value{
			types.NewValue(e.planText),
		},
	}

	return row, nil
}

// Close cleans up the explain operator
func (e *ExplainOperator) Close() error {
	return nil
}

// generateExplainOutput generates the formatted explain output
func (e *ExplainOperator) generateExplainOutput() string {
	switch e.format {
	case "json":
		return e.generateJSONOutput()
	default: // "text"
		return e.generateTextOutput()
	}
}

// generateTextOutput generates text format explain output
func (e *ExplainOperator) generateTextOutput() string {
	var buf bytes.Buffer

	// Add header for EXPLAIN ANALYZE
	if e.analyze {
		buf.WriteString("Query Execution Summary:\n")
		buf.WriteString(fmt.Sprintf("Planning Time: %.2f ms\n", e.ctx.PlanningTime.Seconds()*1000))
		buf.WriteString(fmt.Sprintf("Execution Time: %.2f ms\n", e.ctx.ExecutionTime.Seconds()*1000))
		buf.WriteString("\n")
	}

	// Generate plan tree
	e.writeOperatorPlan(&buf, e.plan, "", true)

	// Add buffer pool statistics if available and verbose
	if e.verbose && e.ctx.BufferStats != nil {
		buf.WriteString("\nBuffer Pool Statistics:\n")
		buf.WriteString(fmt.Sprintf("  Hits: %d\n", e.ctx.BufferStats.Hits))
		buf.WriteString(fmt.Sprintf("  Misses: %d\n", e.ctx.BufferStats.Misses))
		buf.WriteString(fmt.Sprintf("  Pages Read: %d\n", e.ctx.BufferStats.PagesRead))
		buf.WriteString(fmt.Sprintf("  Pages Written: %d\n", e.ctx.BufferStats.PagesWritten))
	}

	// Add plan cache statistics if available and verbose
	if e.verbose && e.ctx.PlanCacheStats != nil {
		buf.WriteString("\nPlan Cache Statistics:\n")
		buf.WriteString(fmt.Sprintf("  Cache Hits: %d\n", e.ctx.PlanCacheStats.HitCount))
		buf.WriteString(fmt.Sprintf("  Cache Misses: %d\n", e.ctx.PlanCacheStats.MissCount))
		buf.WriteString(fmt.Sprintf("  Cache Size: %d\n", e.ctx.PlanCacheStats.CurrentSize))
		buf.WriteString(fmt.Sprintf("  Cache Evictions: %d\n", e.ctx.PlanCacheStats.EvictionCount))
		if e.ctx.PlanCacheStats.HitCount+e.ctx.PlanCacheStats.MissCount > 0 {
			hitRate := float64(e.ctx.PlanCacheStats.HitCount) / float64(e.ctx.PlanCacheStats.HitCount+e.ctx.PlanCacheStats.MissCount) * 100
			buf.WriteString(fmt.Sprintf("  Hit Rate: %.1f%%\n", hitRate))
		}
	}

	return buf.String()
}

// writeOperatorPlan recursively writes operator plan information
func (e *ExplainOperator) writeOperatorPlan(buf *bytes.Buffer, op Operator, indent string, isLast bool) {
	// Determine the tree drawing characters
	prefix := indent
	if indent != "" {
		if isLast {
			prefix += "└─ "
		} else {
			prefix += "├─ "
		}
	}

	// Get operator description
	desc := e.getOperatorDescription(op)
	buf.WriteString(prefix + desc)

	// Add runtime statistics for EXPLAIN ANALYZE
	if e.analyze {
		stats := e.ctx.OperatorStats[op]
		if stats != nil {
			buf.WriteString(e.formatOperatorStats(stats))
		}
	}

	buf.WriteString("\n")

	// Get child operators
	children := e.getChildOperators(op)

	// Calculate new indent for children
	childIndent := indent
	if indent != "" {
		if isLast {
			childIndent += "   "
		} else {
			childIndent += "│  "
		}
	}

	// Recursively write children
	for i, child := range children {
		e.writeOperatorPlan(buf, child, childIndent, i == len(children)-1)
	}
}

// getOperatorDescription returns a string description of the operator
func (e *ExplainOperator) getOperatorDescription(op Operator) string {
	switch o := op.(type) {
	case *StorageScanOperator:
		return fmt.Sprintf("Sequential Scan on %s", o.table.TableName)
	case *IndexScanOperator:
		return fmt.Sprintf("Index Scan using %s on %s", o.index.Name, o.table.TableName)
	case *HashJoinOperator:
		return "Hash Join"
	case *NestedLoopJoinOperator:
		return "Nested Loop Join"
	case *FilterOperator:
		return "Filter"
	case *ProjectOperator:
		return "Project"
	case *SortOperator:
		return "Sort"
	case *LimitOperator:
		return fmt.Sprintf("Limit (count=%d)", o.limit)
	case *AggregateOperator:
		return "Aggregate"
	case *DistinctOperator:
		return "Hash Distinct"
	default:
		// Use type name as fallback
		typeName := fmt.Sprintf("%T", op)
		// Remove package prefix
		if idx := strings.LastIndex(typeName, "."); idx >= 0 {
			typeName = typeName[idx+1:]
		}
		// Remove "Operator" suffix
		typeName = strings.TrimSuffix(typeName, "Operator")
		return typeName
	}
}

// formatOperatorStats formats runtime statistics for display
func (e *ExplainOperator) formatOperatorStats(stats *OperatorStats) string {
	var parts []string

	// Basic timing and row information
	parts = append(parts, fmt.Sprintf("actual time=%.3f..%.3f",
		stats.StartupTimeMs, stats.TotalTimeMs))
	parts = append(parts, fmt.Sprintf("rows=%d", stats.ActualRows))
	parts = append(parts, fmt.Sprintf("loops=%d", stats.ActualLoops))

	// Memory usage if significant
	if stats.MemoryUsedKB > 0 {
		parts = append(parts, fmt.Sprintf("memory=%dKB", stats.MemoryUsedKB))
	}

	// Buffer statistics if available
	if stats.PagesHit > 0 || stats.PagesRead > 0 {
		hitRate := float64(0)
		if total := stats.PagesHit + stats.PagesRead; total > 0 {
			hitRate = float64(stats.PagesHit) / float64(total) * 100
		}
		parts = append(parts, fmt.Sprintf("buffers: hit=%d read=%d (%.1f%% hit rate)",
			stats.PagesHit, stats.PagesRead, hitRate))
	}

	// Extra information if verbose
	if e.verbose && len(stats.ExtraInfo) > 0 {
		for key, value := range stats.ExtraInfo {
			parts = append(parts, fmt.Sprintf("%s=%s", key, value))
		}
	}

	return " (" + strings.Join(parts, " ") + ")"
}

// getChildOperators returns the child operators of the given operator
func (e *ExplainOperator) getChildOperators(op Operator) []Operator {
	switch o := op.(type) {
	case *HashJoinOperator:
		return []Operator{o.left, o.right}
	case *NestedLoopJoinOperator:
		return []Operator{o.left, o.right}
	case *FilterOperator:
		return []Operator{o.child}
	case *ProjectOperator:
		return []Operator{o.child}
	case *SortOperator:
		return []Operator{o.child}
	case *LimitOperator:
		return []Operator{o.child}
	case *AggregateOperator:
		return []Operator{o.child}
	case *DistinctOperator:
		return []Operator{o.child}
	default:
		return nil
	}
}

// generateJSONOutput generates JSON format explain output
func (e *ExplainOperator) generateJSONOutput() string {
	plan := e.buildJSONPlan(e.plan)

	output := map[string]interface{}{
		"Plan": plan,
	}

	if e.analyze {
		output["Planning Time"] = e.ctx.PlanningTime.Seconds() * 1000
		output["Execution Time"] = e.ctx.ExecutionTime.Seconds() * 1000

		if e.ctx.BufferStats != nil {
			output["Buffer Statistics"] = map[string]interface{}{
				"Hits":          e.ctx.BufferStats.Hits,
				"Misses":        e.ctx.BufferStats.Misses,
				"Pages Read":    e.ctx.BufferStats.PagesRead,
				"Pages Written": e.ctx.BufferStats.PagesWritten,
			}
		}

		if e.ctx.PlanCacheStats != nil {
			hitRate := float64(0)
			if total := e.ctx.PlanCacheStats.HitCount + e.ctx.PlanCacheStats.MissCount; total > 0 {
				hitRate = float64(e.ctx.PlanCacheStats.HitCount) / float64(total) * 100
			}

			output["plan_cache_statistics"] = map[string]interface{}{
				"cache_hits":      e.ctx.PlanCacheStats.HitCount,
				"cache_misses":    e.ctx.PlanCacheStats.MissCount,
				"cache_size":      e.ctx.PlanCacheStats.CurrentSize,
				"cache_evictions": e.ctx.PlanCacheStats.EvictionCount,
				"hit_rate":        hitRate,
			}
		}
	}

	jsonBytes, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error generating JSON output: %v", err)
	}

	return string(jsonBytes)
}

// buildJSONPlan recursively builds JSON plan representation
func (e *ExplainOperator) buildJSONPlan(op Operator) map[string]interface{} {
	node := map[string]interface{}{
		"Node Type": e.getOperatorDescription(op),
	}

	// Add runtime statistics if available
	if e.analyze {
		if stats := e.ctx.OperatorStats[op]; stats != nil {
			node["Startup Time"] = stats.StartupTimeMs
			node["Total Time"] = stats.TotalTimeMs
			node["Actual Rows"] = stats.ActualRows
			node["Actual Loops"] = stats.ActualLoops

			if stats.MemoryUsedKB > 0 {
				node["Memory Used (KB)"] = stats.MemoryUsedKB
			}

			if stats.PagesHit > 0 || stats.PagesRead > 0 {
				node["Buffer Hits"] = stats.PagesHit
				node["Buffer Reads"] = stats.PagesRead
			}

			if len(stats.ExtraInfo) > 0 {
				node["Extra Info"] = stats.ExtraInfo
			}
		}
	}

	// Add child plans
	children := e.getChildOperators(op)
	if len(children) > 0 {
		var plans []interface{}
		for _, child := range children {
			plans = append(plans, e.buildJSONPlan(child))
		}
		node["Plans"] = plans
	}

	return node
}
