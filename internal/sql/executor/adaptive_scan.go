package executor

import (
	"fmt"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// ScanMethod represents different scan implementation strategies
type ScanMethod int

const (
	SequentialScanMethod ScanMethod = iota
	IndexScanMethod
	ParallelScanMethod
	AutoScanMethod // Let adaptive logic decide
)

func (sm ScanMethod) String() string {
	switch sm {
	case SequentialScanMethod:
		return "Sequential"
	case IndexScanMethod:
		return "Index"
	case ParallelScanMethod:
		return "Parallel"
	case AutoScanMethod:
		return "Auto"
	default:
		return "Unknown"
	}
}

// AdaptiveScanOperator dynamically adjusts scan strategy based on runtime characteristics
type AdaptiveScanOperator struct {
	baseOperator
	
	// Table information
	table       *catalog.Table
	tableAlias  string
	storage     StorageBackend
	predicate   ExprEvaluator // Optional filter predicate
	
	// Adaptive components
	adaptiveCtx   *AdaptiveContext
	initialMethod ScanMethod
	currentMethod ScanMethod
	currentImpl   Operator // Current scan implementation
	
	// Runtime statistics
	scanStats *RuntimeStats
	
	// Adaptation configuration
	evaluationInterval time.Duration
	lastEvaluation     time.Time
	minRowsForSwitch   int64
	
	// Execution state
	rowsProcessed   int64
	hasSwitched     bool
	switchPoint     int64
	parallelContext *ParallelContext
	
	// Performance tracking
	startTime           time.Time
	lastThroughputCheck time.Time
	lastRowCount        int64
	currentThroughput   float64 // rows per second
	targetThroughput    float64 // expected rows per second
}

// NewAdaptiveScanOperator creates a new adaptive scan operator
func NewAdaptiveScanOperator(
	table *catalog.Table,
	tableAlias string,
	storage StorageBackend,
	predicate ExprEvaluator,
	adaptiveCtx *AdaptiveContext,
) *AdaptiveScanOperator {
	// Build schema from table columns
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	
	// Use alias if provided, otherwise use table name
	if tableAlias == "" {
		tableAlias = table.TableName
	}
	
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name:       col.Name,
			Type:       col.DataType,
			Nullable:   col.IsNullable,
			TableName:  table.TableName,
			TableAlias: tableAlias,
		}
	}
	
	return &AdaptiveScanOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:              table,
		tableAlias:         tableAlias,
		storage:            storage,
		predicate:          predicate,
		adaptiveCtx:        adaptiveCtx,
		initialMethod:      AutoScanMethod,
		currentMethod:      AutoScanMethod,
		evaluationInterval: 100 * time.Millisecond,
		minRowsForSwitch:   5000, // Don't switch until we have enough data
		targetThroughput:   10000, // Target 10K rows/second
	}
}

// Open initializes the adaptive scan
func (as *AdaptiveScanOperator) Open(ctx *ExecContext) error {
	as.ctx = ctx
	as.startTime = time.Now()
	as.lastThroughputCheck = as.startTime
	
	// Initialize statistics
	as.scanStats = NewRuntimeStats(1000) // Default estimate
	as.initStats(1000)
	
	// Set transaction ID if available
	if ctx.Txn != nil {
		as.storage.SetTransactionID(uint64(ctx.Txn.ID()))
	}
	
	// Choose initial scan method
	as.currentMethod = as.selectInitialScanMethod()
	
	// Create initial scan implementation
	impl, err := as.createScanImplementation(as.currentMethod)
	if err != nil {
		return fmt.Errorf("failed to create initial scan implementation: %w", err)
	}
	as.currentImpl = impl
	
	// Log initial decision
	if as.adaptiveCtx != nil {
		as.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveScan",
			fmt.Sprintf("InitialMethod=%s", as.currentMethod),
			"Initial scan method selection",
			as.scanStats,
		)
	}
	
	return as.currentImpl.Open(ctx)
}

// selectInitialScanMethod chooses the best initial scan method
func (as *AdaptiveScanOperator) selectInitialScanMethod() ScanMethod {
	// Simple heuristics for initial selection
	// In a real implementation, this would use cost-based optimization
	
	// If we have a predicate and suitable indexes, prefer index scan
	if as.predicate != nil {
		// TODO: Check if table has suitable indexes for the predicate
		// For now, assume sequential scan is safer
	}
	
	// Check table size estimates
	// For large tables with available parallelism, prefer parallel scan
	if as.adaptiveCtx != nil && as.adaptiveCtx.ExecCtx != nil {
		// TODO: Get table size estimate from catalog statistics
		// For now, default to sequential scan
	}
	
	return SequentialScanMethod
}

// createScanImplementation creates a specific scan implementation
func (as *AdaptiveScanOperator) createScanImplementation(method ScanMethod) (Operator, error) {
	switch method {
	case SequentialScanMethod:
		scan := NewStorageScanOperatorWithAlias(as.table, as.tableAlias, as.storage)
		if as.predicate != nil {
			return NewFilterOperator(scan, as.predicate), nil
		}
		return scan, nil
		
	case ParallelScanMethod:
		if as.parallelContext == nil {
			// Create parallel context for this scan
			as.parallelContext = NewParallelContext(as.adaptiveCtx.ExecCtx, 4)
		}
		scan := NewParallelScanOperatorWithAlias(as.table, as.tableAlias, as.storage, as.parallelContext)
		if as.predicate != nil {
			return NewFilterOperator(scan, as.predicate), nil
		}
		return scan, nil
		
	case IndexScanMethod:
		// For now, fall back to sequential scan
		// In a complete implementation, we'd have index scan selection logic
		scan := NewStorageScanOperatorWithAlias(as.table, as.tableAlias, as.storage)
		if as.predicate != nil {
			return NewFilterOperator(scan, as.predicate), nil
		}
		return scan, nil
		
	default:
		return nil, fmt.Errorf("unsupported scan method: %s", method)
	}
}

// Next returns the next row with adaptive behavior
func (as *AdaptiveScanOperator) Next() (*Row, error) {
	// Check if we should consider adaptation
	if as.shouldEvaluateAdaptation() {
		if err := as.evaluateAdaptation(); err != nil {
			return nil, fmt.Errorf("adaptation evaluation failed: %w", err)
		}
	}
	
	// Get next row from current implementation
	row, err := as.currentImpl.Next()
	if err != nil {
		return nil, err
	}
	
	if row != nil {
		// Update statistics
		as.rowsProcessed++
		as.scanStats.UpdateCardinality(as.rowsProcessed)
		as.updateThroughputStats()
		as.recordRow()
	} else {
		// EOF - finalize statistics
		as.finishStats()
		if as.ctx != nil && as.ctx.StatsCollector != nil && as.stats != nil {
			as.ctx.StatsCollector(as, as.stats)
		}
	}
	
	return row, nil
}

// updateThroughputStats calculates current scan throughput
func (as *AdaptiveScanOperator) updateThroughputStats() {
	now := time.Now()
	timeDiff := now.Sub(as.lastThroughputCheck)
	
	// Update throughput every second
	if timeDiff >= time.Second {
		rowsDiff := as.rowsProcessed - as.lastRowCount
		as.currentThroughput = float64(rowsDiff) / timeDiff.Seconds()
		
		as.lastThroughputCheck = now
		as.lastRowCount = as.rowsProcessed
	}
}

// shouldEvaluateAdaptation determines if we should check for adaptation
func (as *AdaptiveScanOperator) shouldEvaluateAdaptation() bool {
	if as.adaptiveCtx == nil || !as.adaptiveCtx.Enabled {
		return false
	}
	
	// Don't adapt if we've already switched
	if as.hasSwitched {
		return false
	}
	
	// Don't adapt too frequently
	if time.Since(as.lastEvaluation) < as.evaluationInterval {
		return false
	}
	
	// Don't adapt until we have enough data
	if as.rowsProcessed < as.minRowsForSwitch {
		return false
	}
	
	return true
}

// evaluateAdaptation checks if we should switch scan methods
func (as *AdaptiveScanOperator) evaluateAdaptation() error {
	as.lastEvaluation = time.Now()
	
	// Collect current runtime statistics
	snapshot := as.scanStats.GetSnapshot()
	
	// Check if current performance is poor
	shouldSwitch, newMethod, reason := as.shouldSwitchScanMethod(&snapshot)
	
	if shouldSwitch {
		if err := as.switchScanMethod(newMethod, reason); err != nil {
			return fmt.Errorf("failed to switch scan method: %w", err)
		}
	}
	
	return nil
}

// shouldSwitchScanMethod determines if we should switch scan algorithms
func (as *AdaptiveScanOperator) shouldSwitchScanMethod(stats *RuntimeStats) (bool, ScanMethod, string) {
	// Check throughput performance
	if as.currentThroughput > 0 && as.currentThroughput < as.targetThroughput*0.5 {
		switch as.currentMethod {
		case SequentialScanMethod:
			// If sequential scan is slow and we have parallel capability, switch
			if as.adaptiveCtx.ExecCtx != nil {
				return true, ParallelScanMethod, fmt.Sprintf("Low throughput (%.0f rows/s, target %.0f)", as.currentThroughput, as.targetThroughput)
			}
		}
	}
	
	// Check for memory pressure
	if as.adaptiveCtx.MaxMemory > 0 && stats.IsMemoryPressure(as.adaptiveCtx.MaxMemory, 0.8) {
		if as.currentMethod == ParallelScanMethod {
			return true, SequentialScanMethod, "High memory pressure - switch to sequential"
		}
	}
	
	// Check for poor CPU utilization (would require system metrics)
	// This is a placeholder for more sophisticated monitoring
	if as.currentThroughput > 0 && as.currentThroughput > as.targetThroughput*2.0 {
		// Performance is very good, might consider more aggressive parallelism
		// But for now, keep current method
	}
	
	return false, as.currentMethod, ""
}

// switchScanMethod performs the actual switch to a new scan algorithm
func (as *AdaptiveScanOperator) switchScanMethod(newMethod ScanMethod, reason string) error {
	// Log the decision
	if as.adaptiveCtx != nil {
		as.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveScan",
			fmt.Sprintf("Switch %s -> %s", as.currentMethod, newMethod),
			reason,
			as.scanStats,
		)
	}
	
	// For scan operations, switching mid-execution is complex because we need to
	// track where we are in the table. For now, we'll just log the decision
	// and apply it to future operations. A complete implementation would need
	// to handle state transfer between scan methods.
	
	// In a production system, you might:
	// 1. Materialize current position
	// 2. Close current implementation
	// 3. Create new implementation starting from saved position
	// 4. Handle any necessary state conversion
	
	// For this implementation, we'll defer the switch to avoid complexity
	// but still log the adaptive decision for monitoring
	
	as.switchPoint = as.rowsProcessed
	as.hasSwitched = true
	
	return nil
}

// Close cleans up the adaptive scan
func (as *AdaptiveScanOperator) Close() error {
	var err error
	
	// Close current implementation
	if as.currentImpl != nil {
		if closeErr := as.currentImpl.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	
	// Close parallel context if we created one
	if as.parallelContext != nil {
		if closeErr := as.parallelContext.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	
	// Ensure stats are finalized
	as.finishStats()
	if as.ctx != nil && as.ctx.StatsCollector != nil && as.stats != nil {
		as.ctx.StatsCollector(as, as.stats)
	}
	
	return err
}

// GetAdaptiveStats returns adaptive-specific statistics
func (as *AdaptiveScanOperator) GetAdaptiveStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	stats["initial_method"] = as.initialMethod.String()
	stats["current_method"] = as.currentMethod.String()
	stats["has_switched"] = as.hasSwitched
	stats["switch_point"] = as.switchPoint
	stats["rows_processed"] = as.rowsProcessed
	stats["current_throughput"] = as.currentThroughput
	stats["target_throughput"] = as.targetThroughput
	
	if as.adaptiveCtx != nil && as.adaptiveCtx.DecisionLog != nil {
		stats["decision_count"] = as.adaptiveCtx.DecisionLog.GetDecisionCount()
	}
	
	return stats
}

// AdaptiveScanConfig configures adaptive scan behavior
type AdaptiveScanConfig struct {
	// Enable adaptive behavior
	EnableAdaptation bool
	
	// Minimum rows before considering switches
	MinRowsForSwitch int64
	
	// How often to evaluate adaptation
	EvaluationInterval time.Duration
	
	// Target throughput (rows per second)
	TargetThroughput float64
	
	// Memory pressure threshold for triggering switches
	MemoryPressureThreshold float64
}

// DefaultAdaptiveScanConfig returns reasonable defaults
func DefaultAdaptiveScanConfig() *AdaptiveScanConfig {
	return &AdaptiveScanConfig{
		EnableAdaptation:        true,
		MinRowsForSwitch:        5000,
		EvaluationInterval:      100 * time.Millisecond,
		TargetThroughput:        10000, // 10K rows/second
		MemoryPressureThreshold: 0.8,
	}
}

// Make AdaptiveScanOperator implement ParallelizableOperator interface
func (as *AdaptiveScanOperator) CanParallelize() bool {
	return true
}

func (as *AdaptiveScanOperator) GetParallelDegree() int {
	if as.currentMethod == ParallelScanMethod {
		return 4
	}
	return 1
}

func (as *AdaptiveScanOperator) CreateParallelInstance(pc *ParallelContext) (Operator, error) {
	// Create a parallel version of the adaptive scan
	adaptiveScan := &AdaptiveScanOperator{
		baseOperator:       as.baseOperator,
		table:              as.table,
		tableAlias:         as.tableAlias,
		storage:            as.storage,
		predicate:          as.predicate,
		adaptiveCtx:        as.adaptiveCtx,
		initialMethod:      ParallelScanMethod, // Force parallel for parallel instance
		currentMethod:      ParallelScanMethod,
		evaluationInterval: as.evaluationInterval,
		minRowsForSwitch:   as.minRowsForSwitch,
		targetThroughput:   as.targetThroughput,
		parallelContext:    pc,
	}
	
	return adaptiveScan, nil
}