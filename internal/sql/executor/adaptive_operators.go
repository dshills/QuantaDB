package executor

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/planner"
)

// adaptiveOperatorWrapper wraps scalar and vectorized operators for adaptive execution
type adaptiveOperatorWrapper struct {
	physicalPlan planner.PhysicalPlan
	scalarOp     Operator
	vectorizedOp VectorizedOperator
	currentMode  planner.ExecutionMode
	currentOp    Operator
	metrics      *OperatorMetrics
	monitor      *ExecutionMonitor

	// Runtime statistics
	rowsProcessed    atomic.Int64
	executionStart   time.Time
	lastModeSwitch   time.Time
	modeSwitchCount  int
	vectorizedErrors int
}

// Open opens the operator
func (aow *adaptiveOperatorWrapper) Open(ctx *ExecContext) error {
	aow.executionStart = time.Now()

	// Choose initial operator based on mode
	switch aow.currentMode {
	case planner.ExecutionModeVectorized:
		if aow.vectorizedOp != nil {
			aow.currentOp = aow.vectorizedOp
		} else {
			// Fall back to scalar if vectorized not available
			aow.currentMode = planner.ExecutionModeScalar
			aow.currentOp = aow.scalarOp
		}

	case planner.ExecutionModeScalar:
		aow.currentOp = aow.scalarOp

	case planner.ExecutionModeAdaptive, planner.ExecutionModeHybrid:
		// Start with vectorized if available
		if aow.vectorizedOp != nil {
			aow.currentOp = aow.vectorizedOp
			aow.currentMode = planner.ExecutionModeVectorized
		} else {
			aow.currentOp = aow.scalarOp
			aow.currentMode = planner.ExecutionModeScalar
		}
	}

	if aow.currentOp == nil {
		return fmt.Errorf("no operator available for mode %s", aow.currentMode)
	}

	return aow.currentOp.Open(ctx)
}

// Next returns the next row
func (aow *adaptiveOperatorWrapper) Next() (*Row, error) {
	row, err := aow.currentOp.Next()

	// Track metrics
	if row != nil {
		aow.rowsProcessed.Add(1)
	}

	// Handle vectorized errors
	if err != nil && aow.currentMode == planner.ExecutionModeVectorized {
		aow.vectorizedErrors++

		// Consider falling back to scalar after multiple errors
		if aow.vectorizedErrors > 3 && aow.scalarOp != nil {
			if switchErr := aow.SwitchMode(planner.ExecutionModeScalar); switchErr == nil {
				// Retry with scalar operator
				return aow.currentOp.Next()
			}
		}
	}

	// Update metrics periodically
	if aow.rowsProcessed.Load()%1000 == 0 {
		aow.updateMetrics()
	}

	return row, err
}

// Close closes the operator
func (aow *adaptiveOperatorWrapper) Close() error {
	// Final metrics update
	aow.updateMetrics()

	if aow.currentOp != nil {
		return aow.currentOp.Close()
	}
	return nil
}

// Schema returns the operator schema
func (aow *adaptiveOperatorWrapper) Schema() *Schema {
	if aow.currentOp != nil {
		return aow.currentOp.Schema()
	}

	// Try to get schema from any available operator
	if aow.scalarOp != nil {
		return aow.scalarOp.Schema()
	}
	if aow.vectorizedOp != nil {
		return aow.vectorizedOp.Schema()
	}

	return nil
}

// SwitchMode switches between execution modes
func (aow *adaptiveOperatorWrapper) SwitchMode(mode planner.ExecutionMode) error {
	if mode == aow.currentMode {
		return nil
	}

	// Check if target mode is available
	var newOp Operator
	switch mode {
	case planner.ExecutionModeScalar:
		if aow.scalarOp == nil {
			return fmt.Errorf("scalar operator not available")
		}
		newOp = aow.scalarOp

	case planner.ExecutionModeVectorized:
		if aow.vectorizedOp == nil {
			return fmt.Errorf("vectorized operator not available")
		}
		newOp = aow.vectorizedOp

	default:
		return fmt.Errorf("unsupported execution mode: %v", mode)
	}

	// Close current operator
	if aow.currentOp != nil {
		if err := aow.currentOp.Close(); err != nil {
			return fmt.Errorf("failed to close current operator: %w", err)
		}
	}

	// Open new operator
	// Note: In a real implementation, we'd need to restore state
	ctx := &ExecContext{} // This should be preserved from Open()
	if err := newOp.Open(ctx); err != nil {
		return fmt.Errorf("failed to open new operator: %w", err)
	}

	// Switch operators
	aow.currentOp = newOp
	aow.currentMode = mode
	aow.lastModeSwitch = time.Now()
	aow.modeSwitchCount++

	// Reset error counters
	if mode == planner.ExecutionModeScalar {
		aow.vectorizedErrors = 0
	}

	return nil
}

// GetCurrentMode returns the current execution mode
func (aow *adaptiveOperatorWrapper) GetCurrentMode() planner.ExecutionMode {
	return aow.currentMode
}

// GetMetrics returns current operator metrics
func (aow *adaptiveOperatorWrapper) GetMetrics() *OperatorMetrics {
	aow.updateMetrics()
	return aow.metrics
}

// updateMetrics updates operator metrics
func (aow *adaptiveOperatorWrapper) updateMetrics() {
	if aow.metrics == nil {
		aow.metrics = &OperatorMetrics{
			OperatorID:   fmt.Sprintf("%p", aow),
			OperatorType: aow.physicalPlan.GetOperatorType(),
		}
	}

	aow.metrics.RowsProcessed = aow.rowsProcessed.Load()
	aow.metrics.ExecutionTime = time.Since(aow.executionStart)
	aow.metrics.ExecutionMode = aow.currentMode
	aow.metrics.LastUpdate = time.Now()

	// Estimate memory usage (simplified)
	aow.metrics.MemoryUsed = aow.metrics.RowsProcessed * 100

	// Record in monitor
	if aow.monitor != nil {
		aow.monitor.RecordOperatorMetrics(aow.metrics)
	}
}

// monitoringOperatorWrapper adds monitoring to an adaptive operator
type monitoringOperatorWrapper struct {
	AdaptiveOperator
	monitor   *ExecutionMonitor
	startTime time.Time
	rowCount  atomic.Int64
}

// Open opens the operator with monitoring
func (mow *monitoringOperatorWrapper) Open(ctx *ExecContext) error {
	mow.startTime = time.Now()
	return mow.AdaptiveOperator.Open(ctx)
}

// Next returns the next row with monitoring
func (mow *monitoringOperatorWrapper) Next() (*Row, error) {
	row, err := mow.AdaptiveOperator.Next()

	if row != nil {
		mow.rowCount.Add(1)

		// Update metrics periodically
		if mow.rowCount.Load()%100 == 0 {
			metrics := mow.GetMetrics()
			metrics.RowsProcessed = mow.rowCount.Load()
			metrics.ExecutionTime = time.Since(mow.startTime)
			mow.monitor.RecordOperatorMetrics(metrics)
		}
	}

	return row, err
}

// Close closes the operator with final monitoring
func (mow *monitoringOperatorWrapper) Close() error {
	// Final metrics update
	metrics := mow.GetMetrics()
	metrics.RowsProcessed = mow.rowCount.Load()
	metrics.ExecutionTime = time.Since(mow.startTime)
	mow.monitor.RecordOperatorMetrics(metrics)

	return mow.AdaptiveOperator.Close()
}

// adaptiveResultWrapper provides adaptive result handling
type adaptiveResultWrapper struct {
	operator     AdaptiveOperator
	executor     *AdaptiveExecutor
	context      *ExecContext
	physicalPlan planner.PhysicalPlan

	// Adaptation state
	rowsReturned       int64
	lastAdaptCheck     time.Time
	adaptCheckInterval time.Duration
	replanCount        int

	// Buffering for re-planning
	bufferedRows []*Row
	bufferSize   int
	isBuffering  bool
}

// Next returns the next row with adaptive behavior
func (arw *adaptiveResultWrapper) Next() (*Row, error) {
	// Return buffered rows first
	if len(arw.bufferedRows) > 0 {
		row := arw.bufferedRows[0]
		arw.bufferedRows = arw.bufferedRows[1:]
		arw.rowsReturned++
		return row, nil
	}

	// Check if we should adapt
	if arw.shouldCheckAdaptation() {
		if err := arw.checkAndAdapt(); err != nil {
			// Log adaptation error but continue
			fmt.Printf("Adaptation check failed: %v\n", err)
		}
	}

	// Get next row from operator
	row, err := arw.operator.Next()
	if err != nil {
		return nil, err
	}

	if row != nil {
		arw.rowsReturned++

		// Buffer rows if we're preparing for adaptation
		if arw.isBuffering && len(arw.bufferedRows) < arw.bufferSize {
			arw.bufferedRows = append(arw.bufferedRows, row)
			// Continue buffering
			return arw.Next()
		}
	}

	return row, err
}

// Close closes the result
func (arw *adaptiveResultWrapper) Close() error {
	return arw.operator.Close()
}

// Schema returns the result schema
func (arw *adaptiveResultWrapper) Schema() *Schema {
	return arw.operator.Schema()
}

// shouldCheckAdaptation determines if we should check for adaptation
func (arw *adaptiveResultWrapper) shouldCheckAdaptation() bool {
	if !arw.executor.adaptiveConfig.EnableAdaptiveExecution {
		return false
	}

	// Don't check too frequently
	if time.Since(arw.lastAdaptCheck) < arw.adaptCheckInterval {
		return false
	}

	// Don't replan too many times
	if arw.replanCount >= arw.executor.adaptiveConfig.MaxReplanAttempts {
		return false
	}

	// Check after processing enough rows
	if arw.rowsReturned < 1000 {
		return false
	}

	return true
}

// checkAndAdapt checks if adaptation is needed and performs it
func (arw *adaptiveResultWrapper) checkAndAdapt() error {
	arw.lastAdaptCheck = time.Now()

	// Get current metrics
	metrics := arw.operator.GetMetrics()

	// Check if we're significantly off from predictions
	if arw.shouldReplan(metrics) {
		return arw.replan()
	}

	// Check if we should switch execution mode
	if arw.shouldSwitchMode(metrics) {
		return arw.switchMode(metrics)
	}

	return nil
}

// shouldReplan determines if re-planning is needed
func (arw *adaptiveResultWrapper) shouldReplan(metrics *OperatorMetrics) bool {
	// Compare actual vs predicted performance
	// This is simplified - would need actual vs predicted metrics

	// For now, don't replan
	return false
}

// shouldSwitchMode determines if execution mode should be switched
func (arw *adaptiveResultWrapper) shouldSwitchMode(metrics *OperatorMetrics) bool {
	// Check memory pressure
	memoryPressure := arw.executor.executionMonitor.GetMetrics().TotalMemory
	memoryLimit := arw.executor.config.QueryMemoryLimit

	if float64(memoryPressure)/float64(memoryLimit) > arw.executor.adaptiveConfig.MemoryPressureThreshold {
		// High memory pressure - consider switching to scalar
		if metrics.ExecutionMode == planner.ExecutionModeVectorized {
			return true
		}
	}

	// Check error rate for vectorized execution
	if wrapper, ok := arw.operator.(*adaptiveOperatorWrapper); ok {
		if wrapper.vectorizedErrors > 5 && metrics.ExecutionMode == planner.ExecutionModeVectorized {
			return true
		}
	}

	return false
}

// replan performs re-planning
func (arw *adaptiveResultWrapper) replan() error {
	// Start buffering to avoid losing rows
	arw.isBuffering = true
	arw.bufferSize = 100

	// TODO: Implement actual re-planning
	// This would involve:
	// 1. Closing current operators
	// 2. Re-generating physical plan with updated statistics
	// 3. Creating new operators
	// 4. Restoring execution state

	arw.replanCount++
	arw.isBuffering = false

	return fmt.Errorf("re-planning not yet implemented")
}

// switchMode switches the execution mode
func (arw *adaptiveResultWrapper) switchMode(metrics *OperatorMetrics) error {
	newMode := planner.ExecutionModeScalar
	if metrics.ExecutionMode == planner.ExecutionModeScalar {
		newMode = planner.ExecutionModeVectorized
	}

	// Only switch if operator supports it
	if arw.executor.adaptiveConfig.EnableOperatorAdaptation {
		return arw.operator.SwitchMode(newMode)
	}

	return nil
}
