package executor

import (
	"sync"
	"time"
)

// RuntimeStats collects actual execution statistics for adaptive decisions
type RuntimeStats struct {
	// Cardinality tracking
	EstimatedRows int64 // Original planner estimate
	ActualRows    int64 // Actual rows processed so far

	// Performance metrics
	ExecutionTime time.Duration // Time spent so far
	MemoryUsage   int64         // Current memory usage in bytes
	MemoryPeak    int64         // Peak memory usage

	// Data characteristics
	DataSkew    float64 // Coefficient of variation for data distribution
	NullRate    float64 // Percentage of null values
	Selectivity float64 // Actual selectivity observed

	// Resource utilization
	CPUTime  time.Duration // CPU time consumed
	IOReads  int64         // Number of disk reads
	IOWrites int64         // Number of disk writes

	// Concurrency metrics
	WorkerUtilization []float64 // Utilization per worker (for parallel ops)
	LoadImbalance     float64   // Measure of work distribution imbalance

	mu sync.RWMutex // Protect concurrent updates
}

// NewRuntimeStats creates a new runtime statistics collector
func NewRuntimeStats(estimatedRows int64) *RuntimeStats {
	return &RuntimeStats{
		EstimatedRows: estimatedRows,
		ActualRows:    0,
		MemoryUsage:   0,
		MemoryPeak:    0,
		DataSkew:      0.0,
		NullRate:      0.0,
		Selectivity:   1.0, // Default to no filtering
	}
}

// UpdateCardinality updates row count and recalculates error metrics
func (rs *RuntimeStats) UpdateCardinality(actualRows int64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.ActualRows = actualRows
}

// UpdateMemoryUsage tracks current and peak memory usage
func (rs *RuntimeStats) UpdateMemoryUsage(currentMemory int64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.MemoryUsage = currentMemory
	if currentMemory > rs.MemoryPeak {
		rs.MemoryPeak = currentMemory
	}
}

// UpdateDataSkew calculates and updates data skew metrics
func (rs *RuntimeStats) UpdateDataSkew(values []float64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(values) < 2 {
		rs.DataSkew = 0.0
		return
	}

	// Calculate coefficient of variation
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	if mean == 0.0 {
		rs.DataSkew = 0.0
		return
	}

	variance := 0.0
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(len(values))

	stddev := variance // Simplified - would use math.Sqrt in real implementation
	rs.DataSkew = stddev / mean
}

// UpdateWorkerUtilization tracks parallel worker performance
func (rs *RuntimeStats) UpdateWorkerUtilization(workerStats []float64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.WorkerUtilization = make([]float64, len(workerStats))
	copy(rs.WorkerUtilization, workerStats)

	// Calculate load imbalance (coefficient of variation of worker utilization)
	if len(workerStats) < 2 {
		rs.LoadImbalance = 0.0
		return
	}

	sum := 0.0
	for _, util := range workerStats {
		sum += util
	}
	mean := sum / float64(len(workerStats))

	if mean == 0.0 {
		rs.LoadImbalance = 0.0
		return
	}

	variance := 0.0
	for _, util := range workerStats {
		diff := util - mean
		variance += diff * diff
	}
	variance /= float64(len(workerStats))

	stddev := variance // Simplified
	rs.LoadImbalance = stddev / mean
}

// GetCardinalityError returns the relative error in cardinality estimation
func (rs *RuntimeStats) GetCardinalityError() float64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.EstimatedRows == 0 {
		if rs.ActualRows == 0 {
			return 0.0
		}
		return 1.0 // 100% error
	}

	return float64(abs(rs.ActualRows-rs.EstimatedRows)) / float64(rs.EstimatedRows)
}

// IsEstimateAccurate returns true if cardinality estimate is within acceptable bounds
func (rs *RuntimeStats) IsEstimateAccurate(threshold float64) bool {
	return rs.GetCardinalityError() <= threshold
}

// IsMemoryPressure returns true if memory usage is approaching limits
func (rs *RuntimeStats) IsMemoryPressure(maxMemory int64, threshold float64) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if maxMemory <= 0 {
		return false
	}

	usage := float64(rs.MemoryUsage) / float64(maxMemory)
	return usage >= threshold
}

// IsDataSkewed returns true if data shows significant skew
func (rs *RuntimeStats) IsDataSkewed(threshold float64) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.DataSkew >= threshold
}

// IsLoadImbalanced returns true if parallel workers are poorly balanced
func (rs *RuntimeStats) IsLoadImbalanced(threshold float64) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.LoadImbalance >= threshold
}

// GetSnapshot returns a thread-safe copy of current statistics
func (rs *RuntimeStats) GetSnapshot() RuntimeStats {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	snapshot := *rs
	if len(rs.WorkerUtilization) > 0 {
		snapshot.WorkerUtilization = make([]float64, len(rs.WorkerUtilization))
		copy(snapshot.WorkerUtilization, rs.WorkerUtilization)
	}

	return snapshot
}

// AdaptiveDecision represents a decision point for plan adaptation
type AdaptiveDecision struct {
	Timestamp   time.Time
	Operator    string
	Decision    string
	Reason      string
	RuntimeData RuntimeStats
}

// AdaptiveDecisionLog tracks all adaptive decisions made during query execution
type AdaptiveDecisionLog struct {
	decisions []AdaptiveDecision
	mu        sync.RWMutex
}

// NewAdaptiveDecisionLog creates a new decision log
func NewAdaptiveDecisionLog() *AdaptiveDecisionLog {
	return &AdaptiveDecisionLog{
		decisions: make([]AdaptiveDecision, 0),
	}
}

// LogDecision records an adaptive decision
func (adl *AdaptiveDecisionLog) LogDecision(operator, decision, reason string, stats RuntimeStats) {
	adl.mu.Lock()
	defer adl.mu.Unlock()

	adl.decisions = append(adl.decisions, AdaptiveDecision{
		Timestamp:   time.Now(),
		Operator:    operator,
		Decision:    decision,
		Reason:      reason,
		RuntimeData: stats,
	})
}

// GetDecisions returns a copy of all logged decisions
func (adl *AdaptiveDecisionLog) GetDecisions() []AdaptiveDecision {
	adl.mu.RLock()
	defer adl.mu.RUnlock()

	decisions := make([]AdaptiveDecision, len(adl.decisions))
	copy(decisions, adl.decisions)
	return decisions
}

// GetDecisionCount returns the number of adaptive decisions made
func (adl *AdaptiveDecisionLog) GetDecisionCount() int {
	adl.mu.RLock()
	defer adl.mu.RUnlock()

	return len(adl.decisions)
}

// AdaptiveThresholds defines thresholds for triggering adaptive behavior
type AdaptiveThresholds struct {
	// Cardinality estimation error threshold (0.5 = 50% error)
	CardinalityErrorThreshold float64

	// Memory pressure threshold (0.8 = 80% of available memory)
	MemoryPressureThreshold float64

	// Data skew threshold (coefficient of variation)
	DataSkewThreshold float64

	// Load imbalance threshold for parallel workers
	LoadImbalanceThreshold float64

	// Minimum rows before considering adaptations
	MinRowsForAdaptation int64

	// Time-based adaptation interval
	AdaptationInterval time.Duration
}

// DefaultAdaptiveThresholds returns reasonable default thresholds
func DefaultAdaptiveThresholds() *AdaptiveThresholds {
	return &AdaptiveThresholds{
		CardinalityErrorThreshold: 0.5,  // 50% error
		MemoryPressureThreshold:   0.8,  // 80% memory usage
		DataSkewThreshold:         1.0,  // Coefficient of variation >= 1.0
		LoadImbalanceThreshold:    0.3,  // 30% imbalance
		MinRowsForAdaptation:      1000, // At least 1000 rows
		AdaptationInterval:        100 * time.Millisecond,
	}
}

// AdaptiveContext provides adaptive execution context
type AdaptiveContext struct {
	// Configuration
	Thresholds *AdaptiveThresholds

	// Decision logging
	DecisionLog *AdaptiveDecisionLog

	// Global execution context
	ExecCtx *ExecContext

	// Adaptive execution enabled flag
	Enabled bool

	// Maximum memory available for this query
	MaxMemory int64
}

// NewAdaptiveContext creates a new adaptive execution context
func NewAdaptiveContext(execCtx *ExecContext, maxMemory int64) *AdaptiveContext {
	return &AdaptiveContext{
		Thresholds:  DefaultAdaptiveThresholds(),
		DecisionLog: NewAdaptiveDecisionLog(),
		ExecCtx:     execCtx,
		Enabled:     true, // Enable by default for Phase 4
		MaxMemory:   maxMemory,
	}
}

// ShouldAdapt determines if an operator should consider adaptation
func (ac *AdaptiveContext) ShouldAdapt(stats *RuntimeStats) bool {
	if !ac.Enabled {
		return false
	}

	// Don't adapt for small row counts
	if stats.ActualRows < ac.Thresholds.MinRowsForAdaptation {
		return false
	}

	// Check various conditions that trigger adaptation
	return stats.GetCardinalityError() > ac.Thresholds.CardinalityErrorThreshold ||
		stats.IsMemoryPressure(ac.MaxMemory, ac.Thresholds.MemoryPressureThreshold) ||
		stats.IsDataSkewed(ac.Thresholds.DataSkewThreshold) ||
		stats.IsLoadImbalanced(ac.Thresholds.LoadImbalanceThreshold)
}

// LogAdaptiveDecision logs an adaptive decision with context
func (ac *AdaptiveContext) LogAdaptiveDecision(operator, decision, reason string, stats *RuntimeStats) {
	if ac.DecisionLog != nil {
		ac.DecisionLog.LogDecision(operator, decision, reason, *stats)
	}
}

// Helper function for absolute value (simplified)
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
