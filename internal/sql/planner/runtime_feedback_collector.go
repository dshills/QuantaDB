package planner

import (
	"sync"
	"time"
)

// RuntimeFeedbackCollector collects runtime performance metrics
type RuntimeFeedbackCollector struct {
	mu                  sync.RWMutex
	operatorPerformance map[OperatorSignature]*PerformanceMetrics
	memoryUsagePatterns map[QuerySignature]*MemoryProfile
	cacheEffectiveness  map[CacheKey]*CachePerformance
	queryHistory        *QueryHistoryTracker
	calibrationData     *CostModelCalibrationData
}

// PerformanceMetrics tracks actual vs predicted performance
type PerformanceMetrics struct {
	ActualExecutionTime time.Duration
	PredictedTime       time.Duration
	MemoryUsed          int64
	PredictedMemory     int64
	RowsProcessed       int64
	PredictedRows       int64
	VectorizationRatio  float64
	CacheHitRate        float64
	LastUpdated         time.Time
	SampleCount         int64
}

// QuerySignature uniquely identifies a query pattern
type QuerySignature struct {
	QueryHash      string
	MainOperator   OperatorType
	JoinCount      int
	PredicateCount int
	AggregateCount int
}

// MemoryProfile tracks memory usage patterns
type MemoryProfile struct {
	PeakMemoryUsage  int64
	AvgMemoryUsage   int64
	MemorySpikes     int
	GCPressureEvents int
	LastOOM          time.Time
	SuccessfulRuns   int64
	FailedRuns       int64
}

// CacheKey identifies a cached result
type CacheKey struct {
	QueryHash    string
	Parameters   string
	TableVersion int64
}

// CachePerformance tracks cache effectiveness
type CachePerformance struct {
	HitCount       int64
	MissCount      int64
	EvictionCount  int64
	AvgHitLatency  time.Duration
	AvgMissLatency time.Duration
	MemorySaved    int64
	ComputeSaved   time.Duration
	LastAccess     time.Time
}

// QueryHistoryTracker maintains query execution history
type QueryHistoryTracker struct {
	mu         sync.RWMutex
	history    []QueryExecution
	maxHistory int
	currentIdx int
}

// QueryExecution represents a single query execution
type QueryExecution struct {
	Signature     QuerySignature
	ExecutionTime time.Duration
	MemoryUsed    int64
	RowsProduced  int64
	PlanType      ExecutionMode
	Success       bool
	Timestamp     time.Time
}

// CostModelCalibrationData stores data for cost model calibration
type CostModelCalibrationData struct {
	mu                    sync.RWMutex
	scalarRowCostSamples  []float64
	vectorizedCostSamples []float64
	memoryOverheadSamples []float64
	lastCalibration       time.Time
	calibrationCount      int64
}

// NewRuntimeFeedbackCollector creates a new feedback collector
func NewRuntimeFeedbackCollector() *RuntimeFeedbackCollector {
	return &RuntimeFeedbackCollector{
		operatorPerformance: make(map[OperatorSignature]*PerformanceMetrics),
		memoryUsagePatterns: make(map[QuerySignature]*MemoryProfile),
		cacheEffectiveness:  make(map[CacheKey]*CachePerformance),
		queryHistory:        NewQueryHistoryTracker(10000),
		calibrationData:     NewCostModelCalibrationData(),
	}
}

// NewQueryHistoryTracker creates a new query history tracker
func NewQueryHistoryTracker(maxHistory int) *QueryHistoryTracker {
	return &QueryHistoryTracker{
		history:    make([]QueryExecution, maxHistory),
		maxHistory: maxHistory,
		currentIdx: 0,
	}
}

// NewCostModelCalibrationData creates new calibration data
func NewCostModelCalibrationData() *CostModelCalibrationData {
	return &CostModelCalibrationData{
		scalarRowCostSamples:  make([]float64, 0, 1000),
		vectorizedCostSamples: make([]float64, 0, 1000),
		memoryOverheadSamples: make([]float64, 0, 1000),
		lastCalibration:       time.Now(),
	}
}

// RecordOperatorMetrics records actual operator performance metrics
func (rfc *RuntimeFeedbackCollector) RecordOperatorMetrics(
	signature OperatorSignature,
	metrics *PerformanceMetrics,
) {
	rfc.mu.Lock()
	defer rfc.mu.Unlock()

	existing, exists := rfc.operatorPerformance[signature]
	if !exists {
		rfc.operatorPerformance[signature] = metrics
		metrics.SampleCount = 1
		metrics.LastUpdated = time.Now()
	} else {
		// Update with exponential moving average
		alpha := 0.3 // Weight for new sample

		existing.ActualExecutionTime = time.Duration(
			(1-alpha)*float64(existing.ActualExecutionTime) + alpha*float64(metrics.ActualExecutionTime))
		existing.MemoryUsed = int64(
			(1-alpha)*float64(existing.MemoryUsed) + alpha*float64(metrics.MemoryUsed))
		existing.RowsProcessed = int64(
			(1-alpha)*float64(existing.RowsProcessed) + alpha*float64(metrics.RowsProcessed))

		existing.VectorizationRatio = (1-alpha)*existing.VectorizationRatio + alpha*metrics.VectorizationRatio
		existing.CacheHitRate = (1-alpha)*existing.CacheHitRate + alpha*metrics.CacheHitRate

		existing.SampleCount++
		existing.LastUpdated = time.Now()
	}
}

// RecordQueryExecution records a complete query execution
func (rfc *RuntimeFeedbackCollector) RecordQueryExecution(
	signature QuerySignature,
	executionTime time.Duration,
	memoryUsed int64,
	rowsProduced int64,
	planType ExecutionMode,
	success bool,
) {
	// Record in history
	rfc.queryHistory.AddExecution(QueryExecution{
		Signature:     signature,
		ExecutionTime: executionTime,
		MemoryUsed:    memoryUsed,
		RowsProduced:  rowsProduced,
		PlanType:      planType,
		Success:       success,
		Timestamp:     time.Now(),
	})

	// Update memory profile
	rfc.updateMemoryProfile(signature, memoryUsed, success)

	// Update calibration data if vectorized
	if planType == ExecutionModeVectorized {
		rfc.updateCalibrationData(executionTime, memoryUsed, rowsProduced)
	}
}

// RecordCacheAccess records cache access patterns
func (rfc *RuntimeFeedbackCollector) RecordCacheAccess(
	key CacheKey,
	hit bool,
	latency time.Duration,
	memorySaved int64,
) {
	rfc.mu.Lock()
	defer rfc.mu.Unlock()

	perf, exists := rfc.cacheEffectiveness[key]
	if !exists {
		perf = &CachePerformance{}
		rfc.cacheEffectiveness[key] = perf
	}

	if hit {
		perf.HitCount++
		perf.AvgHitLatency = (perf.AvgHitLatency*time.Duration(perf.HitCount-1) + latency) /
			time.Duration(perf.HitCount)
		perf.MemorySaved += memorySaved
	} else {
		perf.MissCount++
		perf.AvgMissLatency = (perf.AvgMissLatency*time.Duration(perf.MissCount-1) + latency) /
			time.Duration(perf.MissCount)
	}

	perf.LastAccess = time.Now()
}

// GetOperatorPerformance retrieves performance metrics for an operator
func (rfc *RuntimeFeedbackCollector) GetOperatorPerformance(
	signature OperatorSignature,
) (*PerformanceMetrics, bool) {
	rfc.mu.RLock()
	defer rfc.mu.RUnlock()

	metrics, exists := rfc.operatorPerformance[signature]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	copy := *metrics
	return &copy, true
}

// GetMemoryProfile retrieves memory profile for a query pattern
func (rfc *RuntimeFeedbackCollector) GetMemoryProfile(
	signature QuerySignature,
) (*MemoryProfile, bool) {
	rfc.mu.RLock()
	defer rfc.mu.RUnlock()

	profile, exists := rfc.memoryUsagePatterns[signature]
	if !exists {
		return nil, false
	}

	copy := *profile
	return &copy, true
}

// GetCacheEffectiveness retrieves cache performance metrics
func (rfc *RuntimeFeedbackCollector) GetCacheEffectiveness(
	key CacheKey,
) (*CachePerformance, bool) {
	rfc.mu.RLock()
	defer rfc.mu.RUnlock()

	perf, exists := rfc.cacheEffectiveness[key]
	if !exists {
		return nil, false
	}

	copy := *perf
	return &copy, true
}

// UpdateCostModel updates the cost model based on collected feedback
func (rfc *RuntimeFeedbackCollector) UpdateCostModel(
	model *EnhancedVectorizedCostModel,
) error {
	rfc.mu.RLock()
	defer rfc.mu.RUnlock()

	// Calculate adjustment factors based on prediction accuracy
	scalarAdjustment := rfc.calculateScalarCostAdjustment()
	vectorizedAdjustment := rfc.calculateVectorizedCostAdjustment()

	// Apply adjustments with damping factor to avoid oscillation
	dampingFactor := 0.1

	model.scalarRowCost *= (1.0 + scalarAdjustment*dampingFactor)
	model.vectorizedRowCost *= (1.0 + vectorizedAdjustment*dampingFactor)

	// Update thresholds based on success rates
	rfc.updateVectorizationThresholds(model)

	return nil
}

// GetQueryHistory retrieves recent query execution history
func (rfc *RuntimeFeedbackCollector) GetQueryHistory(limit int) []QueryExecution {
	return rfc.queryHistory.GetRecent(limit)
}

// GetCalibrationStats returns cost model calibration statistics
func (rfc *RuntimeFeedbackCollector) GetCalibrationStats() map[string]interface{} {
	rfc.calibrationData.mu.RLock()
	defer rfc.calibrationData.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["calibration_count"] = rfc.calibrationData.calibrationCount
	stats["last_calibration"] = rfc.calibrationData.lastCalibration
	stats["scalar_samples"] = len(rfc.calibrationData.scalarRowCostSamples)
	stats["vectorized_samples"] = len(rfc.calibrationData.vectorizedCostSamples)

	return stats
}

// Private helper methods

func (rfc *RuntimeFeedbackCollector) updateMemoryProfile(
	signature QuerySignature,
	memoryUsed int64,
	success bool,
) {
	rfc.mu.Lock()
	defer rfc.mu.Unlock()

	profile, exists := rfc.memoryUsagePatterns[signature]
	if !exists {
		profile = &MemoryProfile{}
		rfc.memoryUsagePatterns[signature] = profile
	}

	if memoryUsed > profile.PeakMemoryUsage {
		profile.PeakMemoryUsage = memoryUsed
	}

	// Update average
	totalRuns := profile.SuccessfulRuns + profile.FailedRuns
	if totalRuns > 0 {
		profile.AvgMemoryUsage = (profile.AvgMemoryUsage*totalRuns + memoryUsed) / (totalRuns + 1)
	} else {
		profile.AvgMemoryUsage = memoryUsed
	}

	if success {
		profile.SuccessfulRuns++
	} else {
		profile.FailedRuns++
		profile.LastOOM = time.Now()
	}
}

func (rfc *RuntimeFeedbackCollector) updateCalibrationData(
	executionTime time.Duration,
	memoryUsed int64,
	rowsProcessed int64,
) {
	rfc.calibrationData.mu.Lock()
	defer rfc.calibrationData.mu.Unlock()

	if rowsProcessed > 0 {
		costPerRow := float64(executionTime.Nanoseconds()) / float64(rowsProcessed)
		rfc.calibrationData.vectorizedCostSamples = append(
			rfc.calibrationData.vectorizedCostSamples, costPerRow)

		// Keep only recent samples
		if len(rfc.calibrationData.vectorizedCostSamples) > 1000 {
			rfc.calibrationData.vectorizedCostSamples =
				rfc.calibrationData.vectorizedCostSamples[100:]
		}
	}

	rfc.calibrationData.calibrationCount++
}

func (rfc *RuntimeFeedbackCollector) calculateScalarCostAdjustment() float64 {
	// Calculate adjustment based on prediction accuracy
	totalError := 0.0
	count := 0

	for _, metrics := range rfc.operatorPerformance {
		if metrics.PredictedTime > 0 && metrics.SampleCount > 5 {
			error := (float64(metrics.ActualExecutionTime) - float64(metrics.PredictedTime)) /
				float64(metrics.PredictedTime)
			totalError += error
			count++
		}
	}

	if count == 0 {
		return 0.0
	}

	return totalError / float64(count)
}

func (rfc *RuntimeFeedbackCollector) calculateVectorizedCostAdjustment() float64 {
	// Similar to scalar but for vectorized operations
	totalError := 0.0
	count := 0

	for sig, metrics := range rfc.operatorPerformance {
		if sig.ExecutionMode == ExecutionModeVectorized &&
			metrics.PredictedTime > 0 && metrics.SampleCount > 5 {
			error := (float64(metrics.ActualExecutionTime) - float64(metrics.PredictedTime)) /
				float64(metrics.PredictedTime)
			totalError += error
			count++
		}
	}

	if count == 0 {
		return 0.0
	}

	return totalError / float64(count)
}

func (rfc *RuntimeFeedbackCollector) updateVectorizationThresholds(
	model *EnhancedVectorizedCostModel,
) {
	// Analyze success rates for different operators and cardinalities
	// This would update thresholds based on actual performance
	// For now, this is a placeholder
}

// QueryHistoryTracker methods

func (qht *QueryHistoryTracker) AddExecution(exec QueryExecution) {
	qht.mu.Lock()
	defer qht.mu.Unlock()

	qht.history[qht.currentIdx] = exec
	qht.currentIdx = (qht.currentIdx + 1) % qht.maxHistory
}

func (qht *QueryHistoryTracker) GetRecent(limit int) []QueryExecution {
	qht.mu.RLock()
	defer qht.mu.RUnlock()

	if limit > qht.maxHistory {
		limit = qht.maxHistory
	}

	result := make([]QueryExecution, 0, limit)

	// Start from most recent and work backwards
	for i := 0; i < limit; i++ {
		idx := (qht.currentIdx - i - 1 + qht.maxHistory) % qht.maxHistory
		if qht.history[idx].Timestamp.IsZero() {
			break
		}
		result = append(result, qht.history[idx])
	}

	return result
}
