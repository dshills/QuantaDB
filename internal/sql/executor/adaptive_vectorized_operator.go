package executor

import (
	"fmt"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/storage"
)

// VectorizedConfig holds configuration for vectorized execution
type VectorizedConfig struct {
	EnableVectorizedExecution   bool
	VectorizedBatchSize        int
	VectorizedMemoryLimit      int64
	VectorizedFallbackThreshold int
}

// DefaultVectorizedConfig returns default configuration
func DefaultVectorizedConfig() *VectorizedConfig {
	return &VectorizedConfig{
		EnableVectorizedExecution:   true,
		VectorizedBatchSize:        1024,
		VectorizedMemoryLimit:      64 * 1024 * 1024, // 64MB
		VectorizedFallbackThreshold: 100,
	}
}

// AdaptiveVectorizedOperator wraps operators with intelligent vectorized/scalar fallback
type AdaptiveVectorizedOperator struct {
	baseOperator

	// Child operators
	vectorizedImpl Operator // Vectorized implementation
	scalarImpl     Operator // Scalar fallback implementation
	currentImpl    Operator // Currently active implementation

	// Configuration
	config *VectorizedConfig

	// Adaptive state
	useVectorized    bool
	fallbackReason   string
	switchCount      int
	lastSwitchTime   time.Time

	// Performance tracking
	vectorizedStats *RuntimeStats
	scalarStats     *RuntimeStats
	
	// Memory monitoring
	memoryUsage     int64
	memoryThreshold int64
	memoryManager   *storage.MemoryManager
}

// NewAdaptiveVectorizedScanOperator creates an adaptive scan operator
func NewAdaptiveVectorizedScanOperator(
	table *catalog.Table,
	storage StorageBackend,
	predicate ExprEvaluator,
	config *VectorizedConfig,
	memoryManager *storage.MemoryManager,
) *AdaptiveVectorizedOperator {
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name: col.Name,
			Type: col.DataType,
		}
	}

	// Create both implementations
	vectorizedImpl := NewVectorizedScanOperator(table, storage, predicate)
	scalarImpl := NewStorageScanOperator(table, "", storage)
	if predicate != nil {
		scalarImpl = NewFilterOperator(scalarImpl, predicate)
	}

	return &AdaptiveVectorizedOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		vectorizedImpl:  vectorizedImpl,
		scalarImpl:      scalarImpl,
		config:          config,
		vectorizedStats: NewRuntimeStats(1000),
		scalarStats:     NewRuntimeStats(1000),
		memoryThreshold: config.VectorizedMemoryLimit,
		memoryManager:   memoryManager,
	}
}

// NewAdaptiveVectorizedFilterOperator creates an adaptive filter operator  
func NewAdaptiveVectorizedFilterOperator(
	child Operator,
	predicate ExprEvaluator,
	config *VectorizedConfig,
	memoryManager *storage.MemoryManager,
) *AdaptiveVectorizedOperator {
	// Create both implementations
	vectorizedImpl := NewVectorizedFilterOperator(child, predicate)
	scalarImpl := NewFilterOperator(child, predicate)

	return &AdaptiveVectorizedOperator{
		baseOperator: baseOperator{
			schema: child.Schema(),
		},
		vectorizedImpl:  vectorizedImpl,
		scalarImpl:      scalarImpl,
		config:          config,
		vectorizedStats: NewRuntimeStats(1000),
		scalarStats:     NewRuntimeStats(1000),
		memoryThreshold: config.VectorizedMemoryLimit,
		memoryManager:   memoryManager,
	}
}

// Open initializes the adaptive operator
func (avo *AdaptiveVectorizedOperator) Open(ctx *ExecContext) error {
	avo.ctx = ctx
	avo.initStats(1000)

	// Choose initial implementation
	if err := avo.chooseInitialImplementation(); err != nil {
		return fmt.Errorf("failed to choose initial implementation: %w", err)
	}

	// Open the chosen implementation
	return avo.currentImpl.Open(ctx)
}

// chooseInitialImplementation decides whether to use vectorized or scalar execution
func (avo *AdaptiveVectorizedOperator) chooseInitialImplementation() error {
	// Check if vectorized execution is enabled
	if !avo.config.EnableVectorizedExecution {
		avo.useVectorized = false
		avo.fallbackReason = "Vectorized execution disabled in configuration"
		avo.currentImpl = avo.scalarImpl
		return nil
	}

	// Check memory availability using memory manager if available
	estimatedMemory := avo.estimateVectorizedMemoryUsage()
	availableMemory := avo.memoryThreshold
	
	if avo.memoryManager != nil {
		availableMemory = avo.memoryManager.GetAvailableMemory(storage.SubsystemVectorized)
	}
	
	if estimatedMemory > availableMemory {
		avo.useVectorized = false
		avo.fallbackReason = "Insufficient memory for vectorized execution"
		avo.currentImpl = avo.scalarImpl
		return nil
	}

	// Default to vectorized if available
	avo.useVectorized = true
	avo.fallbackReason = "Vectorized execution chosen for performance"
	avo.currentImpl = avo.vectorizedImpl

	return nil
}

// Next returns the next row with adaptive fallback support
func (avo *AdaptiveVectorizedOperator) Next() (*Row, error) {
	// Check if we should consider switching implementations
	if avo.shouldConsiderSwitch() {
		if err := avo.evaluateSwitching(); err != nil {
			// Log error but continue with current implementation
			fmt.Printf("Warning: failed to evaluate switching: %v\n", err)
		}
	}

	// Get next row from current implementation
	row, err := avo.currentImpl.Next()
	
	// Handle vectorized-specific errors with fallback
	if err != nil && avo.useVectorized {
		if avo.isVectorizedError(err) {
			// Attempt fallback to scalar implementation
			if fallbackErr := avo.fallbackToScalar(err); fallbackErr != nil {
				return nil, fmt.Errorf("vectorized execution failed and fallback failed: %v, fallback error: %v", err, fallbackErr)
			}
			// Retry with scalar implementation
			return avo.currentImpl.Next()
		}
	}

	if err != nil {
		return nil, err
	}

	// Update statistics
	avo.updateStats(row)
	avo.recordRow()

	return row, nil
}

// shouldConsiderSwitch determines if we should evaluate switching implementations
func (avo *AdaptiveVectorizedOperator) shouldConsiderSwitch() bool {
	// Don't switch too frequently
	if time.Since(avo.lastSwitchTime) < 5*time.Second {
		return false
	}

	// Don't switch if we've already switched too many times
	if avo.switchCount >= 3 {
		return false
	}

	return true
}

// evaluateSwitching checks if we should switch between vectorized and scalar execution
func (avo *AdaptiveVectorizedOperator) evaluateSwitching() error {
	currentStats := avo.getCurrentStats()
	
	// Check memory pressure using memory manager if available
	isMemoryPressure := false
	if avo.memoryManager != nil {
		isMemoryPressure = avo.memoryManager.IsMemoryPressure()
	} else {
		// Fallback to local memory threshold check
		isMemoryPressure = avo.memoryUsage > avo.memoryThreshold
	}
	
	if isMemoryPressure && avo.useVectorized {
		return avo.switchToScalar("Memory pressure detected")
	}

	// Check performance degradation
	if avo.useVectorized && avo.isPerformancePoor(currentStats) {
		return avo.switchToScalar("Poor vectorized performance detected")
	}

	// Check if scalar is struggling and vectorization might help
	if !avo.useVectorized && avo.shouldTryVectorized(currentStats) {
		return avo.switchToVectorized("Scalar performance poor, trying vectorized")
	}

	return nil
}

// isVectorizedError determines if an error is specific to vectorized execution
func (avo *AdaptiveVectorizedOperator) isVectorizedError(err error) bool {
	// Check for common vectorized execution errors
	errorString := err.Error()
	
	vectorizedErrors := []string{
		"unsupported vectorized operator",
		"vectorized batch overflow",
		"vectorized memory allocation failed",
		"unsupported data type for vectorization",
		"vectorized null bitmap error",
	}

	for _, vecErr := range vectorizedErrors {
		if contains(errorString, vecErr) {
			return true
		}
	}

	return false
}

// fallbackToScalar switches to scalar implementation due to error
func (avo *AdaptiveVectorizedOperator) fallbackToScalar(originalError error) error {
	avo.fallbackReason = fmt.Sprintf("Fallback due to error: %v", originalError)
	
	// Close current vectorized implementation
	if avo.currentImpl != nil {
		avo.currentImpl.Close()
	}

	// Switch to scalar
	avo.useVectorized = false
	avo.currentImpl = avo.scalarImpl
	avo.switchCount++
	avo.lastSwitchTime = time.Now()

	// Open scalar implementation
	return avo.scalarImpl.Open(avo.ctx)
}

// switchToScalar switches to scalar implementation for performance reasons
func (avo *AdaptiveVectorizedOperator) switchToScalar(reason string) error {
	if !avo.useVectorized {
		return nil // Already using scalar
	}

	avo.fallbackReason = reason
	
	// Close current implementation
	if avo.currentImpl != nil {
		avo.currentImpl.Close()
	}

	// Switch to scalar
	avo.useVectorized = false
	avo.currentImpl = avo.scalarImpl
	avo.switchCount++
	avo.lastSwitchTime = time.Now()

	// Open scalar implementation
	return avo.scalarImpl.Open(avo.ctx)
}

// switchToVectorized switches to vectorized implementation
func (avo *AdaptiveVectorizedOperator) switchToVectorized(reason string) error {
	if avo.useVectorized {
		return nil // Already using vectorized
	}

	// Check if vectorized execution is still enabled
	if !avo.config.EnableVectorizedExecution {
		return nil
	}

	// Check memory availability
	if avo.estimateVectorizedMemoryUsage() > avo.memoryThreshold {
		return nil
	}

	avo.fallbackReason = reason

	// Close current implementation
	if avo.currentImpl != nil {
		avo.currentImpl.Close()
	}

	// Switch to vectorized
	avo.useVectorized = true
	avo.currentImpl = avo.vectorizedImpl
	avo.switchCount++
	avo.lastSwitchTime = time.Now()

	// Open vectorized implementation
	return avo.vectorizedImpl.Open(avo.ctx)
}

// getCurrentStats returns current performance statistics
func (avo *AdaptiveVectorizedOperator) getCurrentStats() *RuntimeStats {
	if avo.useVectorized {
		return avo.vectorizedStats
	}
	return avo.scalarStats
}

// isPerformancePoor checks if current performance is significantly poor
func (avo *AdaptiveVectorizedOperator) isPerformancePoor(stats *RuntimeStats) bool {
	// Check if rows per second is below threshold
	if stats.RowsPerSecond > 0 && stats.RowsPerSecond < 1000 {
		return true
	}

	// Check if memory usage is excessive
	if avo.memoryUsage > avo.memoryThreshold*2 {
		return true
	}

	return false
}

// shouldTryVectorized checks if we should try switching to vectorized execution
func (avo *AdaptiveVectorizedOperator) shouldTryVectorized(stats *RuntimeStats) bool {
	// Only try if scalar performance is poor and we have enough data
	return stats.ActualRows > 1000 && avo.isPerformancePoor(stats)
}

// updateStats updates performance statistics
func (avo *AdaptiveVectorizedOperator) updateStats(row *Row) {
	if avo.useVectorized {
		avo.vectorizedStats.UpdateCardinality(avo.vectorizedStats.ActualRows + 1)
	} else {
		avo.scalarStats.UpdateCardinality(avo.scalarStats.ActualRows + 1)
	}

	// Update memory usage (simplified estimation)
	if row != nil {
		avo.memoryUsage += 128 // Estimated row size
		
		// Update memory manager with vectorized operator usage
		if avo.memoryManager != nil {
			avo.memoryManager.UpdateSubsystemUsage(storage.SubsystemVectorized, avo.memoryUsage)
		}
	}
}

// estimateVectorizedMemoryUsage estimates memory needed for vectorized execution
func (avo *AdaptiveVectorizedOperator) estimateVectorizedMemoryUsage() int64 {
	batchSize := int64(avo.config.VectorizedBatchSize)
	avgRowSize := int64(128) // Estimated average row size
	
	// Memory for input batch + output batch + null bitmaps + overhead
	return batchSize * avgRowSize * 3
}

// Close cleans up the adaptive operator
func (avo *AdaptiveVectorizedOperator) Close() error {
	// Finalize stats
	avo.finishStats()
	if avo.ctx != nil && avo.ctx.StatsCollector != nil && avo.stats != nil {
		avo.ctx.StatsCollector(avo, avo.stats)
	}

	// Close current implementation
	if avo.currentImpl != nil {
		return avo.currentImpl.Close()
	}

	return nil
}

// GetAdaptiveStats returns adaptive execution statistics
func (avo *AdaptiveVectorizedOperator) GetAdaptiveStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	stats["use_vectorized"] = avo.useVectorized
	stats["fallback_reason"] = avo.fallbackReason
	stats["switch_count"] = avo.switchCount
	stats["memory_usage"] = avo.memoryUsage
	stats["memory_threshold"] = avo.memoryThreshold
	
	if avo.useVectorized {
		stats["vectorized_rows"] = avo.vectorizedStats.ActualRows
		stats["vectorized_rows_per_second"] = avo.vectorizedStats.RowsPerSecond
	} else {
		stats["scalar_rows"] = avo.scalarStats.ActualRows
		stats["scalar_rows_per_second"] = avo.scalarStats.RowsPerSecond
	}
	
	return stats
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Helper methods for adaptive execution already provided by existing RuntimeStats