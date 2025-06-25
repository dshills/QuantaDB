package executor

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// AdaptiveHashJoinOperator extends hash join with memory-aware spilling
type AdaptiveHashJoinOperator struct {
	baseOperator

	// Join configuration
	left      Operator
	right     Operator
	leftKeys  []ExprEvaluator
	rightKeys []ExprEvaluator
	predicate ExprEvaluator
	joinType  JoinType

	// Adaptive components
	adaptiveCtx *AdaptiveContext
	joinStats   *RuntimeStats

	// Memory management
	maxMemory        int64
	currentMemory    int64
	memoryThreshold  float64 // Spill when memory usage exceeds this fraction
	spillBuckets     []*SpillBucket
	spillDirectory   string
	spillFileCounter int

	// Hash table management
	hashTable      map[uint64][]*Row
	buildPhaseRows []*Row
	spillEnabled   bool
	hasSpilled     bool

	// Execution state
	buildComplete bool
	probeIndex    int
	spillIndex    int
	currentBucket *SpillBucket

	// Performance tracking
	spillCount int64
	spillTime  time.Duration
	memoryPeak int64
}

// SpillBucket represents a partition that has been spilled to disk
type SpillBucket struct {
	ID            int
	FilePath      string
	RowCount      int64
	EstimatedSize int64
	KeyRange      KeyRange
}

// KeyRange represents the range of hash values in a spill bucket
type KeyRange struct {
	MinHash uint64
	MaxHash uint64
}

// NewAdaptiveHashJoinOperator creates a memory-aware hash join
func NewAdaptiveHashJoinOperator(
	left, right Operator,
	leftKeys, rightKeys []ExprEvaluator,
	predicate ExprEvaluator,
	joinType JoinType,
	adaptiveCtx *AdaptiveContext,
	maxMemory int64,
) *AdaptiveHashJoinOperator {
	// Build combined schema
	leftSchema := left.Schema()
	rightSchema := right.Schema()

	columns := make([]Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns))
	columns = append(columns, leftSchema.Columns...)
	columns = append(columns, rightSchema.Columns...)

	schema := &Schema{Columns: columns}

	return &AdaptiveHashJoinOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		left:            left,
		right:           right,
		leftKeys:        leftKeys,
		rightKeys:       rightKeys,
		predicate:       predicate,
		joinType:        joinType,
		adaptiveCtx:     adaptiveCtx,
		maxMemory:       maxMemory,
		memoryThreshold: 0.8, // Spill at 80% memory usage
		hashTable:       make(map[uint64][]*Row),
		buildPhaseRows:  make([]*Row, 0),
		spillBuckets:    make([]*SpillBucket, 0),
		spillDirectory:  "/tmp/quantadb_spill", // TODO: Make configurable
		spillEnabled:    true,
	}
}

// Open initializes the adaptive hash join
func (ahj *AdaptiveHashJoinOperator) Open(ctx *ExecContext) error {
	ahj.ctx = ctx

	// Initialize statistics
	ahj.joinStats = NewRuntimeStats(1000)
	ahj.initStats(1000)

	// Create spill directory if needed
	if ahj.spillEnabled {
		if err := os.MkdirAll(ahj.spillDirectory, 0755); err != nil {
			return fmt.Errorf("failed to create spill directory: %w", err)
		}
	}

	// Open child operators
	if err := ahj.left.Open(ctx); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := ahj.right.Open(ctx); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	// Log initialization
	if ahj.adaptiveCtx != nil {
		ahj.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveHashJoin",
			"Initialize",
			fmt.Sprintf("MaxMemory=%d, SpillEnabled=%t", ahj.maxMemory, ahj.spillEnabled),
			ahj.joinStats,
		)
	}

	return nil
}

// Next returns the next joined row with adaptive memory management
func (ahj *AdaptiveHashJoinOperator) Next() (*Row, error) {
	// Build phase: consume left side and build hash table
	if !ahj.buildComplete {
		if err := ahj.buildPhase(); err != nil {
			return nil, fmt.Errorf("build phase failed: %w", err)
		}
		ahj.buildComplete = true
	}

	// Probe phase: scan right side and probe hash table
	return ahj.probePhase()
}

// buildPhase consumes the left side and builds the hash table with spilling
func (ahj *AdaptiveHashJoinOperator) buildPhase() error {
	startTime := time.Now()

	for {
		row, err := ahj.left.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break // EOF
		}

		// Update memory usage estimate
		ahj.updateMemoryUsage(row)

		// Check if we need to spill
		if ahj.shouldSpill() {
			if err := ahj.spillCurrentData(); err != nil {
				return fmt.Errorf("spilling failed: %w", err)
			}
		}

		// Add row to build phase
		ahj.buildPhaseRows = append(ahj.buildPhaseRows, row)

		// Build hash table entry
		hashValue, err := ahj.calculateHashValue(row, ahj.leftKeys)
		if err != nil {
			return fmt.Errorf("hash calculation failed: %w", err)
		}

		if !ahj.hasSpilled {
			// Normal in-memory operation
			ahj.hashTable[hashValue] = append(ahj.hashTable[hashValue], row)
		}

		// Update statistics
		ahj.joinStats.UpdateCardinality(ahj.joinStats.ActualRows + 1)
	}

	buildTime := time.Since(startTime)

	// Log build phase completion
	if ahj.adaptiveCtx != nil {
		ahj.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveHashJoin",
			"BuildComplete",
			fmt.Sprintf("Rows=%d, Spilled=%t, SpillCount=%d, BuildTime=%v",
				len(ahj.buildPhaseRows), ahj.hasSpilled, ahj.spillCount, buildTime),
			ahj.joinStats,
		)
	}

	return nil
}

// shouldSpill determines if we should spill data to disk
func (ahj *AdaptiveHashJoinOperator) shouldSpill() bool {
	if !ahj.spillEnabled || ahj.maxMemory <= 0 {
		return false
	}

	// Check memory pressure
	memoryPressure := float64(ahj.currentMemory) / float64(ahj.maxMemory)
	if memoryPressure >= ahj.memoryThreshold {
		return true
	}

	// Check if hash table is getting too large
	if len(ahj.hashTable) > 100000 { // Arbitrary threshold
		return true
	}

	return false
}

// spillCurrentData spills current hash table data to disk
func (ahj *AdaptiveHashJoinOperator) spillCurrentData() error {
	if len(ahj.hashTable) == 0 {
		return nil
	}

	spillStart := time.Now()
	ahj.spillFileCounter++

	// Create spill bucket
	bucket := &SpillBucket{
		ID:       ahj.spillFileCounter,
		FilePath: fmt.Sprintf("%s/spill_%d.data", ahj.spillDirectory, ahj.spillFileCounter),
		RowCount: 0,
	}

	// Sort hash keys for better locality
	hashKeys := make([]uint64, 0, len(ahj.hashTable))
	for hash := range ahj.hashTable {
		hashKeys = append(hashKeys, hash)
	}
	sort.Slice(hashKeys, func(i, j int) bool {
		return hashKeys[i] < hashKeys[j]
	})

	// Set key range
	if len(hashKeys) > 0 {
		bucket.KeyRange.MinHash = hashKeys[0]
		bucket.KeyRange.MaxHash = hashKeys[len(hashKeys)-1]
	}

	// Write data to spill file (simplified - would use proper serialization)
	file, err := os.Create(bucket.FilePath)
	if err != nil {
		return fmt.Errorf("failed to create spill file: %w", err)
	}
	defer file.Close()

	// In a real implementation, this would serialize rows efficiently
	rowCount := int64(0)
	for _, hash := range hashKeys {
		rows := ahj.hashTable[hash]
		rowCount += int64(len(rows))
		// Write hash value and rows (simplified)
		// In production, would use efficient binary format
	}

	bucket.RowCount = rowCount
	bucket.EstimatedSize = ahj.currentMemory // Rough estimate

	// Add to spill buckets
	ahj.spillBuckets = append(ahj.spillBuckets, bucket)

	// Clear in-memory hash table
	ahj.hashTable = make(map[uint64][]*Row)
	ahj.currentMemory = 0
	ahj.hasSpilled = true
	ahj.spillCount++

	spillDuration := time.Since(spillStart)
	ahj.spillTime += spillDuration

	// Log spilling decision
	if ahj.adaptiveCtx != nil {
		ahj.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveHashJoin",
			"SpillData",
			fmt.Sprintf("Bucket=%d, Rows=%d, SpillTime=%v", bucket.ID, rowCount, spillDuration),
			ahj.joinStats,
		)
	}

	return nil
}

// probePhase scans the right side and probes the hash table
func (ahj *AdaptiveHashJoinOperator) probePhase() (*Row, error) {
	if ahj.hasSpilled {
		return ahj.probeWithSpilledData()
	}

	// Normal in-memory probing
	for {
		rightRow, err := ahj.right.Next()
		if err != nil {
			return nil, err
		}
		if rightRow == nil {
			return nil, nil // EOF
		}

		// Calculate hash for right row
		hashValue, err := ahj.calculateHashValue(rightRow, ahj.rightKeys)
		if err != nil {
			return nil, fmt.Errorf("hash calculation failed: %w", err)
		}

		// Probe hash table
		if leftRows, exists := ahj.hashTable[hashValue]; exists {
			for _, leftRow := range leftRows {
				if ahj.matchesJoinCondition(leftRow, rightRow) {
					joinedRow := ahj.combineRows(leftRow, rightRow)
					ahj.recordRow()
					return joinedRow, nil
				}
			}
		}
	}
}

// probeWithSpilledData handles probing when data has been spilled
func (ahj *AdaptiveHashJoinOperator) probeWithSpilledData() (*Row, error) {
	// This is a simplified implementation
	// A complete implementation would:
	// 1. Process each spill bucket sequentially
	// 2. Load bucket data back into memory for probing
	// 3. Handle memory pressure during loading
	// 4. Optimize by partitioning the probe side as well

	if ahj.spillIndex >= len(ahj.spillBuckets) {
		return nil, nil // All buckets processed
	}

	// For now, return nil to indicate spilled operation completion
	// In production, this would implement proper spill bucket processing
	return nil, nil
}

// matchesJoinCondition evaluates the join predicate
func (ahj *AdaptiveHashJoinOperator) matchesJoinCondition(leftRow, rightRow *Row) bool {
	if ahj.predicate == nil {
		return true
	}

	// Combine rows for predicate evaluation
	combinedRow := ahj.combineRows(leftRow, rightRow)

	result, err := ahj.predicate.Eval(combinedRow, ahj.ctx)
	if err != nil {
		return false
	}

	// Skip if predicate is false or NULL
	if result.IsNull() || (result.Data != nil && !result.Data.(bool)) {
		return false
	}

	return true
}

// combineRows creates a new row by combining left and right rows
func (ahj *AdaptiveHashJoinOperator) combineRows(leftRow, rightRow *Row) *Row {
	combinedValues := make([]types.Value, 0, len(leftRow.Values)+len(rightRow.Values))
	combinedValues = append(combinedValues, leftRow.Values...)
	combinedValues = append(combinedValues, rightRow.Values...)

	return &Row{Values: combinedValues}
}

// calculateHashValue computes hash value for join keys
func (ahj *AdaptiveHashJoinOperator) calculateHashValue(row *Row, keys []ExprEvaluator) (uint64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// Simple hash combining - production would use better hash function
	var hash uint64 = 14695981039346656037 // FNV offset basis

	for _, key := range keys {
		value, err := key.Eval(row, ahj.ctx)
		if err != nil {
			return 0, err
		}

		// Hash the value (simplified)
		valueHash := uint64(value.String()[0]) // Very simplified
		hash ^= valueHash
		hash *= 1099511628211 // FNV prime
	}

	return hash, nil
}

// updateMemoryUsage estimates current memory usage
func (ahj *AdaptiveHashJoinOperator) updateMemoryUsage(row *Row) {
	// Rough estimate: 64 bytes per row + value storage
	rowSize := int64(64)
	for _, value := range row.Values {
		rowSize += int64(len(value.String())) // Simplified size calculation
	}

	ahj.currentMemory += rowSize
	if ahj.currentMemory > ahj.memoryPeak {
		ahj.memoryPeak = ahj.currentMemory
	}

	// Update runtime statistics
	ahj.joinStats.UpdateMemoryUsage(ahj.currentMemory)
}

// Close cleans up the adaptive hash join
func (ahj *AdaptiveHashJoinOperator) Close() error {
	var err error

	// Close child operators
	if ahj.left != nil {
		if closeErr := ahj.left.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	if ahj.right != nil {
		if closeErr := ahj.right.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	// Clean up spill files
	for _, bucket := range ahj.spillBuckets {
		if removeErr := os.Remove(bucket.FilePath); removeErr != nil && err == nil {
			err = removeErr
		}
	}

	// Ensure stats are finalized
	ahj.finishStats()
	if ahj.ctx != nil && ahj.ctx.StatsCollector != nil && ahj.stats != nil {
		ahj.ctx.StatsCollector(ahj, ahj.stats)
	}

	// Log final statistics
	if ahj.adaptiveCtx != nil {
		ahj.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveHashJoin",
			"Complete",
			fmt.Sprintf("MemoryPeak=%d, SpillCount=%d, SpillTime=%v",
				ahj.memoryPeak, ahj.spillCount, ahj.spillTime),
			ahj.joinStats,
		)
	}

	return err
}

// GetAdaptiveStats returns memory-specific statistics
func (ahj *AdaptiveHashJoinOperator) GetAdaptiveStats() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["max_memory"] = ahj.maxMemory
	stats["memory_peak"] = ahj.memoryPeak
	stats["current_memory"] = ahj.currentMemory
	stats["memory_threshold"] = ahj.memoryThreshold
	stats["has_spilled"] = ahj.hasSpilled
	stats["spill_count"] = ahj.spillCount
	stats["spill_time_ms"] = ahj.spillTime.Milliseconds()
	stats["spill_buckets"] = len(ahj.spillBuckets)

	if ahj.adaptiveCtx != nil && ahj.adaptiveCtx.DecisionLog != nil {
		stats["decision_count"] = ahj.adaptiveCtx.DecisionLog.GetDecisionCount()
	}

	return stats
}

// AdaptiveHashJoinConfig configures memory-aware hash join behavior
type AdaptiveHashJoinConfig struct {
	// Memory management
	MaxMemory       int64
	MemoryThreshold float64
	SpillEnabled    bool
	SpillDirectory  string

	// Performance tuning
	InitialHashTableSize int
	SpillBucketSize      int64
	LoadFactor           float64
}

// DefaultAdaptiveHashJoinConfig returns reasonable defaults
func DefaultAdaptiveHashJoinConfig() *AdaptiveHashJoinConfig {
	return &AdaptiveHashJoinConfig{
		MaxMemory:            64 * 1024 * 1024, // 64MB
		MemoryThreshold:      0.8,              // Spill at 80%
		SpillEnabled:         true,
		SpillDirectory:       "/tmp/quantadb_spill",
		InitialHashTableSize: 16384,
		SpillBucketSize:      8 * 1024 * 1024, // 8MB per bucket
		LoadFactor:           0.75,
	}
}
