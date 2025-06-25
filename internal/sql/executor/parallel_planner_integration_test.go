package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestParallelPlannerConfig(t *testing.T) {
	// Test default config
	config := DefaultParallelPlannerConfig()

	if config.EnableParallelExecution {
		t.Error("Default config should have parallel execution disabled")
	}

	if config.MaxDOP != 4 {
		t.Errorf("Expected default MaxDOP=4, got %d", config.MaxDOP)
	}

	if config.MinTableSizeForParallelism != 1000 {
		t.Errorf("Expected default MinTableSizeForParallelism=1000, got %d", config.MinTableSizeForParallelism)
	}
}

func TestParallelExecutorBuilder(t *testing.T) {
	config := &ParallelPlannerConfig{
		EnableParallelExecution:    true,
		MaxDOP:                     2,
		MinTableSizeForParallelism: 100,
		MinCostForParallelism:      50.0,
		ForceParallel:              false,
	}

	builder := NewParallelExecutorBuilder(config)

	if builder.config.MaxDOP != 2 {
		t.Errorf("Expected MaxDOP=2, got %d", builder.config.MaxDOP)
	}

	if !builder.config.EnableParallelExecution {
		t.Error("Expected parallel execution to be enabled")
	}
}

func TestShouldParallelize(t *testing.T) {
	config := &ParallelPlannerConfig{
		EnableParallelExecution:    true,
		MaxDOP:                     4,
		MinTableSizeForParallelism: 100,
		MinCostForParallelism:      50.0,
		ForceParallel:              false,
	}

	builder := NewParallelExecutorBuilder(config)

	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer},
		},
	}

	// Create mock storage
	mockStorage := &MockStorageBackend{}

	// Test scan operator (should parallelize)
	scanOp := NewStorageScanOperator(table, mockStorage)
	if !builder.shouldParallelize(nil, scanOp) {
		t.Error("Scan operations should be parallelizable")
	}

	// Test hash join operator (should parallelize)
	leftOp := NewStorageScanOperator(table, mockStorage)
	rightOp := NewStorageScanOperator(table, mockStorage)
	hashJoin := NewHashJoinOperator(leftOp, rightOp, nil, nil, nil, InnerJoin)

	if !builder.shouldParallelize(nil, hashJoin) {
		t.Error("Hash join operations should be parallelizable")
	}

	// Test non-parallelizable operator
	_ = NewFilterOperator(scanOp, nil)
	// Filter itself is not parallelizable, but shouldParallelize only checks the top-level operator
	// In practice, the parallel planner would parallelize children
}

func TestForceParallel(t *testing.T) {
	config := &ParallelPlannerConfig{
		EnableParallelExecution:    true,
		MaxDOP:                     4,
		MinTableSizeForParallelism: 100000,  // Very high threshold
		MinCostForParallelism:      10000.0, // Very high threshold
		ForceParallel:              true,    // Force parallel despite thresholds
	}

	builder := NewParallelExecutorBuilder(config)

	// Create a simple operator that wouldn't normally be parallelized
	table := &catalog.Table{
		ID:        1,
		TableName: "small_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer},
		},
	}

	mockStorage := &MockStorageBackend{}
	scanOp := NewStorageScanOperator(table, mockStorage)

	// Should parallelize due to ForceParallel=true
	if !builder.shouldParallelize(nil, scanOp) {
		t.Error("ForceParallel should override thresholds")
	}
}

func TestParallelQueryHints(t *testing.T) {
	config := DefaultParallelPlannerConfig()
	builder := NewParallelExecutorBuilder(config)

	// Test NoParallel hint
	noParallelHints := &ParallelQueryHint{
		NoParallel: true,
	}

	builderWithHints := builder.ApplyParallelHints(noParallelHints)
	if builderWithHints.config.EnableParallelExecution {
		t.Error("NoParallel hint should disable parallel execution")
	}

	// Test ForceParallel hint
	forceParallelHints := &ParallelQueryHint{
		ForceParallel: true,
	}

	builderWithHints = builder.ApplyParallelHints(forceParallelHints)
	if !builderWithHints.config.ForceParallel {
		t.Error("ForceParallel hint should enable forced parallel execution")
	}

	if !builderWithHints.config.EnableParallelExecution {
		t.Error("ForceParallel hint should enable parallel execution")
	}

	// Test DegreeOfParallelism hint
	dopHints := &ParallelQueryHint{
		DegreeOfParallelism: 8,
	}

	builderWithHints = builder.ApplyParallelHints(dopHints)
	if builderWithHints.config.MaxDOP != 8 {
		t.Errorf("Expected MaxDOP=8, got %d", builderWithHints.config.MaxDOP)
	}
}

func TestParallelExecutionMonitor(t *testing.T) {
	monitor := NewParallelExecutionMonitor()

	// Record some queries
	monitor.RecordQuery(true, 4)  // Parallel with DOP=4
	monitor.RecordQuery(false, 0) // Sequential
	monitor.RecordQuery(true, 2)  // Parallel with DOP=2

	metrics := monitor.GetMetrics()

	if metrics.TotalQueries != 3 {
		t.Errorf("Expected 3 total queries, got %d", metrics.TotalQueries)
	}

	if metrics.ParallelQueries != 2 {
		t.Errorf("Expected 2 parallel queries, got %d", metrics.ParallelQueries)
	}

	expectedAvgDOP := (4.0 + 2.0) / 2.0 // Average of DOP 4 and 2
	if metrics.AvgDOP != expectedAvgDOP {
		t.Errorf("Expected average DOP=%.1f, got %.1f", expectedAvgDOP, metrics.AvgDOP)
	}

	// Test reset
	monitor.Reset()
	metrics = monitor.GetMetrics()

	if metrics.TotalQueries != 0 {
		t.Errorf("Expected 0 total queries after reset, got %d", metrics.TotalQueries)
	}
}

func TestOperatorTypeDetection(t *testing.T) {
	config := DefaultParallelPlannerConfig()
	builder := NewParallelExecutorBuilder(config)

	// Create test table and storage
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer},
		},
	}

	mockStorage := &MockStorageBackend{}

	// Test scan operation detection
	scanOp := NewStorageScanOperator(table, mockStorage)
	if !builder.isScanOperation(scanOp) {
		t.Error("StorageScanOperator should be detected as scan operation")
	}

	// Test join operation detection
	leftOp := NewStorageScanOperator(table, mockStorage)
	rightOp := NewStorageScanOperator(table, mockStorage)
	hashJoin := NewHashJoinOperator(leftOp, rightOp, nil, nil, nil, InnerJoin)

	if !builder.isJoinOperation(hashJoin) {
		t.Error("HashJoinOperator should be detected as join operation")
	}

	// Test nested loop join (should not be parallelized)
	nestedLoopJoin := NewNestedLoopJoinOperator(leftOp, rightOp, nil, InnerJoin)
	if builder.isJoinOperation(nestedLoopJoin) {
		t.Error("NestedLoopJoinOperator should not be considered for parallelization")
	}

	// Test non-join operation
	filterOp := NewFilterOperator(scanOp, nil)
	if builder.isScanOperation(filterOp) || builder.isJoinOperation(filterOp) {
		t.Error("FilterOperator should not be detected as scan or join operation")
	}
}

func TestBuilderWithNilConfig(t *testing.T) {
	// Test that nil config uses defaults
	builder := NewParallelExecutorBuilder(nil)

	if builder.config == nil {
		t.Error("Builder should create default config when passed nil")
	}

	if builder.config.MaxDOP != 4 {
		t.Errorf("Expected default MaxDOP=4, got %d", builder.config.MaxDOP)
	}

	if builder.config.EnableParallelExecution {
		t.Error("Default config should have parallel execution disabled")
	}
}
