package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestRuntimeStats(t *testing.T) {
	stats := NewRuntimeStats(1000)
	
	// Test initial state
	if stats.EstimatedRows != 1000 {
		t.Errorf("Expected EstimatedRows=1000, got %d", stats.EstimatedRows)
	}
	
	if stats.ActualRows != 0 {
		t.Errorf("Expected ActualRows=0, got %d", stats.ActualRows)
	}
	
	// Test cardinality updates
	stats.UpdateCardinality(500)
	if stats.ActualRows != 500 {
		t.Errorf("Expected ActualRows=500, got %d", stats.ActualRows)
	}
	
	// Test cardinality error calculation
	error := stats.GetCardinalityError()
	expected := 0.5 // 50% error (500 vs 1000)
	if error != expected {
		t.Errorf("Expected cardinality error=%.2f, got %.2f", expected, error)
	}
	
	// Test accuracy check
	if stats.IsEstimateAccurate(0.3) {
		t.Error("Should not be accurate with 50% error and 30% threshold")
	}
	
	if !stats.IsEstimateAccurate(0.6) {
		t.Error("Should be accurate with 50% error and 60% threshold")
	}
}

func TestRuntimeStatsMemoryPressure(t *testing.T) {
	stats := NewRuntimeStats(1000)
	
	// Test memory pressure detection
	maxMemory := int64(1000)
	
	stats.UpdateMemoryUsage(500)
	if stats.IsMemoryPressure(maxMemory, 0.8) {
		t.Error("Should not detect memory pressure at 50% usage with 80% threshold")
	}
	
	stats.UpdateMemoryUsage(900)
	if !stats.IsMemoryPressure(maxMemory, 0.8) {
		t.Error("Should detect memory pressure at 90% usage with 80% threshold")
	}
	
	// Test peak memory tracking
	if stats.MemoryPeak != 900 {
		t.Errorf("Expected MemoryPeak=900, got %d", stats.MemoryPeak)
	}
	
	stats.UpdateMemoryUsage(800) // Lower usage
	if stats.MemoryPeak != 900 {
		t.Errorf("Peak should remain 900, got %d", stats.MemoryPeak)
	}
}

func TestRuntimeStatsDataSkew(t *testing.T) {
	stats := NewRuntimeStats(1000)
	
	// Test uniform distribution (no skew)
	uniformValues := []float64{100, 100, 100, 100, 100}
	stats.UpdateDataSkew(uniformValues)
	
	if stats.DataSkew != 0.0 {
		t.Errorf("Expected no skew for uniform distribution, got %.2f", stats.DataSkew)
	}
	
	if stats.IsDataSkewed(0.5) {
		t.Error("Uniform distribution should not be considered skewed")
	}
	
	// Test skewed distribution
	skewedValues := []float64{10, 10, 10, 10, 1000}
	stats.UpdateDataSkew(skewedValues)
	
	if stats.DataSkew == 0.0 {
		t.Error("Expected non-zero skew for skewed distribution")
	}
	
	if !stats.IsDataSkewed(0.5) {
		t.Error("Skewed distribution should be detected")
	}
}

func TestRuntimeStatsWorkerUtilization(t *testing.T) {
	stats := NewRuntimeStats(1000)
	
	// Test balanced worker utilization
	balancedWorkers := []float64{0.8, 0.8, 0.8, 0.8}
	stats.UpdateWorkerUtilization(balancedWorkers)
	
	if stats.LoadImbalance != 0.0 {
		t.Errorf("Expected no load imbalance for balanced workers, got %.2f", stats.LoadImbalance)
	}
	
	if stats.IsLoadImbalanced(0.2) {
		t.Error("Balanced workers should not be considered imbalanced")
	}
	
	// Test imbalanced worker utilization
	imbalancedWorkers := []float64{0.9, 0.1, 0.9, 0.1}
	stats.UpdateWorkerUtilization(imbalancedWorkers)
	
	if stats.LoadImbalance == 0.0 {
		t.Error("Expected non-zero load imbalance for imbalanced workers")
	}
	
	if !stats.IsLoadImbalanced(0.2) {
		t.Error("Imbalanced workers should be detected")
	}
}

func TestAdaptiveDecisionLog(t *testing.T) {
	log := NewAdaptiveDecisionLog()
	
	// Test initial state
	if log.GetDecisionCount() != 0 {
		t.Errorf("Expected 0 decisions initially, got %d", log.GetDecisionCount())
	}
	
	// Test logging decisions
	stats := NewRuntimeStats(1000)
	log.LogDecision("TestOperator", "TestDecision", "TestReason", *stats)
	
	if log.GetDecisionCount() != 1 {
		t.Errorf("Expected 1 decision after logging, got %d", log.GetDecisionCount())
	}
	
	// Test retrieving decisions
	decisions := log.GetDecisions()
	if len(decisions) != 1 {
		t.Errorf("Expected 1 decision in slice, got %d", len(decisions))
	}
	
	decision := decisions[0]
	if decision.Operator != "TestOperator" {
		t.Errorf("Expected operator 'TestOperator', got '%s'", decision.Operator)
	}
	
	if decision.Decision != "TestDecision" {
		t.Errorf("Expected decision 'TestDecision', got '%s'", decision.Decision)
	}
	
	if decision.Reason != "TestReason" {
		t.Errorf("Expected reason 'TestReason', got '%s'", decision.Reason)
	}
}

func TestAdaptiveThresholds(t *testing.T) {
	thresholds := DefaultAdaptiveThresholds()
	
	// Test default values are reasonable
	if thresholds.CardinalityErrorThreshold <= 0 || thresholds.CardinalityErrorThreshold > 1 {
		t.Errorf("Expected reasonable cardinality error threshold, got %.2f", thresholds.CardinalityErrorThreshold)
	}
	
	if thresholds.MemoryPressureThreshold <= 0 || thresholds.MemoryPressureThreshold > 1 {
		t.Errorf("Expected reasonable memory pressure threshold, got %.2f", thresholds.MemoryPressureThreshold)
	}
	
	if thresholds.MinRowsForAdaptation <= 0 {
		t.Errorf("Expected positive MinRowsForAdaptation, got %d", thresholds.MinRowsForAdaptation)
	}
}

func TestAdaptiveContext(t *testing.T) {
	execCtx := &ExecContext{}
	maxMemory := int64(1024 * 1024) // 1MB
	
	adaptiveCtx := NewAdaptiveContext(execCtx, maxMemory)
	
	// Test initial state
	if adaptiveCtx.ExecCtx != execCtx {
		t.Error("ExecContext not set correctly")
	}
	
	if adaptiveCtx.MaxMemory != maxMemory {
		t.Errorf("Expected MaxMemory=%d, got %d", maxMemory, adaptiveCtx.MaxMemory)
	}
	
	if !adaptiveCtx.Enabled {
		t.Error("Adaptive context should be enabled by default")
	}
	
	// Test adaptation decision logic
	stats := NewRuntimeStats(1000)
	stats.UpdateCardinality(100) // Small row count
	
	if adaptiveCtx.ShouldAdapt(stats) {
		t.Error("Should not adapt for small row count")
	}
	
	// Test with larger row count and high error
	stats.UpdateCardinality(2000) // High error (2000 vs 1000 estimate)
	
	if !adaptiveCtx.ShouldAdapt(stats) {
		t.Error("Should adapt for high cardinality error")
	}
	
	// Test decision logging
	adaptiveCtx.LogAdaptiveDecision("TestOp", "TestDecision", "TestReason", stats)
	
	if adaptiveCtx.DecisionLog.GetDecisionCount() != 1 {
		t.Error("Decision should have been logged")
	}
}

func TestAdaptiveJoinOperator(t *testing.T) {
	// Create test tables
	leftTable := &catalog.Table{
		ID:        1,
		TableName: "employees",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer},
			{Name: "name", DataType: types.Text},
		},
	}
	
	rightTable := &catalog.Table{
		ID:        2,
		TableName: "departments",
		Columns: []*catalog.Column{
			{Name: "emp_id", DataType: types.Integer},
			{Name: "dept", DataType: types.Text},
		},
	}
	
	// Create test data
	leftRows := []*Row{
		{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("Alice")}},
		{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("Bob")}},
	}
	
	rightRows := []*Row{
		{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("Engineering")}},
		{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("Sales")}},
	}
	
	leftStorage := &MockStorageBackend{rows: leftRows}
	rightStorage := &MockStorageBackend{rows: rightRows}
	
	// Create operators
	leftScan := NewStorageScanOperator(leftTable, leftStorage)
	rightScan := NewStorageScanOperator(rightTable, rightStorage)
	
	// Create adaptive context
	execCtx := &ExecContext{}
	adaptiveCtx := NewAdaptiveContext(execCtx, 1024*1024)
	
	// Create adaptive join
	adaptiveJoin := NewAdaptiveJoinOperator(
		leftScan, rightScan,
		nil, nil, // No join keys for this test
		nil,      // No predicate
		InnerJoin,
		adaptiveCtx,
	)
	
	// Test initial state
	if adaptiveJoin.initialMethod != AutoJoinMethod {
		t.Errorf("Expected initial method Auto, got %s", adaptiveJoin.initialMethod)
	}
	
	if adaptiveJoin.hasSwitched {
		t.Error("Should not have switched initially")
	}
	
	// Test opening
	err := adaptiveJoin.Open(execCtx)
	if err != nil {
		t.Fatalf("Failed to open adaptive join: %v", err)
	}
	defer adaptiveJoin.Close()
	
	// Test that a method was selected
	if adaptiveJoin.currentMethod == AutoJoinMethod {
		t.Error("Should have selected a specific join method after opening")
	}
	
	// Test execution (basic functionality)
	rowCount := 0
	for {
		row, err := adaptiveJoin.Next()
		if err != nil {
			t.Fatalf("Error during adaptive join execution: %v", err)
		}
		if row == nil {
			break
		}
		rowCount++
	}
	
	// Should have some results
	if rowCount == 0 {
		t.Error("Adaptive join should have produced some results")
	}
	
	// Test adaptive stats
	stats := adaptiveJoin.GetAdaptiveStats()
	if stats == nil {
		t.Error("Should return adaptive stats")
	}
	
	if _, exists := stats["initial_method"]; !exists {
		t.Error("Adaptive stats should include initial_method")
	}
	
	if _, exists := stats["current_method"]; !exists {
		t.Error("Adaptive stats should include current_method")
	}
}

func TestAdaptiveScanOperator(t *testing.T) {
	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer},
			{Name: "value", DataType: types.Integer},
		},
	}
	
	// Create test data
	rows := make([]*Row, 100)
	for i := 0; i < 100; i++ {
		rows[i] = &Row{
			Values: []types.Value{
				types.NewValue(int64(i)),
				types.NewValue(int64(i * 10)),
			},
		}
	}
	
	mockStorage := &MockStorageBackend{rows: rows}
	
	// Create adaptive context
	execCtx := &ExecContext{}
	adaptiveCtx := NewAdaptiveContext(execCtx, 1024*1024)
	
	// Create adaptive scan
	adaptiveScan := NewAdaptiveScanOperator(
		table,
		"",    // No alias
		mockStorage,
		nil,   // No predicate
		adaptiveCtx,
	)
	
	// Test initial state
	if adaptiveScan.initialMethod != AutoScanMethod {
		t.Errorf("Expected initial method Auto, got %s", adaptiveScan.initialMethod)
	}
	
	if adaptiveScan.hasSwitched {
		t.Error("Should not have switched initially")
	}
	
	// Test opening
	err := adaptiveScan.Open(execCtx)
	if err != nil {
		t.Fatalf("Failed to open adaptive scan: %v", err)
	}
	defer adaptiveScan.Close()
	
	// Test that a method was selected
	if adaptiveScan.currentMethod == AutoScanMethod {
		t.Error("Should have selected a specific scan method after opening")
	}
	
	// Test execution
	rowCount := 0
	for {
		row, err := adaptiveScan.Next()
		if err != nil {
			t.Fatalf("Error during adaptive scan execution: %v", err)
		}
		if row == nil {
			break
		}
		rowCount++
	}
	
	// Should process all rows
	if rowCount == 0 {
		t.Error("Adaptive scan should have produced some results")
	}
	
	// Test adaptive stats
	stats := adaptiveScan.GetAdaptiveStats()
	if stats == nil {
		t.Error("Should return adaptive stats")
	}
	
	if stats["rows_processed"].(int64) != int64(rowCount) {
		t.Errorf("Expected rows_processed=%d, got %v", rowCount, stats["rows_processed"])
	}
}

func TestAdaptiveExecutionPlan(t *testing.T) {
	// Create simple test plan
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer},
		},
	}
	
	rows := []*Row{
		{Values: []types.Value{types.NewValue(int64(1))}},
		{Values: []types.Value{types.NewValue(int64(2))}},
		{Values: []types.Value{types.NewValue(int64(3))}},
	}
	
	mockStorage := &MockStorageBackend{rows: rows}
	scan := NewStorageScanOperator(table, mockStorage)
	
	// Create adaptive execution plan
	execCtx := &ExecContext{}
	adaptiveCtx := NewAdaptiveContext(execCtx, 1024*1024)
	config := DefaultAdaptiveExecutionConfig()
	
	adaptivePlan := NewAdaptiveExecutionPlan(scan, adaptiveCtx, config)
	
	// Test initial state
	if adaptivePlan.executionPhase != InitializationPhase {
		t.Errorf("Expected InitializationPhase, got %s", adaptivePlan.executionPhase)
	}
	
	// Test opening
	err := adaptivePlan.Open(execCtx)
	if err != nil {
		t.Fatalf("Failed to open adaptive execution plan: %v", err)
	}
	defer adaptivePlan.Close()
	
	// Should be in running phase
	if adaptivePlan.executionPhase != ExecutionPhase_Running {
		t.Errorf("Expected ExecutionPhase_Running after open, got %s", adaptivePlan.executionPhase)
	}
	
	// Test execution
	rowCount := 0
	for {
		row, err := adaptivePlan.Next()
		if err != nil {
			t.Fatalf("Error during adaptive plan execution: %v", err)
		}
		if row == nil {
			break
		}
		rowCount++
	}
	
	// Should be in completion phase
	if adaptivePlan.executionPhase != CompletionPhase {
		t.Errorf("Expected CompletionPhase after completion, got %s", adaptivePlan.executionPhase)
	}
	
	// Test execution report
	report := adaptivePlan.GetAdaptiveExecutionReport()
	if report == nil {
		t.Error("Should return execution report")
	}
	
	if report.TotalRowsProcessed != int64(rowCount) {
		t.Errorf("Expected TotalRowsProcessed=%d, got %d", rowCount, report.TotalRowsProcessed)
	}
	
	if report.QueryExecutionTime <= 0 {
		t.Error("Should have positive execution time")
	}
}

func TestAdaptiveExecutionConfig(t *testing.T) {
	config := DefaultAdaptiveExecutionConfig()
	
	// Test default values are reasonable
	if !config.EnableGlobalAdaptation {
		t.Error("Global adaptation should be enabled by default")
	}
	
	if config.QueryMemoryLimit <= 0 {
		t.Errorf("Expected positive QueryMemoryLimit, got %d", config.QueryMemoryLimit)
	}
	
	if config.MinRowsPerSecond <= 0 {
		t.Errorf("Expected positive MinRowsPerSecond, got %.2f", config.MinRowsPerSecond)
	}
	
	if config.MaxExecutionTime <= 0 {
		t.Errorf("Expected positive MaxExecutionTime, got %v", config.MaxExecutionTime)
	}
	
	if config.MaxParallelWorkers <= 0 {
		t.Errorf("Expected positive MaxParallelWorkers, got %d", config.MaxParallelWorkers)
	}
}

// Benchmark adaptive execution overhead
func BenchmarkAdaptiveVsSequential(b *testing.B) {
	// Create test data
	table := &catalog.Table{
		ID:        1,
		TableName: "bench_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer},
		},
	}
	
	rows := make([]*Row, 1000)
	for i := 0; i < 1000; i++ {
		rows[i] = &Row{
			Values: []types.Value{types.NewValue(int64(i))},
		}
	}
	
	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mockStorage := &MockStorageBackend{rows: rows}
			scan := NewStorageScanOperator(table, mockStorage)
			execCtx := &ExecContext{}
			
			scan.Open(execCtx)
			for {
				row, _ := scan.Next()
				if row == nil {
					break
				}
			}
			scan.Close()
		}
	})
	
	b.Run("Adaptive", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mockStorage := &MockStorageBackend{rows: rows}
			execCtx := &ExecContext{}
			adaptiveCtx := NewAdaptiveContext(execCtx, 1024*1024)
			config := DefaultAdaptiveExecutionConfig()
			
			scan := NewStorageScanOperator(table, mockStorage)
			adaptivePlan := NewAdaptiveExecutionPlan(scan, adaptiveCtx, config)
			
			adaptivePlan.Open(execCtx)
			for {
				row, _ := adaptivePlan.Next()
				if row == nil {
					break
				}
			}
			adaptivePlan.Close()
		}
	})
}