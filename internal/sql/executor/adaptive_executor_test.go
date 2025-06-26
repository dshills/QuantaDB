package executor

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine/memory"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestAdaptiveExecutor(t *testing.T) {
	// Create test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := memory.NewEngine()
	txnMgr := txn.NewManager()
	
	// Create test table
	schema := catalog.NewSchema("public")
	table := &catalog.Table{
		ID:         1,
		SchemaName: "public",
		TableName:  "test_table",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer, NotNull: true},
			{ID: 2, Name: "value", DataType: types.Integer},
		},
	}
	schema.Tables[table.TableName] = table
	cat.Schemas["public"] = schema
	
	// Create planners
	costEstimator := planner.NewCostEstimator()
	vectorizedModel := planner.NewVectorizedCostModel(&planner.VectorizedModelConfig{
		ScalarRowCost:       1.0,
		VectorizedBatchCost: 10.0,
		DefaultBatchSize:    1024,
	})
	
	physicalPlanner := planner.NewPhysicalPlanner(costEstimator, vectorizedModel, cat)
	adaptivePlanner := planner.NewAdaptivePhysicalPlanner(physicalPlanner)
	
	// Create adaptive executor
	execConfig := &ExecutorRuntimeConfig{
		VectorizedExecutionEnabled: true,
		QueryMemoryLimit:          1024 * 1024 * 1024, // 1GB
		ResultCachingEnabled:      true,
	}
	
	adaptiveExec := NewAdaptiveExecutor(eng, cat, txnMgr, adaptivePlanner, execConfig)
	
	// Test simple query
	t.Run("SimpleSelect", func(t *testing.T) {
		ctx := &ExecContext{
			Catalog:    cat,
			Engine:     eng,
			TxnManager: txnMgr,
		}
		
		// Create a simple scan plan
		scanPlan := &planner.LogicalScan{
			BaseLogicalPlan: planner.BaseLogicalPlan{
				ID: 1,
			},
			TableName: "test_table",
		}
		
		// Execute with adaptive executor
		tx := txnMgr.BeginTransaction()
		result, err := adaptiveExec.Execute(ctx, scanPlan, tx)
		if err != nil {
			t.Fatalf("Failed to execute plan: %v", err)
		}
		defer result.Close()
		
		// Verify execution succeeded
		if result == nil {
			t.Fatal("Expected non-nil result")
		}
	})
	
	// Test adaptive mode switching
	t.Run("AdaptiveModeSwitch", func(t *testing.T) {
		// Configure high memory pressure threshold
		adaptiveExec.adaptiveConfig.MemoryPressureThreshold = 0.5
		
		ctx := &ExecContext{
			Catalog:    cat,
			Engine:     eng,
			TxnManager: txnMgr,
		}
		
		// Create a filter plan
		scanPlan := &planner.LogicalScan{
			BaseLogicalPlan: planner.BaseLogicalPlan{
				ID: 1,
			},
			TableName: "test_table",
		}
		
		filterPlan := &planner.LogicalFilter{
			BaseLogicalPlan: planner.BaseLogicalPlan{
				ID:       2,
				Children: []planner.Plan{scanPlan},
			},
			Predicate: &planner.ColumnRef{
				TableName:  "test_table",
				ColumnName: "value",
			},
		}
		
		// Execute
		tx := txnMgr.BeginTransaction()
		result, err := adaptiveExec.Execute(ctx, filterPlan, tx)
		if err != nil {
			t.Fatalf("Failed to execute plan: %v", err)
		}
		defer result.Close()
		
		// Check that adaptive features are working
		stats := adaptiveExec.GetStatistics()
		t.Logf("Execution stats: vectorized=%d, fallback=%d", 
			stats.VectorizedQueries, stats.FallbackQueries)
	})
}

func BenchmarkAdaptiveVsRegularExecution(b *testing.B) {
	// Create test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := memory.NewEngine()
	txnMgr := txn.NewManager()
	
	// Create test table with more data
	schema := catalog.NewSchema("public")
	table := &catalog.Table{
		ID:         1,
		SchemaName: "public",
		TableName:  "bench_table",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer, NotNull: true},
			{ID: 2, Name: "value", DataType: types.Integer},
			{ID: 3, Name: "text", DataType: types.Text},
		},
	}
	schema.Tables[table.TableName] = table
	cat.Schemas["public"] = schema
	
	// Create planners
	costEstimator := planner.NewCostEstimator()
	vectorizedModel := planner.NewVectorizedCostModel(&planner.VectorizedModelConfig{
		ScalarRowCost:       1.0,
		VectorizedBatchCost: 10.0,
		DefaultBatchSize:    1024,
	})
	
	physicalPlanner := planner.NewPhysicalPlanner(costEstimator, vectorizedModel, cat)
	adaptivePlanner := planner.NewAdaptivePhysicalPlanner(physicalPlanner)
	
	// Benchmark adaptive executor
	b.Run("Adaptive", func(b *testing.B) {
		execConfig := &ExecutorRuntimeConfig{
			VectorizedExecutionEnabled: true,
			QueryMemoryLimit:          1024 * 1024 * 1024,
			ResultCachingEnabled:      true,
		}
		
		adaptiveExec := NewAdaptiveExecutor(eng, cat, txnMgr, adaptivePlanner, execConfig)
		
		ctx := &ExecContext{
			Catalog:    cat,
			Engine:     eng,
			TxnManager: txnMgr,
		}
		
		scanPlan := &planner.LogicalScan{
			BaseLogicalPlan: planner.BaseLogicalPlan{
				ID: 1,
			},
			TableName: "bench_table",
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := txnMgr.BeginTransaction()
			result, err := adaptiveExec.Execute(ctx, scanPlan, tx)
			if err != nil {
				b.Fatal(err)
			}
			result.Close()
			tx.Commit()
		}
	})
	
	// Benchmark regular executor
	b.Run("Regular", func(b *testing.B) {
		execConfig := &ExecutorRuntimeConfig{
			VectorizedExecutionEnabled: false,
			QueryMemoryLimit:          1024 * 1024 * 1024,
			ResultCachingEnabled:      false,
		}
		
		regularExec := NewConfigurableExecutor(eng, cat, txnMgr, execConfig)
		
		ctx := &ExecContext{
			Catalog:    cat,
			Engine:     eng,
			TxnManager: txnMgr,
		}
		
		scanPlan := &planner.LogicalScan{
			BaseLogicalPlan: planner.BaseLogicalPlan{
				ID: 1,
			},
			TableName: "bench_table",
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := txnMgr.BeginTransaction()
			result, err := regularExec.Execute(ctx, scanPlan, tx)
			if err != nil {
				b.Fatal(err)
			}
			result.Close()
			tx.Commit()
		}
	})
}

func TestAdaptiveOperatorModeSwitch(t *testing.T) {
	// Test the adaptive operator wrapper's ability to switch modes
	monitor := NewExecutionMonitor()
	
	// Create mock operators
	scalarOp := &mockOperator{name: "scalar"}
	vectorizedOp := &mockVectorizedOperator{
		mockOperator: mockOperator{name: "vectorized"},
	}
	
	// Create adaptive wrapper
	wrapper := &adaptiveOperatorWrapper{
		physicalPlan: &planner.PhysicalScan{},
		scalarOp:     scalarOp,
		vectorizedOp: vectorizedOp,
		currentMode:  planner.ExecutionModeVectorized,
		monitor:      monitor,
	}
	
	// Test initial mode
	if wrapper.GetCurrentMode() != planner.ExecutionModeVectorized {
		t.Errorf("Expected vectorized mode, got %v", wrapper.GetCurrentMode())
	}
	
	// Switch to scalar mode
	err := wrapper.SwitchMode(planner.ExecutionModeScalar)
	if err != nil {
		t.Fatalf("Failed to switch mode: %v", err)
	}
	
	if wrapper.GetCurrentMode() != planner.ExecutionModeScalar {
		t.Errorf("Expected scalar mode after switch, got %v", wrapper.GetCurrentMode())
	}
	
	// Try to switch to unsupported mode
	err = wrapper.SwitchMode(planner.ExecutionMode("unsupported"))
	if err == nil {
		t.Error("Expected error for unsupported mode")
	}
}

// Mock operator implementations for testing
type mockOperator struct {
	name     string
	openErr  error
	nextErr  error
	closeErr error
	rows     []*Row
	current  int
}

func (m *mockOperator) Open(ctx *ExecContext) error {
	m.current = 0
	return m.openErr
}

func (m *mockOperator) Next() (*Row, error) {
	if m.nextErr != nil {
		return nil, m.nextErr
	}
	if m.current >= len(m.rows) {
		return nil, nil
	}
	row := m.rows[m.current]
	m.current++
	return row, nil
}

func (m *mockOperator) Close() error {
	return m.closeErr
}

func (m *mockOperator) Schema() *Schema {
	return &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "value", Type: types.Integer},
		},
	}
}

type mockVectorizedOperator struct {
	mockOperator
}

func (m *mockVectorizedOperator) NextBatch() (*ColumnBatch, error) {
	// Simple implementation for testing
	return nil, nil
}

func TestExecutionMonitor(t *testing.T) {
	monitor := NewExecutionMonitor()
	
	// Record some metrics
	metrics1 := &OperatorMetrics{
		OperatorID:    "op1",
		OperatorType:  planner.OperatorTypeScan,
		RowsProcessed: 1000,
		ExecutionTime: 100 * time.Millisecond,
		MemoryUsed:    1024 * 1024,
		ExecutionMode: planner.ExecutionModeVectorized,
	}
	
	monitor.RecordOperatorMetrics(metrics1)
	
	// Record more metrics
	metrics2 := &OperatorMetrics{
		OperatorID:    "op2",
		OperatorType:  planner.OperatorTypeFilter,
		RowsProcessed: 500,
		ExecutionTime: 50 * time.Millisecond,
		MemoryUsed:    512 * 1024,
		ExecutionMode: planner.ExecutionModeScalar,
	}
	
	monitor.RecordOperatorMetrics(metrics2)
	
	// Get aggregated metrics
	agg := monitor.GetMetrics()
	
	if agg.TotalRows != 1500 {
		t.Errorf("Expected 1500 total rows, got %d", agg.TotalRows)
	}
	
	if agg.TotalMemory != 1024*1024+512*1024 {
		t.Errorf("Expected %d total memory, got %d", 1024*1024+512*1024, agg.TotalMemory)
	}
	
	if agg.PeakMemory < agg.TotalMemory {
		t.Errorf("Peak memory should be >= total memory")
	}
}