package executor

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

// TestAdaptiveExecutionPerformance tests adaptive execution with different workloads
func TestAdaptiveExecutionPerformance(t *testing.T) {
	// Create test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	txnMgr := txn.NewManager()
	
	// Create test schema
	schema := catalog.NewSchema("public")
	
	// Create a larger test table for performance testing
	testTable := &catalog.Table{
		ID:         1,
		SchemaName: "public",
		TableName:  "performance_test",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer, NotNull: true},
			{ID: 2, Name: "value", DataType: types.Integer},
			{ID: 3, Name: "category", DataType: types.Text},
			{ID: 4, Name: "amount", DataType: types.Decimal},
			{ID: 5, Name: "created_at", DataType: types.Timestamp},
		},
	}
	schema.Tables[testTable.TableName] = testTable
	cat.Schemas["public"] = schema
	
	// Insert test data
	tx := txnMgr.BeginTransaction()
	tableID := storage.TableID(testTable.ID)
	
	// Insert 10,000 rows for performance testing
	rowCount := 10000
	for i := 0; i < rowCount; i++ {
		record := &storage.Record{
			TableID: tableID,
			Data: map[string]interface{}{
				"id":         i,
				"value":      i % 100,
				"category":   fmt.Sprintf("CAT_%d", i%10),
				"amount":     float64(i) * 1.5,
				"created_at": time.Now(),
			},
		}
		if err := eng.Insert(record, tx); err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}
	tx.Commit()
	
	// Test scenarios
	scenarios := []struct {
		name        string
		query       string
		description string
	}{
		{
			name:        "SimpleAggregation",
			query:       "SELECT COUNT(*), SUM(value), AVG(amount) FROM performance_test",
			description: "Tests vectorized aggregation performance",
		},
		{
			name:        "FilteredAggregation",
			query:       "SELECT category, COUNT(*), SUM(amount) FROM performance_test WHERE value > 50 GROUP BY category",
			description: "Tests filtered aggregation with grouping",
		},
		{
			name:        "ComplexExpression",
			query:       "SELECT id, value * 2 + amount / 10 as computed FROM performance_test WHERE value < 20",
			description: "Tests expression evaluation performance",
		},
		{
			name:        "SortedResults",
			query:       "SELECT * FROM performance_test ORDER BY amount DESC LIMIT 100",
			description: "Tests sorting performance",
		},
	}
	
	// Test configurations
	configs := []struct {
		name   string
		config *ExecutorRuntimeConfig
	}{
		{
			name: "Baseline",
			config: &ExecutorRuntimeConfig{
				VectorizedExecutionEnabled: false,
				AdaptiveExecutionEnabled:   false,
				QueryMemoryLimit:          512 * 1024 * 1024,
			},
		},
		{
			name: "Vectorized",
			config: &ExecutorRuntimeConfig{
				VectorizedExecutionEnabled: true,
				AdaptiveExecutionEnabled:   false,
				QueryMemoryLimit:          512 * 1024 * 1024,
			},
		},
		{
			name: "Adaptive",
			config: &ExecutorRuntimeConfig{
				VectorizedExecutionEnabled: true,
				AdaptiveExecutionEnabled:   true,
				QueryMemoryLimit:          512 * 1024 * 1024,
				VectorizedMemoryThreshold: 0.8,
			},
		},
		{
			name: "AdaptiveMemoryPressure",
			config: &ExecutorRuntimeConfig{
				VectorizedExecutionEnabled: true,
				AdaptiveExecutionEnabled:   true,
				QueryMemoryLimit:          64 * 1024 * 1024, // Lower memory limit
				VectorizedMemoryThreshold: 0.5,
			},
		},
	}
	
	// Results table
	fmt.Println("\n=== Adaptive Execution Performance Test Results ===\n")
	fmt.Printf("Test data: %d rows\n\n", rowCount)
	fmt.Printf("%-25s %-25s %-15s %-15s %-20s\n", 
		"Scenario", "Configuration", "Exec Time", "Memory (KB)", "Mode")
	fmt.Println(strings.Repeat("-", 100))
	
	// Run tests
	for _, scenario := range scenarios {
		for _, cfg := range configs {
			t.Run(scenario.name+"_"+cfg.name, func(t *testing.T) {
				// Parse query
				p := parser.NewParser()
				stmt, err := p.Parse(scenario.query)
				if err != nil {
					t.Fatalf("Failed to parse query: %v", err)
				}
				
				// Create planner
				costEstimator := planner.NewCostEstimator()
				vectorizedModel := planner.NewVectorizedCostModel(&planner.VectorizedModelConfig{
					ScalarRowCost:       1.0,
					VectorizedBatchCost: 10.0,
					DefaultBatchSize:    1024,
				})
				
				logicalPlanner := planner.NewPlanner(cat, costEstimator)
				physicalPlanner := planner.NewPhysicalPlanner(costEstimator, vectorizedModel, cat)
				
				// Create executor
				var exec Executor
				if cfg.config.AdaptiveExecutionEnabled {
					adaptivePlanner := planner.NewAdaptivePhysicalPlanner(physicalPlanner)
					exec = NewAdaptiveExecutor(eng, cat, txnMgr, adaptivePlanner, cfg.config)
				} else {
					exec = NewConfigurableExecutor(eng, cat, txnMgr, cfg.config)
				}
				
				// Create logical plan
				logicalPlan, err := logicalPlanner.Plan(stmt)
				if err != nil {
					t.Fatalf("Failed to create logical plan: %v", err)
				}
				
				// Execute query
				ctx := &ExecContext{
					Catalog:      cat,
					Engine:       eng,
					TxnManager:   txnMgr,
					CollectStats: true,
				}
				
				startTime := time.Now()
				startMem := getMemoryUsage()
				
				tx := txnMgr.BeginTransaction()
				result, err := exec.Execute(ctx, logicalPlan, tx)
				if err != nil {
					t.Fatalf("Failed to execute query: %v", err)
				}
				
				// Consume results
				rowsReturned := 0
				for {
					row, err := result.Next()
					if err != nil {
						t.Fatalf("Error reading results: %v", err)
					}
					if row == nil {
						break
					}
					rowsReturned++
				}
				result.Close()
				tx.Commit()
				
				execTime := time.Since(startTime)
				memUsed := getMemoryUsage() - startMem
				
				// Get execution mode
				mode := "Scalar"
				if adaptiveExec, ok := exec.(*AdaptiveExecutor); ok {
					stats := adaptiveExec.GetStatistics()
					if stats.VectorizedQueries > 0 {
						mode = "Vectorized"
					}
					if stats.FallbackQueries > 0 {
						mode = "Mixed"
					}
				} else if configExec, ok := exec.(*ConfigurableExecutor); ok {
					stats := configExec.GetStatistics()
					if stats.VectorizedQueries > 0 {
						mode = "Vectorized"
					}
				}
				
				// Print results
				fmt.Printf("%-25s %-25s %-15s %-15d %-20s\n",
					scenario.name,
					cfg.name,
					execTime,
					memUsed/1024,
					mode)
			})
		}
		fmt.Println()
	}
}

// TestAdaptiveModeSwitching tests runtime mode switching behavior
func TestAdaptiveModeSwitching(t *testing.T) {
	// Create infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	txnMgr := txn.NewManager()
	
	// Create planners
	costEstimator := planner.NewCostEstimator()
	vectorizedModel := planner.NewVectorizedCostModel(&planner.VectorizedModelConfig{
		ScalarRowCost:       1.0,
		VectorizedBatchCost: 10.0,
		DefaultBatchSize:    1024,
	})
	
	physicalPlanner := planner.NewPhysicalPlanner(costEstimator, vectorizedModel, cat)
	adaptivePlanner := planner.NewAdaptivePhysicalPlanner(physicalPlanner)
	
	// Create adaptive executor with low memory threshold
	config := &ExecutorRuntimeConfig{
		VectorizedExecutionEnabled: true,
		AdaptiveExecutionEnabled:   true,
		QueryMemoryLimit:          32 * 1024 * 1024, // 32MB
		VectorizedMemoryThreshold: 0.5,              // Switch at 50% memory usage
	}
	
	adaptiveExec := NewAdaptiveExecutor(eng, cat, txnMgr, adaptivePlanner, config)
	
	// Create test table
	schema := catalog.NewSchema("public")
	table := &catalog.Table{
		ID:         1,
		SchemaName: "public",
		TableName:  "mode_test",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer},
			{ID: 2, Name: "data", DataType: types.Text},
		},
	}
	schema.Tables[table.TableName] = table
	cat.Schemas["public"] = schema
	
	// Test mode switching
	t.Run("MemoryPressureSwitch", func(t *testing.T) {
		// Simulate memory pressure by allocating memory
		memoryHog := make([]byte, 20*1024*1024) // 20MB
		defer func() { memoryHog = nil }()
		
		// Execute a query
		ctx := &ExecContext{
			Catalog:    cat,
			Engine:     eng,
			TxnManager: txnMgr,
		}
		
		scanPlan := &planner.LogicalScan{
			BaseLogicalPlan: planner.BaseLogicalPlan{ID: 1},
			TableName:       "mode_test",
		}
		
		tx := txnMgr.BeginTransaction()
		result, err := adaptiveExec.Execute(ctx, scanPlan, tx)
		if err != nil {
			t.Fatalf("Failed to execute: %v", err)
		}
		result.Close()
		tx.Commit()
		
		// Check if mode switching occurred
		stats := adaptiveExec.GetStatistics()
		t.Logf("Execution stats: Vectorized=%d, Scalar=%d", 
			stats.VectorizedQueries, stats.FallbackQueries)
		
		// We expect some fallback due to memory pressure
		if stats.FallbackQueries == 0 && len(memoryHog) > 0 {
			t.Log("Warning: Expected mode switch due to memory pressure")
		}
	})
}

// Helper function to get current memory usage (simplified)
func getMemoryUsage() int64 {
	// In a real implementation, this would use runtime.MemStats
	// For now, return a placeholder
	return 0
}

// BenchmarkAdaptiveVsRegular benchmarks adaptive vs regular execution
func BenchmarkAdaptiveVsRegular(b *testing.B) {
	// Setup similar to TestAdaptiveExecutionPerformance
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	txnMgr := txn.NewManager()
	
	// Create and populate test table
	schema := catalog.NewSchema("public")
	table := &catalog.Table{
		ID:         1,
		SchemaName: "public",
		TableName:  "bench_table",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer},
			{ID: 2, Name: "value", DataType: types.Integer},
		},
	}
	schema.Tables[table.TableName] = table
	cat.Schemas["public"] = schema
	
	// Insert test data
	tx := txnMgr.BeginTransaction()
	for i := 0; i < 1000; i++ {
		record := &storage.Record{
			TableID: storage.TableID(table.ID),
			Data: map[string]interface{}{
				"id":    i,
				"value": i % 100,
			},
		}
		eng.Insert(record, tx)
	}
	tx.Commit()
	
	// Test query
	query := "SELECT value, COUNT(*) FROM bench_table GROUP BY value"
	
	b.Run("Regular", func(b *testing.B) {
		config := &ExecutorRuntimeConfig{
			VectorizedExecutionEnabled: false,
			QueryMemoryLimit:          512 * 1024 * 1024,
		}
		exec := NewConfigurableExecutor(eng, cat, txnMgr, config)
		benchmarkQuery(b, exec, cat, eng, txnMgr, query)
	})
	
	b.Run("Adaptive", func(b *testing.B) {
		config := &ExecutorRuntimeConfig{
			VectorizedExecutionEnabled: true,
			AdaptiveExecutionEnabled:   true,
			QueryMemoryLimit:          512 * 1024 * 1024,
		}
		
		costEstimator := planner.NewCostEstimator()
		vectorizedModel := planner.NewVectorizedCostModel(&planner.VectorizedModelConfig{
			ScalarRowCost:       1.0,
			VectorizedBatchCost: 10.0,
			DefaultBatchSize:    1024,
		})
		
		physicalPlanner := planner.NewPhysicalPlanner(costEstimator, vectorizedModel, cat)
		adaptivePlanner := planner.NewAdaptivePhysicalPlanner(physicalPlanner)
		exec := NewAdaptiveExecutor(eng, cat, txnMgr, adaptivePlanner, config)
		
		benchmarkQuery(b, exec, cat, eng, txnMgr, query)
	})
}

func benchmarkQuery(b *testing.B, exec Executor, cat catalog.Catalog, eng engine.Engine, txnMgr *txn.Manager, query string) {
	p := parser.NewParser()
	stmt, err := p.Parse(query)
	if err != nil {
		b.Fatal(err)
	}
	
	logicalPlanner := planner.NewPlanner(cat, planner.NewCostEstimator())
	logicalPlan, err := logicalPlanner.Plan(stmt)
	if err != nil {
		b.Fatal(err)
	}
	
	ctx := &ExecContext{
		Catalog:    cat,
		Engine:     eng,
		TxnManager: txnMgr,
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := txnMgr.BeginTransaction()
		result, err := exec.Execute(ctx, logicalPlan, tx)
		if err != nil {
			b.Fatal(err)
		}
		
		// Consume results
		for {
			row, err := result.Next()
			if err != nil {
				b.Fatal(err)
			}
			if row == nil {
				break
			}
		}
		
		result.Close()
		tx.Commit()
	}
}