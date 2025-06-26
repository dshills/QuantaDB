package tpch

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/engine/disk"
	"github.com/dshills/QuantaDB/internal/network"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/txn"
	"github.com/dshills/QuantaDB/internal/wal"
)

// BenchmarkConfig holds configuration for adaptive benchmarks
type BenchmarkConfig struct {
	DataDir           string
	ScaleFactor       float64
	EnableAdaptive    bool
	EnableVectorized  bool
	EnableCaching     bool
	MemoryLimit       int64
	WarmupRuns        int
	MeasurementRuns   int
}

// BenchmarkResult holds results from a benchmark run
type BenchmarkResult struct {
	Query            string
	Config           string
	ExecutionTime    time.Duration
	RowsProcessed    int64
	MemoryUsed       int64
	VectorizedOps    int64
	ScalarOps        int64
	CacheHits        int64
	CacheMisses      int64
	ModeSwitches     int64
}

// AdaptiveBenchmarkSuite runs TPC-H queries with different configurations
type AdaptiveBenchmarkSuite struct {
	server      *network.Server
	engine      engine.Engine
	catalog     catalog.Catalog
	txnManager  *txn.Manager
	wal         *wal.WAL
	dataDir     string
	scaleFactor float64
}

// NewAdaptiveBenchmarkSuite creates a new benchmark suite
func NewAdaptiveBenchmarkSuite(dataDir string, scaleFactor float64) (*AdaptiveBenchmarkSuite, error) {
	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize WAL
	walPath := filepath.Join(dataDir, "wal")
	wal, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Initialize disk engine
	diskEngine, err := disk.NewEngine(dataDir, wal)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk engine: %w", err)
	}

	// Initialize catalog
	catalog := catalog.NewMemoryCatalog()

	// Initialize transaction manager
	txnManager := txn.NewManager()

	return &AdaptiveBenchmarkSuite{
		engine:      diskEngine,
		catalog:     catalog,
		txnManager:  txnManager,
		wal:         wal,
		dataDir:     dataDir,
		scaleFactor: scaleFactor,
	}, nil
}

// Start starts the database server
func (abs *AdaptiveBenchmarkSuite) Start(config *BenchmarkConfig) error {
	// Create executor configuration
	execConfig := &executor.ExecutorRuntimeConfig{
		VectorizedExecutionEnabled:   config.EnableVectorized,
		AdaptiveExecutionEnabled:     config.EnableAdaptive,
		ResultCachingEnabled:         config.EnableCaching,
		QueryMemoryLimit:            config.MemoryLimit,
		VectorizedMemoryThreshold:   0.8,
		ResultCacheMaxSize:          1024 * 1024 * 1024, // 1GB
		ResultCacheMaxEntries:       1000,
		ResultCacheTTL:              5 * time.Minute,
		EnableStatistics:            true,
	}

	// Create planners
	costEstimator := planner.NewCostEstimator()
	vectorizedModel := planner.NewVectorizedCostModel(&planner.VectorizedModelConfig{
		ScalarRowCost:       1.0,
		VectorizedBatchCost: 10.0,
		DefaultBatchSize:    1024,
	})
	
	physicalPlanner := planner.NewPhysicalPlanner(costEstimator, vectorizedModel, abs.catalog)
	
	var exec executor.Executor
	if config.EnableAdaptive {
		adaptivePlanner := planner.NewAdaptivePhysicalPlanner(physicalPlanner)
		exec = executor.NewAdaptiveExecutor(abs.engine, abs.catalog, abs.txnManager, adaptivePlanner, execConfig)
	} else {
		exec = executor.NewConfigurableExecutor(abs.engine, abs.catalog, abs.txnManager, execConfig)
	}

	// Create server configuration
	serverConfig := &network.ServerConfig{
		Address:      "localhost:5433",
		MaxConnections: 100,
	}

	// Create and start server
	abs.server = network.NewServer(serverConfig, abs.engine, abs.catalog, abs.txnManager, exec, abs.wal)
	
	go func() {
		if err := abs.server.Start(); err != nil {
			fmt.Printf("Server error: %v\n", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	return nil
}

// Stop stops the database server
func (abs *AdaptiveBenchmarkSuite) Stop() error {
	if abs.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		abs.server.Shutdown(ctx)
	}
	
	if abs.wal != nil {
		abs.wal.Close()
	}
	
	if abs.engine != nil {
		abs.engine.Close()
	}
	
	return nil
}

// RunBenchmark runs a single query benchmark
func (abs *AdaptiveBenchmarkSuite) RunBenchmark(query string, config *BenchmarkConfig) (*BenchmarkResult, error) {
	// Connect to database
	db, err := sql.Open("postgres", "host=localhost port=5433 user=test password=test dbname=tpch sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	result := &BenchmarkResult{
		Query:  query,
		Config: abs.getConfigName(config),
	}

	// Warmup runs
	for i := 0; i < config.WarmupRuns; i++ {
		rows, err := db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("warmup query failed: %w", err)
		}
		
		// Consume all rows
		for rows.Next() {
			// Just consume
		}
		rows.Close()
	}

	// Measurement runs
	var totalTime time.Duration
	var totalRows int64

	for i := 0; i < config.MeasurementRuns; i++ {
		start := time.Now()
		
		rows, err := db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("measurement query failed: %w", err)
		}
		
		rowCount := int64(0)
		for rows.Next() {
			rowCount++
		}
		rows.Close()
		
		elapsed := time.Since(start)
		totalTime += elapsed
		totalRows += rowCount
	}

	result.ExecutionTime = totalTime / time.Duration(config.MeasurementRuns)
	result.RowsProcessed = totalRows / int64(config.MeasurementRuns)

	// Get execution statistics if available
	if adaptiveExec, ok := abs.server.GetExecutor().(*executor.AdaptiveExecutor); ok {
		stats := adaptiveExec.GetStatistics()
		result.VectorizedOps = stats.VectorizedQueries
		result.ScalarOps = stats.FallbackQueries
		result.CacheHits = stats.CacheHits
		result.CacheMisses = stats.CacheMisses
		result.MemoryUsed = stats.PeakMemoryUsed
	} else if configExec, ok := abs.server.GetExecutor().(*executor.ConfigurableExecutor); ok {
		stats := configExec.GetStatistics()
		result.VectorizedOps = stats.VectorizedQueries
		result.ScalarOps = stats.FallbackQueries
		result.CacheHits = stats.CacheHits
		result.CacheMisses = stats.CacheMisses
		result.MemoryUsed = stats.PeakMemoryUsed
	}

	return result, nil
}

func (abs *AdaptiveBenchmarkSuite) getConfigName(config *BenchmarkConfig) string {
	name := ""
	if config.EnableAdaptive {
		name += "Adaptive+"
	}
	if config.EnableVectorized {
		name += "Vectorized+"
	}
	if config.EnableCaching {
		name += "Cached"
	}
	if name == "" {
		name = "Baseline"
	}
	return name
}

// TestTPCHAdaptivePerformance runs performance tests with TPC-H queries
func TestTPCHAdaptivePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TPC-H performance tests in short mode")
	}

	// Test configurations
	configs := []BenchmarkConfig{
		{
			// Baseline: no optimizations
			EnableAdaptive:   false,
			EnableVectorized: false,
			EnableCaching:    false,
			MemoryLimit:      1024 * 1024 * 1024,
			WarmupRuns:       2,
			MeasurementRuns:  5,
		},
		{
			// Vectorized only
			EnableAdaptive:   false,
			EnableVectorized: true,
			EnableCaching:    false,
			MemoryLimit:      1024 * 1024 * 1024,
			WarmupRuns:       2,
			MeasurementRuns:  5,
		},
		{
			// Adaptive + Vectorized
			EnableAdaptive:   true,
			EnableVectorized: true,
			EnableCaching:    false,
			MemoryLimit:      1024 * 1024 * 1024,
			WarmupRuns:       2,
			MeasurementRuns:  5,
		},
		{
			// Full optimizations
			EnableAdaptive:   true,
			EnableVectorized: true,
			EnableCaching:    true,
			MemoryLimit:      1024 * 1024 * 1024,
			WarmupRuns:       2,
			MeasurementRuns:  5,
		},
		{
			// Memory pressure test
			EnableAdaptive:   true,
			EnableVectorized: true,
			EnableCaching:    true,
			MemoryLimit:      256 * 1024 * 1024, // Lower memory limit
			WarmupRuns:       2,
			MeasurementRuns:  5,
		},
	}

	// Representative TPC-H queries for benchmarking
	queries := []struct {
		name  string
		query string
	}{
		{
			name: "Q1-Aggregation",
			query: `
				SELECT
					l_returnflag,
					l_linestatus,
					SUM(l_quantity) as sum_qty,
					SUM(l_extendedprice) as sum_base_price,
					SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
					SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
					AVG(l_quantity) as avg_qty,
					AVG(l_extendedprice) as avg_price,
					AVG(l_discount) as avg_disc,
					COUNT(*) as count_order
				FROM
					lineitem
				WHERE
					l_shipdate <= date '1998-12-01' - interval '90' day
				GROUP BY
					l_returnflag,
					l_linestatus
				ORDER BY
					l_returnflag,
					l_linestatus`,
		},
		{
			name: "Q3-Join",
			query: `
				SELECT
					l_orderkey,
					SUM(l_extendedprice * (1 - l_discount)) as revenue,
					o_orderdate,
					o_shippriority
				FROM
					customer,
					orders,
					lineitem
				WHERE
					c_mktsegment = 'BUILDING'
					AND c_custkey = o_custkey
					AND l_orderkey = o_orderkey
					AND o_orderdate < date '1995-03-15'
					AND l_shipdate > date '1995-03-15'
				GROUP BY
					l_orderkey,
					o_orderdate,
					o_shippriority
				ORDER BY
					revenue DESC,
					o_orderdate
				LIMIT 10`,
		},
		{
			name: "Q6-Filter",
			query: `
				SELECT
					SUM(l_extendedprice * l_discount) as revenue
				FROM
					lineitem
				WHERE
					l_shipdate >= date '1994-01-01'
					AND l_shipdate < date '1994-01-01' + interval '1' year
					AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
					AND l_quantity < 24`,
		},
		{
			name: "Q12-Complex",
			query: `
				SELECT
					l_shipmode,
					SUM(CASE
						WHEN o_orderpriority = '1-URGENT'
							OR o_orderpriority = '2-HIGH'
							THEN 1
						ELSE 0
					END) as high_line_count,
					SUM(CASE
						WHEN o_orderpriority <> '1-URGENT'
							AND o_orderpriority <> '2-HIGH'
							THEN 1
						ELSE 0
					END) as low_line_count
				FROM
					orders,
					lineitem
				WHERE
					o_orderkey = l_orderkey
					AND l_shipmode IN ('MAIL', 'SHIP')
					AND l_commitdate < l_receiptdate
					AND l_shipdate < l_commitdate
					AND l_receiptdate >= date '1994-01-01'
					AND l_receiptdate < date '1994-01-01' + interval '1' year
				GROUP BY
					l_shipmode
				ORDER BY
					l_shipmode`,
		},
	}

	// Create test directory
	dataDir := filepath.Join(os.TempDir(), "quantadb_adaptive_bench")
	defer os.RemoveAll(dataDir)

	suite, err := NewAdaptiveBenchmarkSuite(dataDir, 0.01) // Small scale factor for testing
	if err != nil {
		t.Fatalf("Failed to create benchmark suite: %v", err)
	}
	defer suite.Stop()

	// Results table
	fmt.Println("\n=== TPC-H Adaptive Execution Performance ===\n")
	fmt.Printf("%-15s %-30s %-15s %-15s %-15s %-15s\n", 
		"Query", "Configuration", "Exec Time", "Rows", "Memory (MB)", "Mode Switches")
	fmt.Println(strings.Repeat("-", 105))

	// Run benchmarks
	for _, query := range queries {
		for _, config := range configs {
			// Start server with configuration
			if err := suite.Start(&config); err != nil {
				t.Errorf("Failed to start server: %v", err)
				continue
			}

			// Load test data if needed
			if err := suite.LoadTestData(); err != nil {
				t.Errorf("Failed to load test data: %v", err)
				suite.Stop()
				continue
			}

			// Run benchmark
			result, err := suite.RunBenchmark(query.query, &config)
			if err != nil {
				t.Errorf("Benchmark failed for %s with %s: %v", 
					query.name, suite.getConfigName(&config), err)
				suite.Stop()
				continue
			}

			// Print results
			fmt.Printf("%-15s %-30s %-15s %-15d %-15.2f %-15d\n",
				query.name,
				result.Config,
				result.ExecutionTime,
				result.RowsProcessed,
				float64(result.MemoryUsed)/(1024*1024),
				result.ModeSwitches)

			// Stop server
			suite.Stop()
		}
		fmt.Println()
	}
}

// LoadTestData loads TPC-H test data
func (abs *AdaptiveBenchmarkSuite) LoadTestData() error {
	// This is a simplified version - in production, you'd load actual TPC-H data
	// For now, create minimal test tables
	
	db, err := sql.Open("postgres", "host=localhost port=5433 user=test password=test dbname=tpch sslmode=disable")
	if err != nil {
		return err
	}
	defer db.Close()

	// Create tables if they don't exist
	tables := []string{
		`CREATE TABLE IF NOT EXISTS lineitem (
			l_orderkey INTEGER,
			l_partkey INTEGER,
			l_suppkey INTEGER,
			l_linenumber INTEGER,
			l_quantity DECIMAL(15,2),
			l_extendedprice DECIMAL(15,2),
			l_discount DECIMAL(15,2),
			l_tax DECIMAL(15,2),
			l_returnflag CHAR(1),
			l_linestatus CHAR(1),
			l_shipdate DATE,
			l_commitdate DATE,
			l_receiptdate DATE,
			l_shipinstruct CHAR(25),
			l_shipmode CHAR(10),
			l_comment VARCHAR(44)
		)`,
		`CREATE TABLE IF NOT EXISTS orders (
			o_orderkey INTEGER,
			o_custkey INTEGER,
			o_orderstatus CHAR(1),
			o_totalprice DECIMAL(15,2),
			o_orderdate DATE,
			o_orderpriority CHAR(15),
			o_clerk CHAR(15),
			o_shippriority INTEGER,
			o_comment VARCHAR(79)
		)`,
		`CREATE TABLE IF NOT EXISTS customer (
			c_custkey INTEGER,
			c_name VARCHAR(25),
			c_address VARCHAR(40),
			c_nationkey INTEGER,
			c_phone CHAR(15),
			c_acctbal DECIMAL(15,2),
			c_mktsegment CHAR(10),
			c_comment VARCHAR(117)
		)`,
	}

	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Insert minimal test data
	// In production, this would load actual TPC-H data files
	
	return nil
}

// BenchmarkAdaptiveQueryExecution benchmarks specific scenarios
func BenchmarkAdaptiveQueryExecution(b *testing.B) {
	dataDir := filepath.Join(os.TempDir(), "quantadb_bench")
	defer os.RemoveAll(dataDir)

	suite, err := NewAdaptiveBenchmarkSuite(dataDir, 0.01)
	if err != nil {
		b.Fatalf("Failed to create benchmark suite: %v", err)
	}
	defer suite.Stop()

	config := &BenchmarkConfig{
		EnableAdaptive:   true,
		EnableVectorized: true,
		EnableCaching:    true,
		MemoryLimit:      1024 * 1024 * 1024,
		WarmupRuns:       0, // No warmup for benchmarks
		MeasurementRuns:  1, // Single run per iteration
	}

	if err := suite.Start(config); err != nil {
		b.Fatalf("Failed to start server: %v", err)
	}

	if err := suite.LoadTestData(); err != nil {
		b.Fatalf("Failed to load test data: %v", err)
	}

	// Simple aggregation query
	query := `SELECT COUNT(*), SUM(l_quantity), AVG(l_extendedprice) FROM lineitem`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.RunBenchmark(query, config)
		if err != nil {
			b.Fatal(err)
		}
	}
}