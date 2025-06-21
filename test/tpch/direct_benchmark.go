package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	
	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/config"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
	"github.com/dshills/QuantaDB/internal/wal"
)

type DirectBenchmark struct {
	engine     storage.Engine
	catalog    *catalog.Catalog
	txnManager *txn.Manager
	walManager *wal.Manager
	ctx        context.Context
}

func NewDirectBenchmark() (*DirectBenchmark, error) {
	// Create temporary data directory
	dataDir := filepath.Join(os.TempDir(), "quantadb_benchmark")
	os.RemoveAll(dataDir)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	// Initialize storage engine
	cfg := &config.Config{
		DataDir: dataDir,
		Storage: config.StorageConfig{
			BufferPoolSize: 1024 * 1024 * 100, // 100MB
			PageSize:       4096,
		},
		WAL: config.WALConfig{
			Enabled: true,
			Path:    filepath.Join(dataDir, "wal"),
		},
	}
	
	engine, err := storage.NewDiskEngine(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage engine: %w", err)
	}
	
	// Initialize WAL
	walManager, err := wal.NewManager(cfg.WAL.Path)
	if err != nil {
		engine.Close()
		return nil, fmt.Errorf("failed to create WAL manager: %w", err)
	}
	
	// Initialize catalog
	cat := catalog.New(engine)
	
	// Initialize transaction manager
	txnManager := txn.NewManager(engine)
	
	return &DirectBenchmark{
		engine:     engine,
		catalog:    cat,
		txnManager: txnManager,
		walManager: walManager,
		ctx:        context.Background(),
	}, nil
}

func (b *DirectBenchmark) Close() error {
	if b.walManager != nil {
		b.walManager.Close()
	}
	if b.engine != nil {
		return b.engine.Close()
	}
	return nil
}

func (b *DirectBenchmark) ExecuteQuery(query string) (int, time.Duration, error) {
	start := time.Now()
	
	// Parse the query
	p := parser.New(query)
	stmt, err := p.Parse()
	if err != nil {
		return 0, 0, fmt.Errorf("parse error: %w", err)
	}
	
	// Create a transaction
	txn := b.txnManager.Begin(txn.ReadCommitted)
	defer txn.Rollback()
	
	// Plan the query
	planner := planner.New(b.catalog, txn)
	plan, err := planner.Plan(stmt)
	if err != nil {
		return 0, 0, fmt.Errorf("planning error: %w", err)
	}
	
	// Execute the query
	exec := executor.New(b.catalog, b.engine, b.txnManager, b.walManager)
	result, err := exec.Execute(b.ctx, plan)
	if err != nil {
		return 0, 0, fmt.Errorf("execution error: %w", err)
	}
	
	// Count rows
	rowCount := 0
	for result.Next() {
		rowCount++
	}
	
	if err := result.Err(); err != nil {
		return 0, 0, fmt.Errorf("result error: %w", err)
	}
	
	// Commit the transaction
	if err := txn.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit error: %w", err)
	}
	
	elapsed := time.Since(start)
	return rowCount, elapsed, nil
}

func (b *DirectBenchmark) LoadSchema() error {
	// Load TPC-H schema
	schemaFile := "schema.sql"
	schemaSQL, err := os.ReadFile(schemaFile)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}
	
	// Execute schema creation
	queries := splitQueries(string(schemaSQL))
	for _, query := range queries {
		if query == "" {
			continue
		}
		
		_, _, err := b.ExecuteQuery(query)
		if err != nil {
			return fmt.Errorf("failed to execute schema query: %w", err)
		}
	}
	
	return nil
}

func (b *DirectBenchmark) LoadData() error {
	// Load generated data files
	tables := []string{"customer", "orders", "lineitem", "part", "supplier", "partsupp", "nation", "region"}
	
	for _, table := range tables {
		dataFile := fmt.Sprintf("data/%s.csv", table)
		if _, err := os.Stat(dataFile); os.IsNotExist(err) {
			log.Printf("Skipping %s - no data file", table)
			continue
		}
		
		// Read CSV and insert data
		data, err := os.ReadFile(dataFile)
		if err != nil {
			return fmt.Errorf("failed to read %s data: %w", table, err)
		}
		
		// For simplicity, we'll create INSERT statements
		// In production, we'd use COPY or bulk loading
		lines := splitLines(string(data))
		for i, line := range lines {
			if i == 0 || line == "" { // Skip header
				continue
			}
			
			insertSQL := generateInsertSQL(table, line)
			_, _, err := b.ExecuteQuery(insertSQL)
			if err != nil {
				return fmt.Errorf("failed to insert into %s: %w", table, err)
			}
		}
		
		log.Printf("Loaded %d rows into %s", len(lines)-1, table)
	}
	
	return nil
}

func (b *DirectBenchmark) RunBenchmark() error {
	fmt.Println("QuantaDB TPC-H Direct Benchmark")
	fmt.Println("================================")
	fmt.Printf("Scale Factor: 0.01\n")
	fmt.Printf("Date: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))
	
	// Test queries
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "Q1: Simple Scan",
			query: "SELECT COUNT(*) FROM customer",
		},
		{
			name:  "Q2: Filter Scan", 
			query: "SELECT COUNT(*) FROM customer WHERE c_mktsegment = 'BUILDING'",
		},
		{
			name:  "Q3: Simple Join",
			query: "SELECT COUNT(*) FROM customer c, orders o WHERE c.c_custkey = o.o_custkey",
		},
		{
			name:  "Q4: Aggregation",
			query: "SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment",
		},
	}
	
	fmt.Println("Query Results:")
	fmt.Println("--------------")
	
	for _, q := range queries {
		rowCount, elapsed, err := b.ExecuteQuery(q.query)
		if err != nil {
			fmt.Printf("%-20s: ERROR - %v\n", q.name, err)
		} else {
			fmt.Printf("%-20s: %d rows in %v\n", q.name, rowCount, elapsed)
		}
	}
	
	return nil
}

// Helper functions

func splitQueries(sql string) []string {
	// Simple query splitter - splits on semicolons
	var queries []string
	current := ""
	
	for _, char := range sql {
		current += string(char)
		if char == ';' {
			queries = append(queries, current)
			current = ""
		}
	}
	
	if current != "" {
		queries = append(queries, current)
	}
	
	return queries
}

func splitLines(data string) []string {
	var lines []string
	current := ""
	
	for _, char := range data {
		if char == '\n' {
			lines = append(lines, current)
			current = ""
		} else {
			current += string(char)
		}
	}
	
	if current != "" {
		lines = append(lines, current)
	}
	
	return lines
}

func generateInsertSQL(table, csvLine string) string {
	// This is a simplified version - in production we'd handle escaping properly
	switch table {
	case "customer":
		return fmt.Sprintf("INSERT INTO customer VALUES (%s)", csvLine)
	case "orders":
		return fmt.Sprintf("INSERT INTO orders VALUES (%s)", csvLine)
	case "lineitem":
		return fmt.Sprintf("INSERT INTO lineitem VALUES (%s)", csvLine)
	default:
		return fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, csvLine)
	}
}

func main() {
	// Create benchmark instance
	bench, err := NewDirectBenchmark()
	if err != nil {
		log.Fatalf("Failed to create benchmark: %v", err)
	}
	defer bench.Close()
	
	// Load schema
	fmt.Println("Loading TPC-H schema...")
	if err := bench.LoadSchema(); err != nil {
		log.Fatalf("Failed to load schema: %v", err)
	}
	
	// Load data
	fmt.Println("Loading TPC-H data...")
	if err := bench.LoadData(); err != nil {
		log.Fatalf("Failed to load data: %v", err)
	}
	
	// Run benchmark
	fmt.Println("\nRunning benchmark...")
	if err := bench.RunBenchmark(); err != nil {
		log.Fatalf("Failed to run benchmark: %v", err)
	}
}