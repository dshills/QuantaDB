package tpch

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// Import PostgreSQL driver
	_ "github.com/lib/pq"
)

// BenchmarkResult holds the result of a single query execution
type BenchmarkResult struct {
	QueryName     string
	Description   string
	ExecutionTime time.Duration
	RowCount      int
	Error         error
	PlanTime      time.Duration
	ExecTime      time.Duration
}

// BenchmarkSuite runs TPC-H benchmarks
type BenchmarkSuite struct {
	db          *sql.DB
	scaleFactor float64
	queries     map[string]string
}

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(connStr string, scaleFactor float64) (*BenchmarkSuite, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &BenchmarkSuite{
		db:          db,
		scaleFactor: scaleFactor,
		queries:     GetQueries(),
	}, nil
}

// Close closes the database connection
func (b *BenchmarkSuite) Close() error {
	return b.db.Close()
}

// RunQuery executes a single query and returns the result
func (b *BenchmarkSuite) RunQuery(name, query string) *BenchmarkResult {
	result := &BenchmarkResult{
		QueryName:   name,
		Description: GetQueryDescriptions()[name],
	}

	// Time the entire execution
	start := time.Now()
	
	// Execute query
	rows, err := b.db.Query(query)
	if err != nil {
		result.Error = err
		result.ExecutionTime = time.Since(start)
		return result
	}
	defer rows.Close()

	// Count rows
	for rows.Next() {
		result.RowCount++
	}

	if err := rows.Err(); err != nil {
		result.Error = err
	}

	result.ExecutionTime = time.Since(start)
	result.ExecTime = result.ExecutionTime // For now, we don't separate plan/exec time
	
	return result
}

// RunAll runs all TPC-H queries and returns results
func (b *BenchmarkSuite) RunAll() []*BenchmarkResult {
	var results []*BenchmarkResult

	// Run queries in order
	queryOrder := []string{"Q3", "Q5", "Q8", "Q10"}
	
	for _, name := range queryOrder {
		if query, ok := b.queries[name]; ok {
			fmt.Printf("Running %s...\n", name)
			result := b.RunQuery(name, query)
			results = append(results, result)
			
			if result.Error != nil {
				fmt.Printf("  ERROR: %v\n", result.Error)
			} else {
				fmt.Printf("  Completed in %v (%d rows)\n", result.ExecutionTime, result.RowCount)
			}
		}
	}

	return results
}

// WarmUp runs each query once to warm up the cache
func (b *BenchmarkSuite) WarmUp() error {
	fmt.Println("Warming up...")
	for name, query := range b.queries {
		fmt.Printf("  Warming up %s...\n", name)
		if _, err := b.db.Exec(query); err != nil {
			// It's OK if warmup fails, we'll see the error in the actual run
			fmt.Printf("    Warning: %v\n", err)
		}
	}
	fmt.Println("Warmup complete")
	return nil
}

// LoadData loads the generated TPC-H data into the database
func (b *BenchmarkSuite) LoadData(schemaFile string, dataFiles []string) error {
	// First, load the schema
	fmt.Println("Loading schema...")
	if err := b.executeFile(schemaFile); err != nil {
		return fmt.Errorf("failed to load schema: %w", err)
	}

	// Then load each data file
	for _, dataFile := range dataFiles {
		fmt.Printf("Loading %s...\n", dataFile)
		if err := b.executeFile(dataFile); err != nil {
			return fmt.Errorf("failed to load %s: %w", dataFile, err)
		}
	}

	// Run ANALYZE to update statistics
	fmt.Println("Analyzing tables...")
	tables := []string{"nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"}
	for _, table := range tables {
		if _, err := b.db.Exec(fmt.Sprintf("ANALYZE %s", table)); err != nil {
			fmt.Printf("Warning: failed to analyze %s: %v\n", table, err)
		}
	}

	return nil
}

// executeFile executes all SQL statements in a file
func (b *BenchmarkSuite) executeFile(filename string) error {
	// For now, we'll use a simple approach
	// In production, you'd want to read and execute statements more carefully
	ctx := context.Background()
	
	// Start a transaction for bulk loading
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO: Read file and execute statements
	// For now, return nil to allow compilation
	
	return tx.Commit()
}

// GenerateReport generates a benchmark report
func GenerateReport(results []*BenchmarkResult, scaleFactor float64) string {
	report := fmt.Sprintf("TPC-H Benchmark Results\n")
	report += fmt.Sprintf("Scale Factor: %.2f\n", scaleFactor)
	report += fmt.Sprintf("Timestamp: %s\n\n", time.Now().Format(time.RFC3339))
	
	report += "Query Results:\n"
	report += "─────────────\n"
	
	var totalTime time.Duration
	successCount := 0
	
	for _, r := range results {
		report += fmt.Sprintf("\n%s - %s\n", r.QueryName, r.Description)
		if r.Error != nil {
			report += fmt.Sprintf("  Status: FAILED\n")
			report += fmt.Sprintf("  Error: %v\n", r.Error)
		} else {
			report += fmt.Sprintf("  Status: SUCCESS\n")
			report += fmt.Sprintf("  Execution Time: %v\n", r.ExecutionTime)
			report += fmt.Sprintf("  Row Count: %d\n", r.RowCount)
			totalTime += r.ExecutionTime
			successCount++
		}
	}
	
	report += fmt.Sprintf("\n─────────────\n")
	report += fmt.Sprintf("Summary:\n")
	report += fmt.Sprintf("  Successful: %d/%d\n", successCount, len(results))
	if successCount > 0 {
		report += fmt.Sprintf("  Total Time: %v\n", totalTime)
		report += fmt.Sprintf("  Average Time: %v\n", totalTime/time.Duration(successCount))
	}
	
	return report
}