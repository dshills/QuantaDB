# Phase 1: EXPLAIN ANALYZE Implementation Guide

## Overview

This document provides detailed implementation steps for adding EXPLAIN ANALYZE functionality to QuantaDB. This is the first step in our query performance optimization journey and will provide critical visibility into query execution.

## 1. Core Data Structures

### 1.1 Runtime Statistics

```go
// File: internal/sql/executor/stats.go

// OperatorStats tracks runtime statistics for each operator
type OperatorStats struct {
    // Timing
    StartupTimeMs   float64  // Time to produce first row
    TotalTimeMs     float64  // Total execution time
    
    // Row counts
    EstimatedRows   int64    // From planner
    ActualRows      int64    // Actually produced
    ActualLoops     int64    // Number of times operator was executed
    
    // I/O statistics  
    PagesHit        int64    // Pages found in buffer pool
    PagesRead       int64    // Pages read from disk
    PagesDirtied    int64    // Pages modified
    
    // Memory usage
    MemoryUsedKB    int64    // Peak memory usage
    TempSpaceUsedKB int64    // Temp disk space used
    
    // Additional info
    ExtraInfo       map[string]string // Operator-specific details
}

// PlanStats aggregates statistics for entire plan
type PlanStats struct {
    TotalTimeMs      float64
    PlanningTimeMs   float64
    ExecutionTimeMs  float64
    RowsReturned     int64
    BufferHits       int64
    BufferReads      int64
    TempSpaceUsedKB  int64
    
    // Tree of operator statistics
    RootOperatorStats *OperatorStatsNode
}

type OperatorStatsNode struct {
    OperatorName string
    Stats        *OperatorStats
    Children     []*OperatorStatsNode
}
```

### 1.2 Extended Execution Context

```go
// File: internal/sql/executor/executor.go

// Add to ExecContext
type ExecContext struct {
    // ... existing fields ...
    
    // Performance monitoring
    CollectStats    bool              // Whether to collect runtime stats
    StartTime       time.Time         // Query start time
    OperatorStats   map[Operator]*OperatorStats // Stats by operator
    BufferStats     *BufferPoolStats  // Buffer pool statistics
}
```

## 2. Operator Instrumentation

### 2.1 Base Operator Changes

```go
// File: internal/sql/executor/operator.go

// Add stats collection to baseOperator
type baseOperator struct {
    schema      *Schema
    ctx         *ExecContext
    stats       *OperatorStats
    startTime   time.Time
    firstRow    bool
}

// Helper methods for all operators
func (o *baseOperator) startExecution() {
    if o.ctx.CollectStats {
        o.startTime = time.Now()
        o.firstRow = true
        o.stats = &OperatorStats{
            EstimatedRows: o.getEstimatedRows(),
        }
    }
}

func (o *baseOperator) recordRow() {
    if o.ctx.CollectStats {
        if o.firstRow {
            o.stats.StartupTimeMs = float64(time.Since(o.startTime).Microseconds()) / 1000.0
            o.firstRow = false
        }
        o.stats.ActualRows++
    }
}

func (o *baseOperator) finishExecution() {
    if o.ctx.CollectStats {
        o.stats.TotalTimeMs = float64(time.Since(o.startTime).Microseconds()) / 1000.0
        o.ctx.OperatorStats[o] = o.stats
    }
}
```

### 2.2 Scan Operator Instrumentation

```go
// File: internal/sql/executor/scan.go

func (s *StorageScanOperator) Open(ctx *ExecContext) error {
    s.ctx = ctx
    s.startExecution() // Start timing
    
    // ... existing code ...
    
    if s.storage != nil {
        scanner, err := s.storage.Scan(s.table.ID)
        if err != nil {
            return fmt.Errorf("failed to create scanner: %w", err)
        }
        s.scanner = scanner
    }
    
    return nil
}

func (s *StorageScanOperator) Next() (*Row, error) {
    row, err := s.scanner.Next()
    if err != nil {
        if err == io.EOF {
            s.finishExecution() // Record final stats
            return nil, nil
        }
        return nil, err
    }
    
    s.recordRow() // Count row
    
    // Track buffer statistics
    if s.ctx.CollectStats && s.ctx.BufferStats != nil {
        s.stats.PagesHit = s.ctx.BufferStats.HitCount - s.stats.PagesHit
        s.stats.PagesRead = s.ctx.BufferStats.MissCount - s.stats.PagesRead
    }
    
    return row, nil
}
```

### 2.3 Hash Join Instrumentation

```go
// File: internal/sql/executor/join.go

func (h *HashJoinOperator) Open(ctx *ExecContext) error {
    h.ctx = ctx
    h.startExecution()
    
    // ... existing code ...
    
    // Build phase timing
    buildStart := time.Now()
    err := h.buildHashTable()
    
    if h.ctx.CollectStats {
        h.stats.ExtraInfo = map[string]string{
            "Hash Build Time": fmt.Sprintf("%.2f ms", 
                float64(time.Since(buildStart).Microseconds())/1000.0),
            "Hash Buckets": fmt.Sprintf("%d", len(h.hashTable)),
            "Hash Memory": fmt.Sprintf("%d KB", h.estimateHashTableMemory()/1024),
        }
        h.stats.MemoryUsedKB = h.estimateHashTableMemory() / 1024
    }
    
    return err
}
```

## 3. EXPLAIN ANALYZE Implementation

### 3.1 SQL Parser Extension

```go
// File: internal/sql/parser/ast.go

// Add ANALYZE option to EXPLAIN
type ExplainStmt struct {
    Statement Statement
    Analyze   bool        // true for EXPLAIN ANALYZE
    Verbose   bool        // Include additional details
    Format    string      // 'text' or 'json'
}
```

### 3.2 Planner Extension

```go
// File: internal/sql/planner/planner.go

func (p *BasicPlanner) planExplain(stmt *parser.ExplainStmt) (LogicalPlan, error) {
    // Plan the inner statement
    innerPlan, err := p.buildLogicalPlan(stmt.Statement)
    if err != nil {
        return nil, err
    }
    
    return &LogicalExplain{
        Plan:    innerPlan,
        Analyze: stmt.Analyze,
        Verbose: stmt.Verbose,
        Format:  stmt.Format,
    }, nil
}
```

### 3.3 Executor Implementation

```go
// File: internal/sql/executor/explain.go

type ExplainOperator struct {
    baseOperator
    child      Operator
    analyze    bool
    verbose    bool
    format     string
    results    []string
    currentIdx int
}

func (e *ExplainOperator) Open(ctx *ExecContext) error {
    e.ctx = ctx
    
    if e.analyze {
        // Enable stats collection
        ctx.CollectStats = true
        ctx.OperatorStats = make(map[Operator]*OperatorStats)
        ctx.StartTime = time.Now()
        
        // Execute the query
        err := e.child.Open(ctx)
        if err != nil {
            return err
        }
        
        // Consume all rows to get accurate stats
        for {
            row, err := e.child.Next()
            if err != nil {
                return err
            }
            if row == nil {
                break
            }
        }
        
        e.child.Close()
        
        // Generate EXPLAIN ANALYZE output
        e.results = e.generateAnalyzeOutput()
    } else {
        // Just show the plan
        e.results = e.generatePlanOutput()
    }
    
    return nil
}

func (e *ExplainOperator) generateAnalyzeOutput() []string {
    var results []string
    
    // Header
    planningTime := float64(e.ctx.StartTime.Sub(e.ctx.StartTime).Microseconds()) / 1000.0
    executionTime := time.Since(e.ctx.StartTime).Seconds() * 1000
    
    results = append(results, fmt.Sprintf("Query Planning Time: %.3f ms", planningTime))
    results = append(results, fmt.Sprintf("Query Execution Time: %.3f ms", executionTime))
    results = append(results, "")
    
    // Build operator tree with stats
    tree := e.buildOperatorTree(e.child, 0)
    results = append(results, tree...)
    
    // Summary statistics
    if e.verbose {
        results = append(results, "")
        results = append(results, "Buffer Statistics:")
        results = append(results, fmt.Sprintf("  Hits: %d pages", e.getTotalStat("PagesHit")))
        results = append(results, fmt.Sprintf("  Reads: %d pages", e.getTotalStat("PagesRead")))
        results = append(results, fmt.Sprintf("  Dirtied: %d pages", e.getTotalStat("PagesDirtied")))
    }
    
    return results
}

func (e *ExplainOperator) buildOperatorTree(op Operator, depth int) []string {
    var results []string
    indent := strings.Repeat("  ", depth)
    
    // Operator name and basic info
    opName := e.getOperatorName(op)
    stats := e.ctx.OperatorStats[op]
    
    if stats != nil {
        // Format: -> Hash Join (cost=100.00..200.00 rows=1000) (actual time=1.234..5.678 rows=950 loops=1)
        line := fmt.Sprintf("%s-> %s (cost=%.2f..%.2f rows=%d width=%d)",
            indent, opName, 
            e.getEstimatedCost(op), e.getEstimatedTotalCost(op),
            stats.EstimatedRows, e.getEstimatedWidth(op))
            
        line += fmt.Sprintf(" (actual time=%.3f..%.3f rows=%d loops=%d)",
            stats.StartupTimeMs, stats.TotalTimeMs,
            stats.ActualRows, stats.ActualLoops)
            
        results = append(results, line)
        
        // Extra details for specific operators
        if e.verbose && stats.ExtraInfo != nil {
            for key, value := range stats.ExtraInfo {
                results = append(results, fmt.Sprintf("%s     %s: %s", indent, key, value))
            }
        }
        
        // Memory usage
        if stats.MemoryUsedKB > 0 {
            results = append(results, fmt.Sprintf("%s     Memory Usage: %d KB", indent, stats.MemoryUsedKB))
        }
        
        // Buffer usage
        if stats.PagesHit > 0 || stats.PagesRead > 0 {
            results = append(results, fmt.Sprintf("%s     Buffers: hit=%d read=%d", 
                indent, stats.PagesHit, stats.PagesRead))
        }
    } else {
        // No stats (EXPLAIN without ANALYZE)
        results = append(results, fmt.Sprintf("%s-> %s", indent, opName))
    }
    
    // Recurse to children
    children := e.getChildren(op)
    for _, child := range children {
        childResults := e.buildOperatorTree(child, depth+1)
        results = append(results, childResults...)
    }
    
    return results
}
```

## 4. Integration with Buffer Pool

```go
// File: internal/storage/buffer_pool.go

// Add statistics tracking
type BufferPoolStats struct {
    HitCount      int64
    MissCount     int64
    EvictionCount int64
    DirtyPages    int64
    mu            sync.RWMutex
}

func (bp *BufferPool) GetPage(pageID PageID) (*Page, error) {
    bp.mu.RLock()
    if frame, exists := bp.pageTable[pageID]; exists {
        bp.mu.RUnlock()
        
        // Record hit
        if bp.stats != nil {
            atomic.AddInt64(&bp.stats.HitCount, 1)
        }
        
        frame.pinCount++
        bp.updateLRU(frame)
        return frame.page, nil
    }
    bp.mu.RUnlock()
    
    // Record miss
    if bp.stats != nil {
        atomic.AddInt64(&bp.stats.MissCount, 1)
    }
    
    // ... rest of implementation
}
```

## 5. Testing

### 5.1 Unit Tests

```go
// File: internal/sql/executor/explain_test.go

func TestExplainAnalyze(t *testing.T) {
    // Setup
    ctx := createTestContext()
    
    // Create a simple query
    query := "SELECT * FROM users WHERE age > 25"
    plan := parseAndPlan(query)
    
    // Create EXPLAIN ANALYZE operator
    explain := &ExplainOperator{
        child:   buildOperator(plan),
        analyze: true,
        verbose: true,
    }
    
    // Execute
    err := explain.Open(ctx)
    require.NoError(t, err)
    
    // Get results
    var results []string
    for {
        row, err := explain.Next()
        require.NoError(t, err)
        if row == nil {
            break
        }
        results = append(results, row.Values[0].Data.(string))
    }
    
    // Verify output contains expected elements
    require.Contains(t, results[0], "Query Planning Time:")
    require.Contains(t, results[1], "Query Execution Time:")
    
    // Find scan operator output
    var scanLine string
    for _, line := range results {
        if strings.Contains(line, "Scan") {
            scanLine = line
            break
        }
    }
    
    require.NotEmpty(t, scanLine)
    require.Contains(t, scanLine, "actual time=")
    require.Contains(t, scanLine, "rows=")
}
```

### 5.2 Integration Tests

```go
// Test with TPC-H queries
func TestExplainAnalyzeTPC_H(t *testing.T) {
    queries := []string{
        tpch.Q1, // Simple aggregation
        tpch.Q3, // Join with ORDER BY
        tpch.Q5, // Complex multi-way join
    }
    
    for i, query := range queries {
        t.Run(fmt.Sprintf("Q%d", i+1), func(t *testing.T) {
            results := executeExplainAnalyze(query)
            
            // Verify stats are collected
            assertContainsStats(t, results)
            
            // Verify plan structure
            assertValidPlanTree(t, results)
            
            // Check for reasonable values
            assertReasonableStats(t, results)
        })
    }
}
```

## 6. Example Output

```sql
EXPLAIN ANALYZE SELECT c.name, SUM(o.amount) 
FROM customers c 
JOIN orders o ON c.id = o.customer_id 
WHERE c.country = 'USA' 
GROUP BY c.name 
ORDER BY SUM(o.amount) DESC 
LIMIT 10;
```

Expected output:
```
Query Planning Time: 2.341 ms
Query Execution Time: 45.678 ms

-> Limit (cost=1250.00..1250.00 rows=10 width=48) (actual time=44.123..44.567 rows=10 loops=1)
   -> Sort (cost=1250.00..1275.00 rows=100 width=48) (actual time=44.120..44.123 rows=10 loops=1)
        Sort Key: sum(o.amount) DESC
        Sort Method: top-N heapsort  Memory: 2 KB
        -> Hash Aggregate (cost=1000.00..1100.00 rows=100 width=48) (actual time=42.234..43.567 rows=87 loops=1)
              Group Key: c.name
              Memory Usage: 16 KB
              -> Hash Join (cost=250.00..750.00 rows=5000 width=40) (actual time=5.234..35.678 rows=4823 loops=1)
                    Hash Cond: (o.customer_id = c.id)
                    Hash Build Time: 4.567 ms
                    Hash Buckets: 1024  Memory Usage: 128 KB
                    -> Scan on orders o (cost=0.00..100.00 rows=10000 width=16) (actual time=0.123..12.345 rows=10000 loops=1)
                          Buffers: hit=23 read=77
                    -> Hash (cost=200.00..200.00 rows=1000 width=32) (actual time=4.567..4.567 rows=921 loops=1)
                          -> Scan on customers c (cost=0.00..200.00 rows=1000 width=32) (actual time=0.234..3.456 rows=921 loops=1)
                                Filter: (country = 'USA')
                                Rows Removed by Filter: 79
                                Buffers: hit=5 read=15

Buffer Statistics:
  Hits: 28 pages
  Reads: 92 pages
  Dirtied: 0 pages
```

## Next Steps

1. Implement basic version (2-3 days)
2. Add buffer statistics integration (1 day)
3. Test with TPC-H queries (1 day)
4. Add JSON output format (1 day)
5. Create performance regression tests (1-2 days)

This implementation provides the foundation for understanding query performance and will be essential for validating the improvements in subsequent phases.