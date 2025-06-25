package executor

import (
	"fmt"
	"time"
)

// OperatorStats tracks runtime statistics for each operator
type OperatorStats struct {
	// Timing
	StartupTimeMs float64 // Time to produce first row
	TotalTimeMs   float64 // Total execution time

	// Row counts
	EstimatedRows int64 // From planner
	ActualRows    int64 // Actually produced
	ActualLoops   int64 // Number of times operator was executed

	// I/O statistics
	PagesHit     int64 // Pages found in buffer pool
	PagesRead    int64 // Pages read from disk
	PagesDirtied int64 // Pages modified

	// Memory usage
	MemoryUsedKB    int64 // Peak memory usage
	TempSpaceUsedKB int64 // Temp disk space used

	// Additional info
	ExtraInfo map[string]string // Operator-specific details
}

// PlanStats aggregates statistics for entire plan
type PlanStats struct {
	TotalTimeMs     float64
	PlanningTimeMs  float64
	ExecutionTimeMs float64
	RowsReturned    int64
	BufferHits      int64
	BufferReads     int64
	TempSpaceUsedKB int64

	// Tree of operator statistics
	RootOperatorStats *OperatorStatsNode
}

// OperatorStatsNode represents a node in the operator statistics tree
type OperatorStatsNode struct {
	OperatorName string
	OperatorType string
	Stats        *OperatorStats
	Children     []*OperatorStatsNode
}

// String returns a formatted string representation of the operator stats
func (s *OperatorStats) String() string {
	if s == nil {
		return "No statistics available"
	}

	result := fmt.Sprintf("(actual time=%.3f..%.3f rows=%d loops=%d)",
		s.StartupTimeMs, s.TotalTimeMs, s.ActualRows, s.ActualLoops)

	if s.PagesHit > 0 || s.PagesRead > 0 {
		result += fmt.Sprintf(" Buffers: hit=%d read=%d", s.PagesHit, s.PagesRead)
	}

	if s.MemoryUsedKB > 0 {
		result += fmt.Sprintf(" Memory: %d KB", s.MemoryUsedKB)
	}

	return result
}

// StatsTimer helps track operator execution timing
type StatsTimer struct {
	startTime time.Time
	firstRow  bool
	stats     *OperatorStats
}

// NewStatsTimer creates a new timer for tracking operator statistics
func NewStatsTimer(stats *OperatorStats) *StatsTimer {
	return &StatsTimer{
		startTime: time.Now(),
		firstRow:  true,
		stats:     stats,
	}
}

// RecordRow records that a row was produced
func (t *StatsTimer) RecordRow() {
	if t.stats == nil {
		return
	}

	if t.firstRow {
		t.stats.StartupTimeMs = float64(time.Since(t.startTime).Microseconds()) / 1000.0
		t.firstRow = false
	}
	t.stats.ActualRows++
}

// Stop finalizes the timing statistics
func (t *StatsTimer) Stop() {
	if t.stats == nil {
		return
	}

	t.stats.TotalTimeMs = float64(time.Since(t.startTime).Microseconds()) / 1000.0
	if t.stats.ActualLoops == 0 {
		t.stats.ActualLoops = 1
	}
}

// BufferPoolStats tracks buffer pool statistics during query execution
type BufferPoolStats struct {
	StartHitCount  int64
	StartMissCount int64
	HitCount       int64
	MissCount      int64
	EvictionCount  int64
	DirtyPages     int64
	Hits           int64 // Total cache hits
	Misses         int64 // Total cache misses
	PagesRead      int64 // Pages read from disk
	PagesWritten   int64 // Pages written to disk
}

// Reset resets the buffer pool statistics
func (s *BufferPoolStats) Reset() {
	s.StartHitCount = 0
	s.StartMissCount = 0
	s.HitCount = 0
	s.MissCount = 0
	s.EvictionCount = 0
	s.DirtyPages = 0
}

// Start captures the current counts as the starting point
func (s *BufferPoolStats) Start() {
	s.StartHitCount = s.HitCount
	s.StartMissCount = s.MissCount
}

// GetHits returns the number of buffer hits since the start
func (s *BufferPoolStats) GetHits() int64 {
	return s.HitCount - s.StartHitCount
}

// GetMisses returns the number of buffer misses since the start
func (s *BufferPoolStats) GetMisses() int64 {
	return s.MissCount - s.StartMissCount
}
