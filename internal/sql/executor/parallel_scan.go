package executor

import (
	"context"
	"fmt"
	"sync"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// ParallelScanOperator implements parallel table scanning
type ParallelScanOperator struct {
	baseOperator
	table       *catalog.Table
	tableAlias  string
	storage     StorageBackend
	parallelCtx *ParallelContext
	workers     []*ParallelScanWorker
	resultChan  chan *ScanResult
	errorChan   chan error
	workerWG    sync.WaitGroup
	started     bool
	startMu     sync.Mutex
	numWorkers  int
}

// ScanResult represents a row result from parallel scanning
type ScanResult struct {
	Row *Row
	Err error
}

// ParallelScanWorker represents a single scan worker
type ParallelScanWorker struct {
	id        int
	table     *catalog.Table
	storage   StorageBackend
	startKey  []byte
	endKey    []byte
	ctx       context.Context
	resultCh  chan<- *ScanResult
}

// NewParallelScanOperator creates a new parallel scan operator
func NewParallelScanOperator(table *catalog.Table, storage StorageBackend, parallelCtx *ParallelContext) *ParallelScanOperator {
	return NewParallelScanOperatorWithAlias(table, "", storage, parallelCtx)
}

// NewParallelScanOperatorWithAlias creates a new parallel scan operator with table alias
func NewParallelScanOperatorWithAlias(table *catalog.Table, alias string, storage StorageBackend, parallelCtx *ParallelContext) *ParallelScanOperator {
	// Build schema from table columns
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	// Use alias if provided, otherwise use table name
	tableAlias := alias
	if tableAlias == "" {
		tableAlias = table.TableName
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name:       col.Name,
			Type:       col.DataType,
			Nullable:   col.IsNullable,
			TableName:  table.TableName,
			TableAlias: tableAlias,
		}
	}

	numWorkers := parallelCtx.MaxDOP
	if numWorkers <= 0 {
		numWorkers = 4 // Default to 4 workers
	}

	return &ParallelScanOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		table:       table,
		tableAlias:  tableAlias,
		storage:     storage,
		parallelCtx: parallelCtx,
		numWorkers:  numWorkers,
		resultChan:  make(chan *ScanResult, numWorkers*2), // Buffered channel
		errorChan:   make(chan error, 1),
	}
}

// CanParallelize returns true (this operator can be parallelized)
func (ps *ParallelScanOperator) CanParallelize() bool {
	return true
}

// GetParallelDegree returns the suggested degree of parallelism
func (ps *ParallelScanOperator) GetParallelDegree() int {
	return ps.numWorkers
}

// CreateParallelInstance creates a parallel instance (returns self)
func (ps *ParallelScanOperator) CreateParallelInstance(pc *ParallelContext) (Operator, error) {
	// This is already a parallel operator
	return ps, nil
}

// Open initializes the parallel scan
func (ps *ParallelScanOperator) Open(ctx *ExecContext) error {
	ps.ctx = ctx

	ps.startMu.Lock()
	defer ps.startMu.Unlock()

	if ps.started {
		return nil
	}

	// Initialize statistics collection
	ps.initStats(1000) // Default estimate

	// Set transaction ID on storage backend if available
	if ctx.Txn != nil {
		ps.storage.SetTransactionID(uint64(ctx.Txn.ID()))
	}

	// Create scan workers and distribute work
	if err := ps.createWorkers(ctx); err != nil {
		return fmt.Errorf("failed to create scan workers: %w", err)
	}

	// Start all workers
	for _, worker := range ps.workers {
		ps.workerWG.Add(1)
		go ps.runWorker(worker)
	}

	// Start result coordinator
	go ps.coordinator()

	ps.started = true
	return nil
}

// createWorkers creates and initializes scan workers
func (ps *ParallelScanOperator) createWorkers(ctx *ExecContext) error {
	// For now, we'll create workers that scan the entire table
	// TODO: Implement table partitioning for better parallelism
	ps.workers = make([]*ParallelScanWorker, ps.numWorkers)

	for i := 0; i < ps.numWorkers; i++ {
		ps.workers[i] = &ParallelScanWorker{
			id:       i,
			table:    ps.table,
			storage:  ps.storage,
			ctx:      ps.parallelCtx.Ctx,
			resultCh: ps.resultChan,
		}
	}

	return nil
}

// runWorker executes a single scan worker
func (ps *ParallelScanOperator) runWorker(worker *ParallelScanWorker) {
	defer ps.workerWG.Done()

	// Create iterator for this worker's partition
	iterator, err := worker.storage.ScanTable(int64(worker.table.ID), ps.ctx.SnapshotTS)
	if err != nil {
		select {
		case ps.errorChan <- fmt.Errorf("worker %d failed to create iterator: %w", worker.id, err):
		case <-worker.ctx.Done():
		}
		return
	}
	defer iterator.Close()

	// Scan rows in this partition
	rowCount := 0
	for {
		select {
		case <-worker.ctx.Done():
			return
		default:
			row, _, err := iterator.Next()
			if err != nil {
				select {
				case ps.resultChan <- &ScanResult{Err: fmt.Errorf("worker %d scan error: %w", worker.id, err)}:
				case <-worker.ctx.Done():
				}
				return
			}

			if row == nil {
				// EOF for this worker
				return
			}

			// Skip rows based on worker ID for simple round-robin distribution
			if rowCount%ps.numWorkers == worker.id {
				select {
				case ps.resultChan <- &ScanResult{Row: row}:
				case <-worker.ctx.Done():
					return
				}
			}
			rowCount++
		}
	}
}

// coordinator coordinates worker results
func (ps *ParallelScanOperator) coordinator() {
	ps.workerWG.Wait()
	close(ps.resultChan)
}

// Next returns the next row from parallel scanning
func (ps *ParallelScanOperator) Next() (*Row, error) {
	// Check for errors in parallel context
	if err := ps.parallelCtx.GetError(); err != nil {
		return nil, err
	}

	// Check for specific scan errors
	select {
	case err := <-ps.errorChan:
		return nil, err
	default:
	}

	// Get next result
	select {
	case result, ok := <-ps.resultChan:
		if !ok {
			// Channel closed - finalize statistics
			ps.finishStats()
			if ps.ctx != nil && ps.ctx.StatsCollector != nil && ps.stats != nil {
				ps.ctx.StatsCollector(ps, ps.stats)
			}
			return nil, nil // EOF
		}

		if result.Err != nil {
			return nil, result.Err
		}

		// Update statistics
		if ps.ctx.Stats != nil {
			ps.ctx.Stats.RowsRead++
		}

		// Record row for performance stats
		ps.recordRow()

		return result.Row, nil

	case err := <-ps.errorChan:
		return nil, err

	case <-ps.parallelCtx.Ctx.Done():
		return nil, ps.parallelCtx.Ctx.Err()
	}
}

// Close cleans up the parallel scan
func (ps *ParallelScanOperator) Close() error {
	// Cancel parallel context to stop workers
	if ps.parallelCtx != nil {
		ps.parallelCtx.Cancel()
	}

	// Wait for workers to finish
	ps.workerWG.Wait()

	// Ensure stats are finalized
	ps.finishStats()
	if ps.ctx != nil && ps.ctx.StatsCollector != nil && ps.stats != nil {
		ps.ctx.StatsCollector(ps, ps.stats)
	}

	return nil
}

// Make StorageScanOperator implement ParallelizableOperator interface
func (s *StorageScanOperator) CanParallelize() bool {
	return true
}

func (s *StorageScanOperator) GetParallelDegree() int {
	return 4 // Default to 4-way parallelism
}

func (s *StorageScanOperator) CreateParallelInstance(pc *ParallelContext) (Operator, error) {
	return NewParallelScanOperatorWithAlias(s.table, s.tableAlias, s.storage, pc), nil
}