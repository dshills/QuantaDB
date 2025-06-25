package executor

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// ParallelHashJoinOperator implements parallel hash join
type ParallelHashJoinOperator struct {
	baseOperator
	left         Operator
	right        Operator
	predicate    ExprEvaluator
	joinType     JoinType
	parallelCtx  *ParallelContext
	numWorkers   int
	partitions   []*HashPartition
	resultChan   chan *JoinResult
	errorChan    chan error
	workerWG     sync.WaitGroup
	started      bool
	startMu      sync.Mutex
}

// JoinResult represents a row result from parallel join
type JoinResult struct {
	Row *Row
	Err error
}

// HashPartition represents a partition of the hash table
type HashPartition struct {
	id        int
	hashTable map[uint64][]*Row
	leftRows  []*Row
	rightRows []*Row
	mutex     sync.RWMutex
}

// NewHashPartition creates a new hash partition
func NewHashPartition(id int) *HashPartition {
	return &HashPartition{
		id:        id,
		hashTable: make(map[uint64][]*Row),
		leftRows:  make([]*Row, 0),
		rightRows: make([]*Row, 0),
	}
}

// NewParallelHashJoinOperator creates a new parallel hash join operator
func NewParallelHashJoinOperator(left, right Operator, predicate ExprEvaluator, joinType JoinType, parallelCtx *ParallelContext) *ParallelHashJoinOperator {
	// Build combined schema
	leftSchema := left.Schema()
	rightSchema := right.Schema()

	columns := make([]Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns))
	columns = append(columns, leftSchema.Columns...)
	columns = append(columns, rightSchema.Columns...)

	schema := &Schema{Columns: columns}

	numWorkers := parallelCtx.MaxDOP
	if numWorkers <= 0 {
		numWorkers = 4
	}

	return &ParallelHashJoinOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		left:        left,
		right:       right,
		predicate:   predicate,
		joinType:    joinType,
		parallelCtx: parallelCtx,
		numWorkers:  numWorkers,
		resultChan:  make(chan *JoinResult, numWorkers*2),
		errorChan:   make(chan error, 1),
	}
}

// CanParallelize returns true (this operator can be parallelized)
func (phj *ParallelHashJoinOperator) CanParallelize() bool {
	return true
}

// GetParallelDegree returns the suggested degree of parallelism
func (phj *ParallelHashJoinOperator) GetParallelDegree() int {
	return phj.numWorkers
}

// CreateParallelInstance creates a parallel instance (returns self)
func (phj *ParallelHashJoinOperator) CreateParallelInstance(pc *ParallelContext) (Operator, error) {
	return phj, nil
}

// Open initializes the parallel hash join
func (phj *ParallelHashJoinOperator) Open(ctx *ExecContext) error {
	phj.ctx = ctx

	phj.startMu.Lock()
	defer phj.startMu.Unlock()

	if phj.started {
		return nil
	}

	// Initialize statistics collection
	phj.initStats(1000)

	// Open child operators
	if err := phj.left.Open(ctx); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := phj.right.Open(ctx); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	// Create partitions
	phj.partitions = make([]*HashPartition, phj.numWorkers)
	for i := 0; i < phj.numWorkers; i++ {
		phj.partitions[i] = NewHashPartition(i)
	}

	// Build phase: read and partition the build side (left)
	if err := phj.buildPhase(); err != nil {
		return fmt.Errorf("build phase failed: %w", err)
	}

	// Probe phase: read probe side and join in parallel
	if err := phj.probePhase(); err != nil {
		return fmt.Errorf("probe phase failed: %w", err)
	}

	phj.started = true
	return nil
}

// buildPhase reads the left (build) side and partitions it
func (phj *ParallelHashJoinOperator) buildPhase() error {
	for {
		row, err := phj.left.Next()
		if err != nil {
			return fmt.Errorf("error reading build side: %w", err)
		}
		if row == nil {
			break // EOF
		}

		// Hash the row to determine partition
		hash := phj.hashRow(row)
		partitionID := int(hash % uint64(phj.numWorkers))

		// Add to appropriate partition
		partition := phj.partitions[partitionID]
		partition.mutex.Lock()
		partition.leftRows = append(partition.leftRows, row)
		partition.mutex.Unlock()
	}

	// Build hash tables for each partition
	for _, partition := range phj.partitions {
		phj.buildHashTable(partition)
	}

	return nil
}

// buildHashTable builds the hash table for a partition
func (phj *ParallelHashJoinOperator) buildHashTable(partition *HashPartition) {
	partition.mutex.Lock()
	defer partition.mutex.Unlock()

	for _, row := range partition.leftRows {
		hash := phj.hashRow(row)
		partition.hashTable[hash] = append(partition.hashTable[hash], row)
	}
}

// probePhase reads the right (probe) side and joins in parallel
func (phj *ParallelHashJoinOperator) probePhase() error {
	// Start probe workers
	for i := 0; i < phj.numWorkers; i++ {
		phj.workerWG.Add(1)
		go phj.probeWorker(i)
	}

	// Read probe side and distribute to workers
	go func() {
		defer func() {
			// Signal workers to finish
			for _, partition := range phj.partitions {
				partition.mutex.Lock()
				// Mark partition as complete (could use a flag)
				partition.mutex.Unlock()
			}
		}()

		for {
			row, err := phj.right.Next()
			if err != nil {
				select {
				case phj.errorChan <- fmt.Errorf("error reading probe side: %w", err):
				case <-phj.parallelCtx.Ctx.Done():
				}
				return
			}
			if row == nil {
				break // EOF
			}

			// Hash the row to determine partition
			hash := phj.hashRow(row)
			partitionID := int(hash % uint64(phj.numWorkers))

			// Add to appropriate partition
			partition := phj.partitions[partitionID]
			partition.mutex.Lock()
			partition.rightRows = append(partition.rightRows, row)
			partition.mutex.Unlock()
		}
	}()

	// Start result coordinator
	go phj.coordinator()

	return nil
}

// probeWorker processes joins for a specific partition
func (phj *ParallelHashJoinOperator) probeWorker(partitionID int) {
	defer phj.workerWG.Done()

	partition := phj.partitions[partitionID]

	for {
		select {
		case <-phj.parallelCtx.Ctx.Done():
			return
		default:
			// Get a batch of probe rows
			var probeRows []*Row
			partition.mutex.RLock()
			if len(partition.rightRows) > 0 {
				// Take up to 100 rows at a time
				batchSize := 100
				if len(partition.rightRows) < batchSize {
					batchSize = len(partition.rightRows)
				}
				probeRows = make([]*Row, batchSize)
				copy(probeRows, partition.rightRows[:batchSize])
				partition.rightRows = partition.rightRows[batchSize:]
			}
			partition.mutex.RUnlock()

			if len(probeRows) == 0 {
				// No more rows, but keep checking periodically
				// TODO: Better synchronization mechanism
				continue
			}

			// Process this batch
			for _, probeRow := range probeRows {
				if err := phj.processProbeRow(partition, probeRow); err != nil {
					select {
					case phj.errorChan <- err:
					case <-phj.parallelCtx.Ctx.Done():
					}
					return
				}
			}
		}
	}
}

// processProbeRow processes a single probe row against the hash table
func (phj *ParallelHashJoinOperator) processProbeRow(partition *HashPartition, probeRow *Row) error {
	hash := phj.hashRow(probeRow)

	partition.mutex.RLock()
	buildRows := partition.hashTable[hash]
	partition.mutex.RUnlock()

	// Join with all matching build rows
	for _, buildRow := range buildRows {
		joinedRow := phj.joinRows(buildRow, probeRow)

		// Evaluate join predicate if present
		if phj.predicate != nil {
			result, err := phj.predicate.Eval(joinedRow, phj.ctx)
			if err != nil {
				return fmt.Errorf("error evaluating join predicate: %w", err)
			}

			// Skip if predicate is false or NULL
			if result.IsNull() || (result.Data != nil && !result.Data.(bool)) {
				continue
			}
		}

		// Send result
		select {
		case phj.resultChan <- &JoinResult{Row: joinedRow}:
		case <-phj.parallelCtx.Ctx.Done():
			return phj.parallelCtx.Ctx.Err()
		}
	}

	return nil
}

// hashRow computes a hash for a row
func (phj *ParallelHashJoinOperator) hashRow(row *Row) uint64 {
	hasher := fnv.New64a()
	
	// For simplicity, hash all values in the row
	// TODO: Hash only join key columns for better performance
	for _, value := range row.Values {
		if !value.IsNull() {
			hasher.Write([]byte(fmt.Sprintf("%v", value.Data)))
		}
	}
	
	return hasher.Sum64()
}

// joinRows combines build and probe rows
func (phj *ParallelHashJoinOperator) joinRows(buildRow, probeRow *Row) *Row {
	values := make([]types.Value, len(buildRow.Values)+len(probeRow.Values))
	copy(values, buildRow.Values)
	copy(values[len(buildRow.Values):], probeRow.Values)

	return &Row{Values: values}
}

// coordinator coordinates worker results
func (phj *ParallelHashJoinOperator) coordinator() {
	phj.workerWG.Wait()
	close(phj.resultChan)
}

// Next returns the next joined row
func (phj *ParallelHashJoinOperator) Next() (*Row, error) {
	// Check for errors in parallel context
	if err := phj.parallelCtx.GetError(); err != nil {
		return nil, err
	}

	// Check for specific join errors
	select {
	case err := <-phj.errorChan:
		return nil, err
	default:
	}

	// Get next result
	select {
	case result, ok := <-phj.resultChan:
		if !ok {
			// Channel closed - finalize statistics
			phj.finishStats()
			if phj.ctx != nil && phj.ctx.StatsCollector != nil && phj.stats != nil {
				phj.ctx.StatsCollector(phj, phj.stats)
			}
			return nil, nil // EOF
		}

		if result.Err != nil {
			return nil, result.Err
		}

		// Update statistics
		if phj.ctx.Stats != nil {
			phj.ctx.Stats.RowsReturned++
		}

		// Record row for performance stats
		phj.recordRow()

		return result.Row, nil

	case err := <-phj.errorChan:
		return nil, err

	case <-phj.parallelCtx.Ctx.Done():
		return nil, phj.parallelCtx.Ctx.Err()
	}
}

// Close cleans up the parallel hash join
func (phj *ParallelHashJoinOperator) Close() error {
	var err error

	// Cancel parallel context to stop workers
	if phj.parallelCtx != nil {
		phj.parallelCtx.Cancel()
	}

	// Wait for workers to finish
	phj.workerWG.Wait()

	// Close child operators
	if phj.left != nil {
		if closeErr := phj.left.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	if phj.right != nil {
		if closeErr := phj.right.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	// Ensure stats are finalized
	phj.finishStats()
	if phj.ctx != nil && phj.ctx.StatsCollector != nil && phj.stats != nil {
		phj.ctx.StatsCollector(phj, phj.stats)
	}

	return err
}

// Make HashJoinOperator implement ParallelizableOperator interface
func (hj *HashJoinOperator) CanParallelize() bool {
	return true
}

func (hj *HashJoinOperator) GetParallelDegree() int {
	return 4 // Default to 4-way parallelism
}

func (hj *HashJoinOperator) CreateParallelInstance(pc *ParallelContext) (Operator, error) {
	return NewParallelHashJoinOperator(hj.left, hj.right, hj.predicate, hj.joinType, pc), nil
}