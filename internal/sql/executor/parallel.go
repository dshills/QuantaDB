package executor

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

// ParallelContext provides context for parallel query execution
type ParallelContext struct {
	// Parent execution context
	ExecCtx *ExecContext
	// Background context for cancellation
	Ctx context.Context
	// Cancel function to stop all workers
	Cancel context.CancelFunc
	// Worker pool for task distribution
	WorkerPool *WorkerPool
	// Maximum degree of parallelism
	MaxDOP int
	// Shared error for early termination
	Error error
	// Mutex for error access
	ErrorMu sync.RWMutex
}

// NewParallelContext creates a new parallel execution context
func NewParallelContext(execCtx *ExecContext, maxDOP int) *ParallelContext {
	if maxDOP <= 0 {
		maxDOP = runtime.NumCPU()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ParallelContext{
		ExecCtx:    execCtx,
		Ctx:        ctx,
		Cancel:     cancel,
		WorkerPool: NewWorkerPool(maxDOP),
		MaxDOP:     maxDOP,
	}
}

// SetError sets an error and cancels execution
func (pc *ParallelContext) SetError(err error) {
	pc.ErrorMu.Lock()
	defer pc.ErrorMu.Unlock()

	if pc.Error == nil {
		pc.Error = err
		pc.Cancel()
	}
}

// GetError returns the current error
func (pc *ParallelContext) GetError() error {
	pc.ErrorMu.RLock()
	defer pc.ErrorMu.RUnlock()
	return pc.Error
}

// Close cleans up the parallel context
func (pc *ParallelContext) Close() error {
	pc.Cancel()
	return pc.WorkerPool.Close()
}

// WorkerPool manages a pool of worker goroutines
type WorkerPool struct {
	workers   int
	taskQueue chan Task
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	closeMu   sync.RWMutex
}

// Task represents a unit of work for parallel execution
type Task interface {
	Execute(ctx context.Context) error
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	wp := &WorkerPool{
		workers:   workers,
		taskQueue: make(chan Task, workers*2), // Buffered queue
		ctx:       ctx,
		cancel:    cancel,
	}

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}

	return wp
}

// Submit submits a task to the worker pool
func (wp *WorkerPool) Submit(task Task) error {
	wp.closeMu.RLock()
	defer wp.closeMu.RUnlock()

	if wp.closed {
		return fmt.Errorf("worker pool is closed")
	}

	select {
	case wp.taskQueue <- task:
		return nil
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	}
}

// worker is the main worker goroutine
func (wp *WorkerPool) worker() {
	defer wp.wg.Done()

	for {
		select {
		case task, ok := <-wp.taskQueue:
			if !ok {
				return // Channel closed
			}

			// Execute task
			if err := task.Execute(wp.ctx); err != nil {
				// TODO: Better error handling - for now we continue
				continue
			}

		case <-wp.ctx.Done():
			return // Context canceled
		}
	}
}

// Close shuts down the worker pool
func (wp *WorkerPool) Close() error {
	wp.closeMu.Lock()
	defer wp.closeMu.Unlock()

	if wp.closed {
		return nil
	}

	wp.closed = true
	wp.cancel()
	close(wp.taskQueue)
	wp.wg.Wait()

	return nil
}

// ParallelizableOperator defines operators that can be parallelized
type ParallelizableOperator interface {
	Operator
	// CanParallelize returns true if this operator can be parallelized
	CanParallelize() bool
	// GetParallelDegree returns the suggested degree of parallelism
	GetParallelDegree() int
	// CreateParallelInstance creates a parallel version of this operator
	CreateParallelInstance(pc *ParallelContext) (Operator, error)
}

// ParallelExecutionPlan wraps a regular plan with parallel execution capabilities
type ParallelExecutionPlan struct {
	baseOperator
	rootOperator Operator
	parallelCtx  *ParallelContext
	maxDOP       int
}

// NewParallelExecutionPlan creates a new parallel execution plan
func NewParallelExecutionPlan(root Operator, maxDOP int) *ParallelExecutionPlan {
	return &ParallelExecutionPlan{
		baseOperator: baseOperator{
			schema: root.Schema(),
		},
		rootOperator: root,
		maxDOP:       maxDOP,
	}
}

// Open initializes parallel execution
func (pep *ParallelExecutionPlan) Open(ctx *ExecContext) error {
	pep.ctx = ctx

	// Create parallel context
	pep.parallelCtx = NewParallelContext(ctx, pep.maxDOP)

	// Try to parallelize the plan
	parallelRoot, err := pep.parallelizePlan(pep.rootOperator)
	if err != nil {
		// If parallelization fails, fall back to sequential execution
		parallelRoot = pep.rootOperator
	}

	// Open the (possibly parallelized) plan
	return parallelRoot.Open(ctx)
}

// parallelizePlan attempts to parallelize operators in the plan
func (pep *ParallelExecutionPlan) parallelizePlan(op Operator) (Operator, error) {
	// Check if operator can be parallelized
	if parallelizable, ok := op.(ParallelizableOperator); ok && parallelizable.CanParallelize() {
		// Create parallel instance
		return parallelizable.CreateParallelInstance(pep.parallelCtx)
	}

	// For now, just return the original operator
	// TODO: Implement recursive parallelization of child operators
	return op, nil
}

// Next returns the next row from parallel execution
func (pep *ParallelExecutionPlan) Next() (*Row, error) {
	// Check for errors in parallel context
	if err := pep.parallelCtx.GetError(); err != nil {
		return nil, err
	}

	return pep.rootOperator.Next()
}

// Close cleans up parallel execution
func (pep *ParallelExecutionPlan) Close() error {
	var err error

	// Close root operator
	if pep.rootOperator != nil {
		err = pep.rootOperator.Close()
	}

	// Close parallel context
	if pep.parallelCtx != nil {
		if closeErr := pep.parallelCtx.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}

// Exchange operator handles data flow between parallel operators
type ExchangeOperator struct {
	baseOperator
	child      Operator
	outputChan chan *Row
	errorChan  chan error
	workerWG   sync.WaitGroup
	bgCtx      context.Context
	cancel     context.CancelFunc
	started    bool
	startMu    sync.Mutex
}

// NewExchangeOperator creates a new exchange operator
func NewExchangeOperator(child Operator, bufferSize int) *ExchangeOperator {
	bgCtx, cancel := context.WithCancel(context.Background())

	return &ExchangeOperator{
		baseOperator: baseOperator{
			schema: child.Schema(),
		},
		child:      child,
		outputChan: make(chan *Row, bufferSize),
		errorChan:  make(chan error, 1),
		bgCtx:      bgCtx,
		cancel:     cancel,
	}
}

// Open starts the exchange operator
func (e *ExchangeOperator) Open(ctx *ExecContext) error {
	e.ctx = ctx

	e.startMu.Lock()
	defer e.startMu.Unlock()

	if e.started {
		return nil
	}

	// Open child operator
	if err := e.child.Open(ctx); err != nil {
		return fmt.Errorf("failed to open child: %w", err)
	}

	// Start producer goroutine
	e.workerWG.Add(1)
	go e.producer()

	e.started = true
	return nil
}

// producer reads from child and writes to output channel
func (e *ExchangeOperator) producer() {
	defer e.workerWG.Done()
	defer close(e.outputChan)

	for {
		select {
		case <-e.bgCtx.Done():
			return
		default:
			row, err := e.child.Next()
			if err != nil {
				select {
				case e.errorChan <- err:
				case <-e.bgCtx.Done():
				}
				return
			}

			if row == nil {
				// EOF
				return
			}

			select {
			case e.outputChan <- row:
			case <-e.bgCtx.Done():
				return
			}
		}
	}
}

// Next returns the next row from the exchange
func (e *ExchangeOperator) Next() (*Row, error) {
	select {
	case row, ok := <-e.outputChan:
		if !ok {
			// Channel closed - check for error
			select {
			case err := <-e.errorChan:
				return nil, err
			default:
				return nil, nil // EOF
			}
		}
		return row, nil

	case err := <-e.errorChan:
		return nil, err

	case <-e.bgCtx.Done():
		return nil, e.bgCtx.Err()
	}
}

// Close cleans up the exchange operator
func (e *ExchangeOperator) Close() error {
	e.cancel()
	e.workerWG.Wait()

	if e.child != nil {
		return e.child.Close()
	}

	return nil
}

// ParallelResult interface for common result handling
type ParallelResult interface {
	GetRow() *Row
	GetError() error
}

// handleParallelNext provides common Next() implementation for parallel operators
func handleParallelNext[T ParallelResult](
	parallelCtx *ParallelContext,
	errorChan chan error,
	resultChan chan T,
	collectStats func(),
) (*Row, error) {
	// Check for errors in parallel context
	if err := parallelCtx.GetError(); err != nil {
		return nil, err
	}

	// Check for specific errors
	select {
	case err := <-errorChan:
		return nil, err
	default:
	}

	// Get next result
	select {
	case result, ok := <-resultChan:
		if !ok {
			return nil, nil // EOF
		}

		if result.GetError() != nil {
			return nil, result.GetError()
		}

		// Update statistics
		if collectStats != nil {
			collectStats()
		}

		return result.GetRow(), nil

	case err := <-errorChan:
		return nil, err

	case <-parallelCtx.Ctx.Done():
		return nil, parallelCtx.Ctx.Err()
	}
}
