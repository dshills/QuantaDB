package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// IORequest represents a disk I/O request
type IORequest struct {
	Type      IORequestType
	PageID    PageID
	Data      []byte
	Result    chan IOResult
	Priority  IOPriority
	Timestamp time.Time
}

// IORequestType represents the type of I/O operation
type IORequestType int

const (
	IORead IORequestType = iota
	IOWrite
	IOSync
)

// IOPriority represents the priority of I/O requests
type IOPriority int

const (
	IOPriorityLow IOPriority = iota
	IOPriorityNormal
	IOPriorityHigh
	IOPriorityCritical
)

// IOResult represents the result of an I/O operation
type IOResult struct {
	Page  *Page
	Error error
}

// IOStats tracks I/O performance metrics
type IOStats struct {
	RequestsProcessed int64
	BytesRead         int64
	BytesWritten      int64
	AverageLatency    time.Duration
	QueueDepth        int
	TotalLatency      time.Duration
}

// ParallelIOManager manages parallel I/O operations
type ParallelIOManager struct {
	diskManager  *DiskManager
	requestQueue chan *IORequest
	workers      []*IOWorker
	workerCount  int
	stats        *IOStats
	statsMu      sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	batchSize    int
	batchTimeout time.Duration
}

// IOWorker processes I/O requests
type IOWorker struct {
	id           int
	manager      *ParallelIOManager
	requestBatch []*IORequest
	batchMutex   sync.Mutex
}

// NewParallelIOManager creates a new parallel I/O manager
func NewParallelIOManager(diskManager *DiskManager, workerCount int) *ParallelIOManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &ParallelIOManager{
		diskManager:  diskManager,
		requestQueue: make(chan *IORequest, 1000), // Buffered channel for requests
		workerCount:  workerCount,
		stats:        &IOStats{},
		ctx:          ctx,
		cancel:       cancel,
		batchSize:    10,                   // Process up to 10 requests per batch
		batchTimeout: 5 * time.Millisecond, // Max wait time for batch completion
	}
}

// Start initializes and starts the I/O workers
func (pim *ParallelIOManager) Start() error {
	pim.workers = make([]*IOWorker, pim.workerCount)

	for i := 0; i < pim.workerCount; i++ {
		worker := &IOWorker{
			id:           i,
			manager:      pim,
			requestBatch: make([]*IORequest, 0, pim.batchSize),
		}
		pim.workers[i] = worker

		pim.wg.Add(1)
		go worker.run()
	}

	return nil
}

// Stop gracefully shuts down the I/O manager
func (pim *ParallelIOManager) Stop() error {
	// Cancel context to signal workers to stop
	pim.cancel()

	// Close request queue
	close(pim.requestQueue)

	// Wait for all workers to finish
	pim.wg.Wait()

	return nil
}

// ReadPageAsync reads a page asynchronously
func (pim *ParallelIOManager) ReadPageAsync(pageID PageID, priority IOPriority) <-chan IOResult {
	resultChan := make(chan IOResult, 1)

	request := &IORequest{
		Type:      IORead,
		PageID:    pageID,
		Result:    resultChan,
		Priority:  priority,
		Timestamp: time.Now(),
	}

	select {
	case pim.requestQueue <- request:
		// Request queued successfully
	default:
		// Queue is full, return error immediately
		resultChan <- IOResult{Error: fmt.Errorf("I/O queue is full")}
		close(resultChan)
	}

	return resultChan
}

// WritePageAsync writes a page asynchronously
func (pim *ParallelIOManager) WritePageAsync(page *Page, priority IOPriority) <-chan IOResult {
	resultChan := make(chan IOResult, 1)

	request := &IORequest{
		Type:      IOWrite,
		PageID:    page.Header.PageID,
		Data:      page.Serialize(),
		Result:    resultChan,
		Priority:  priority,
		Timestamp: time.Now(),
	}

	select {
	case pim.requestQueue <- request:
		// Request queued successfully
	default:
		// Queue is full, return error immediately
		resultChan <- IOResult{Error: fmt.Errorf("I/O queue is full")}
		close(resultChan)
	}

	return resultChan
}

// ReadPagesBatch reads multiple pages in a single batch operation
func (pim *ParallelIOManager) ReadPagesBatch(pageIDs []PageID, priority IOPriority) map[PageID]<-chan IOResult {
	results := make(map[PageID]<-chan IOResult)

	for _, pageID := range pageIDs {
		results[pageID] = pim.ReadPageAsync(pageID, priority)
	}

	return results
}

// SyncAsync performs a sync operation asynchronously
func (pim *ParallelIOManager) SyncAsync(priority IOPriority) <-chan IOResult {
	resultChan := make(chan IOResult, 1)

	request := &IORequest{
		Type:      IOSync,
		Result:    resultChan,
		Priority:  priority,
		Timestamp: time.Now(),
	}

	select {
	case pim.requestQueue <- request:
		// Request queued successfully
	default:
		// Queue is full, return error immediately
		resultChan <- IOResult{Error: fmt.Errorf("I/O queue is full")}
		close(resultChan)
	}

	return resultChan
}

// GetStats returns I/O performance statistics
func (pim *ParallelIOManager) GetStats() IOStats {
	pim.statsMu.RLock()
	defer pim.statsMu.RUnlock()

	// Return a proper copy to avoid lock copying
	stats := IOStats{
		RequestsProcessed: pim.stats.RequestsProcessed,
		BytesRead:         pim.stats.BytesRead,
		BytesWritten:      pim.stats.BytesWritten,
		AverageLatency:    pim.stats.AverageLatency,
		TotalLatency:      pim.stats.TotalLatency,
		QueueDepth:        len(pim.requestQueue),
	}

	return stats
}

// SetBatchConfig configures batching parameters
func (pim *ParallelIOManager) SetBatchConfig(batchSize int, batchTimeout time.Duration) {
	pim.batchSize = batchSize
	pim.batchTimeout = batchTimeout
}

// IOWorker methods

// run is the main worker loop
func (w *IOWorker) run() {
	defer w.manager.wg.Done()

	ticker := time.NewTicker(w.manager.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-w.manager.ctx.Done():
			// Process remaining batch before exiting
			w.processBatch()
			return

		case request, ok := <-w.manager.requestQueue:
			if !ok {
				// Channel closed, process remaining batch and exit
				w.processBatch()
				return
			}

			w.addToBatch(request)

		case <-ticker.C:
			// Timeout reached, process current batch
			w.processBatch()
		}
	}
}

// addToBatch adds a request to the current batch
func (w *IOWorker) addToBatch(request *IORequest) {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()

	w.requestBatch = append(w.requestBatch, request)

	// Process batch if it's full
	if len(w.requestBatch) >= w.manager.batchSize {
		w.processBatchUnsafe()
	}
}

// processBatch processes the current batch of requests
func (w *IOWorker) processBatch() {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()
	w.processBatchUnsafe()
}

// processBatchUnsafe processes batch without locking (must be called with lock held)
func (w *IOWorker) processBatchUnsafe() {
	if len(w.requestBatch) == 0 {
		return
	}

	startTime := time.Now()

	// Sort batch by priority and request type for optimization
	w.sortBatch()

	// Process each request in the batch
	for _, request := range w.requestBatch {
		w.processRequest(request, startTime)
	}

	// Clear the batch
	w.requestBatch = w.requestBatch[:0]
}

// sortBatch sorts requests by priority and type for optimal processing
func (w *IOWorker) sortBatch() {
	// Simple priority-based sorting
	// Higher priority requests are processed first
	// Within same priority, group by operation type

	for i := 0; i < len(w.requestBatch)-1; i++ {
		for j := i + 1; j < len(w.requestBatch); j++ {
			if w.shouldSwap(w.requestBatch[i], w.requestBatch[j]) {
				w.requestBatch[i], w.requestBatch[j] = w.requestBatch[j], w.requestBatch[i]
			}
		}
	}
}

// shouldSwap determines if two requests should be swapped for optimal ordering
func (w *IOWorker) shouldSwap(a, b *IORequest) bool {
	// Higher priority first
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}

	// Within same priority, group operations (reads together, writes together)
	if a.Type != b.Type {
		return a.Type > b.Type // Reads (0) before writes (1)
	}

	// Within same operation type, order by page ID for sequential access
	return a.PageID > b.PageID
}

// processRequest processes a single I/O request
func (w *IOWorker) processRequest(request *IORequest, batchStartTime time.Time) {
	var result IOResult

	switch request.Type {
	case IORead:
		page, err := w.manager.diskManager.ReadPage(request.PageID)
		result = IOResult{Page: page, Error: err}

		if err == nil {
			w.updateReadStats(len(page.Serialize()))
		}

	case IOWrite:
		// Deserialize page from data if needed
		page := &Page{}
		if err := page.Deserialize(request.Data); err != nil {
			result = IOResult{Error: fmt.Errorf("failed to deserialize page: %w", err)}
		} else {
			err := w.manager.diskManager.WritePage(page)
			result = IOResult{Error: err}

			if err == nil {
				w.updateWriteStats(len(request.Data))
			}
		}

	case IOSync:
		err := w.manager.diskManager.Sync()
		result = IOResult{Error: err}
	}

	// Update latency statistics
	latency := time.Since(request.Timestamp)
	w.updateLatencyStats(latency)

	// Send result back
	select {
	case request.Result <- result:
		// Result sent successfully
	default:
		// Result channel might be closed, ignore
	}

	close(request.Result)
}

// updateReadStats updates read statistics
func (w *IOWorker) updateReadStats(bytesRead int) {
	w.manager.statsMu.Lock()
	defer w.manager.statsMu.Unlock()

	w.manager.stats.BytesRead += int64(bytesRead)
	w.manager.stats.RequestsProcessed++
}

// updateWriteStats updates write statistics
func (w *IOWorker) updateWriteStats(bytesWritten int) {
	w.manager.statsMu.Lock()
	defer w.manager.statsMu.Unlock()

	w.manager.stats.BytesWritten += int64(bytesWritten)
	w.manager.stats.RequestsProcessed++
}

// updateLatencyStats updates latency statistics
func (w *IOWorker) updateLatencyStats(latency time.Duration) {
	w.manager.statsMu.Lock()
	defer w.manager.statsMu.Unlock()

	w.manager.stats.TotalLatency += latency
	if w.manager.stats.RequestsProcessed > 0 {
		w.manager.stats.AverageLatency = w.manager.stats.TotalLatency / time.Duration(w.manager.stats.RequestsProcessed)
	}
}
