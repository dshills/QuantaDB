package executor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
)

// TestTask implements the Task interface for testing
type TestTask struct {
	id       int
	duration time.Duration
	result   chan int
	err      error
}

func (t *TestTask) Execute(ctx context.Context) error {
	if t.err != nil {
		return t.err
	}

	select {
	case <-time.After(t.duration):
		t.result <- t.id
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWorkerPool(t *testing.T) {
	workers := 2
	wp := NewWorkerPool(workers)
	defer wp.Close()

	// Submit tasks
	numTasks := 5
	resultChan := make(chan int, numTasks)

	for i := 0; i < numTasks; i++ {
		task := &TestTask{
			id:       i,
			duration: 10 * time.Millisecond,
			result:   resultChan,
		}

		err := wp.Submit(task)
		if err != nil {
			t.Fatalf("Failed to submit task %d: %v", i, err)
		}
	}

	// Collect results
	results := make(map[int]bool)
	for i := 0; i < numTasks; i++ {
		select {
		case result := <-resultChan:
			results[result] = true
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for task results")
		}
	}

	// Verify all tasks completed
	if len(results) != numTasks {
		t.Errorf("Expected %d results, got %d", numTasks, len(results))
	}

	for i := 0; i < numTasks; i++ {
		if !results[i] {
			t.Errorf("Task %d did not complete", i)
		}
	}
}

func TestWorkerPoolCancellation(t *testing.T) {
	wp := NewWorkerPool(1)
	defer wp.Close()

	// Submit a long-running task
	resultChan := make(chan int, 1)
	task := &TestTask{
		id:       1,
		duration: 1 * time.Second,
		result:   resultChan,
	}

	err := wp.Submit(task)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Close worker pool to cancel tasks
	time.Sleep(10 * time.Millisecond) // Let task start
	wp.Close()

	// Task should not complete
	select {
	case <-resultChan:
		t.Error("Task completed despite cancellation")
	case <-time.After(100 * time.Millisecond):
		// Expected - task was canceled
	}
}

func TestParallelContext(t *testing.T) {
	execCtx := &ExecContext{}
	pc := NewParallelContext(execCtx, 2)
	defer pc.Close()

	// Test basic properties
	if pc.MaxDOP != 2 {
		t.Errorf("Expected MaxDOP=2, got %d", pc.MaxDOP)
	}

	if pc.ExecCtx != execCtx {
		t.Error("ExecContext not set correctly")
	}

	if pc.WorkerPool == nil {
		t.Error("WorkerPool not created")
	}

	// Test error handling
	testErr := &TestError{"test error"}
	pc.SetError(testErr)

	if pc.GetError() != testErr {
		t.Error("Error not set correctly")
	}

	// Context should be canceled
	select {
	case <-pc.Ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Context not canceled after setting error")
	}
}

func TestExchangeOperator(t *testing.T) {
	// Create mock child operator
	mockChild := NewParallelTestMockOperator([]*Row{
		{Values: []types.Value{types.NewValue(int64(1))}},
		{Values: []types.Value{types.NewValue(int64(2))}},
		{Values: []types.Value{types.NewValue(int64(3))}},
	}, &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
		},
	})

	// Create exchange operator
	exchange := NewExchangeOperator(mockChild, 2)

	// Open exchange
	ctx := &ExecContext{}
	err := exchange.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open exchange: %v", err)
	}
	defer exchange.Close()

	// Read all rows
	var rows []*Row
	for {
		row, err := exchange.Next()
		if err != nil {
			t.Fatalf("Error reading from exchange: %v", err)
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}

	// Verify all rows received
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}

	// Verify row content
	for i, row := range rows {
		expectedValue := int64(i + 1)
		if row.Values[0].Data != expectedValue {
			t.Errorf("Row %d: expected %d, got %v", i, expectedValue, row.Values[0].Data)
		}
	}
}

func TestParallelScanOperator(t *testing.T) {
	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer},
			{Name: "name", DataType: types.Text},
		},
	}

	// Create mock storage backend
	mockStorage := &MockStorageBackend{
		rows: []*Row{
			{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("Alice")}},
			{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("Bob")}},
			{Values: []types.Value{types.NewValue(int64(3)), types.NewValue("Charlie")}},
			{Values: []types.Value{types.NewValue(int64(4)), types.NewValue("Diana")}},
		},
	}

	// Create parallel context
	execCtx := &ExecContext{}
	parallelCtx := NewParallelContext(execCtx, 2)
	defer parallelCtx.Close()

	// Create parallel scan operator
	scan := NewParallelScanOperator(table, mockStorage, parallelCtx)

	// Verify parallelizable interface
	if !scan.CanParallelize() {
		t.Error("ParallelScanOperator should be parallelizable")
	}

	if scan.GetParallelDegree() != 2 {
		t.Errorf("Expected parallel degree 2, got %d", scan.GetParallelDegree())
	}

	// Open scan
	err := scan.Open(execCtx)
	if err != nil {
		t.Fatalf("Failed to open parallel scan: %v", err)
	}
	defer scan.Close()

	// Read all rows
	var rows []*Row
	for {
		row, err := scan.Next()
		if err != nil {
			t.Fatalf("Error reading from parallel scan: %v", err)
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}

	// Verify we got some rows (parallel distribution may affect order/count)
	if len(rows) == 0 {
		t.Error("Expected at least some rows from parallel scan")
	}

	// Verify row structure
	for i, row := range rows {
		if len(row.Values) != 2 {
			t.Errorf("Row %d: expected 2 values, got %d", i, len(row.Values))
		}
	}
}

// Mock implementations for testing

type ParallelTestMockOperator struct {
	rows   []*Row
	schema *Schema
	index  int
	opened bool
}

func NewParallelTestMockOperator(rows []*Row, schema *Schema) *ParallelTestMockOperator {
	return &ParallelTestMockOperator{
		rows:   rows,
		schema: schema,
	}
}

func (m *ParallelTestMockOperator) Open(ctx *ExecContext) error {
	m.opened = true
	m.index = 0
	return nil
}

func (m *ParallelTestMockOperator) Next() (*Row, error) {
	if !m.opened {
		return nil, &TestError{"operator not opened"}
	}

	if m.index >= len(m.rows) {
		return nil, nil // EOF
	}

	row := m.rows[m.index]
	m.index++
	return row, nil
}

func (m *ParallelTestMockOperator) Close() error {
	m.opened = false
	return nil
}

func (m *ParallelTestMockOperator) Schema() *Schema {
	return m.schema
}

type MockStorageBackend struct {
	rows      []*Row
	txnID     uint64
	scanIndex int
}

func (m *MockStorageBackend) ScanTable(tableID int64, snapshotTS int64) (RowIterator, error) {
	return &MockRowIterator{rows: m.rows}, nil
}

func (m *MockStorageBackend) SetTransactionID(txnID uint64) {
	m.txnID = txnID
}

func (m *MockStorageBackend) GetBufferPoolStats() *storage.BufferPoolStats {
	return nil
}

// Implement other StorageBackend methods as needed
func (m *MockStorageBackend) InsertRow(tableID int64, row *Row) (RowID, error) {
	return RowID{}, nil
}

func (m *MockStorageBackend) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	return nil
}

func (m *MockStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	return nil
}

func (m *MockStorageBackend) CreateTable(table *catalog.Table) error {
	return nil
}

func (m *MockStorageBackend) DropTable(tableID int64) error {
	return nil
}

func (m *MockStorageBackend) GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error) {
	return nil, nil
}

type MockRowIterator struct {
	rows  []*Row
	index int
}

func (m *MockRowIterator) Next() (*Row, RowID, error) {
	if m.index >= len(m.rows) {
		return nil, RowID{}, nil // EOF
	}

	row := m.rows[m.index]
	m.index++
	return row, RowID{}, nil
}

func (m *MockRowIterator) Close() error {
	return nil
}

type TestError struct {
	message string
}

func (e *TestError) Error() string {
	return e.message
}

func TestParallelExecutionPlan(t *testing.T) {
	// Create mock root operator
	mockRoot := NewParallelTestMockOperator([]*Row{
		{Values: []types.Value{types.NewValue(int64(1))}},
		{Values: []types.Value{types.NewValue(int64(2))}},
	}, &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
		},
	})

	// Create parallel execution plan
	plan := NewParallelExecutionPlan(mockRoot, 2)

	// Open plan
	ctx := &ExecContext{}
	err := plan.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open parallel execution plan: %v", err)
	}
	defer plan.Close()

	// Read all rows
	var rows []*Row
	for {
		row, err := plan.Next()
		if err != nil {
			t.Fatalf("Error reading from parallel plan: %v", err)
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}

	// Verify rows
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

func TestConcurrentWorkerPool(t *testing.T) {
	wp := NewWorkerPool(4)
	defer wp.Close()

	numTasks := 100
	resultChan := make(chan int, numTasks)
	var wg sync.WaitGroup

	// Submit tasks concurrently
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task := &TestTask{
				id:       id,
				duration: time.Millisecond,
				result:   resultChan,
			}

			err := wp.Submit(task)
			if err != nil {
				t.Errorf("Failed to submit task %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// Collect results
	results := make(map[int]bool)
	for i := 0; i < numTasks; i++ {
		select {
		case result := <-resultChan:
			results[result] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for task results, got %d/%d", len(results), numTasks)
		}
	}

	// Verify all tasks completed
	if len(results) != numTasks {
		t.Errorf("Expected %d results, got %d", numTasks, len(results))
	}
}
