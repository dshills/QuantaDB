package executor

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// ExternalSort implements external merge sort for large datasets
type ExternalSort struct {
	sortKeys    []int                // Column indices to sort by
	compareFn   func(*Row, *Row) int // Comparison function
	memoryLimit int64                // Memory limit in bytes
	tempDir     string               // Directory for temporary files
	spillFiles  []string             // List of spill files created
	rowEstimate int                  // Estimated bytes per row
}

// NewExternalSort creates a new external sort operator
func NewExternalSort(sortKeys []int, compareFn func(*Row, *Row) int) *ExternalSort {
	return &ExternalSort{
		sortKeys:    sortKeys,
		compareFn:   compareFn,
		memoryLimit: 100 * 1024 * 1024, // 100MB default
		tempDir:     os.TempDir(),
		rowEstimate: 100, // Assume 100 bytes per row initially
	}
}

// Sort sorts the input and returns a sorted iterator
func (es *ExternalSort) Sort(input SimpleRowIterator) (SimpleRowIterator, error) {
	// Create sorted runs
	runs, err := es.createSortedRuns(input)
	if err != nil {
		return nil, fmt.Errorf("failed to create sorted runs: %w", err)
	}

	// If only one run and it's in memory, return it directly
	if len(runs) == 1 {
		return runs[0], nil
	}

	// Merge multiple runs
	return es.mergeRuns(runs)
}

// createSortedRuns creates sorted runs from the input
func (es *ExternalSort) createSortedRuns(input SimpleRowIterator) ([]SimpleRowIterator, error) {
	var runs []SimpleRowIterator

	for {
		// Load batch up to memory limit
		batch, err := es.loadBatch(input)
		if err != nil {
			return nil, err
		}

		if len(batch) == 0 {
			break // No more input
		}

		// Sort batch in memory
		sort.Slice(batch, func(i, j int) bool {
			return es.compareFn(batch[i], batch[j]) < 0
		})

		// Decide whether to keep in memory or spill
		batchSize := es.estimateBatchSize(batch)
		if batchSize > es.memoryLimit/4 || len(runs) > 0 {
			// Spill to disk if batch is large or we already have spilled runs
			run, err := es.spillToDisk(batch)
			if err != nil {
				return nil, err
			}
			runs = append(runs, run)
		} else {
			// Keep in memory
			run := NewMemoryIterator(batch)
			runs = append(runs, run)
		}
	}

	return runs, nil
}

// loadBatch loads rows until memory limit is reached
func (es *ExternalSort) loadBatch(input SimpleRowIterator) ([]*Row, error) {
	var batch []*Row
	currentSize := int64(0)

	for currentSize < es.memoryLimit {
		row, err := input.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			break // End of input
		}

		batch = append(batch, row)
		currentSize += int64(es.rowEstimate)

		// Update row estimate based on actual data
		if len(batch) == 1 {
			es.rowEstimate = es.estimateRowSize(row)
		}
	}

	return batch, nil
}

// estimateRowSize estimates the memory size of a row
func (es *ExternalSort) estimateRowSize(row *Row) int {
	size := 8 // Overhead
	for _, val := range row.Values {
		switch val.Type() {
		case types.Integer:
			size += 8
		case types.Boolean:
			size++
		case types.Text:
			str, _ := val.AsString()
			size += len(str) + 8
		default:
			size += 32 // Conservative estimate
		}
	}
	return size
}

// estimateBatchSize estimates total memory used by a batch
func (es *ExternalSort) estimateBatchSize(batch []*Row) int64 {
	if len(batch) == 0 {
		return 0
	}
	return int64(len(batch) * es.rowEstimate)
}

// spillToDisk writes a sorted batch to a temporary file
func (es *ExternalSort) spillToDisk(batch []*Row) (SimpleRowIterator, error) {
	// Create temporary file
	tempFile, err := os.CreateTemp(es.tempDir, "quantadb_sort_*.tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	fileName := tempFile.Name()
	es.spillFiles = append(es.spillFiles, fileName)

	// Write rows to file
	writer := NewRowWriter(tempFile)
	for _, row := range batch {
		if err := writer.WriteRow(row); err != nil {
			tempFile.Close()
			return nil, fmt.Errorf("failed to write row: %w", err)
		}
	}

	if err := tempFile.Close(); err != nil {
		return nil, err
	}

	// Return iterator for the file
	return NewDiskIterator(fileName)
}

// mergeRuns merges multiple sorted runs into a single sorted stream
func (es *ExternalSort) mergeRuns(runs []SimpleRowIterator) (SimpleRowIterator, error) {
	if len(runs) == 0 {
		return NewMemoryIterator(nil), nil
	}

	if len(runs) == 1 {
		return runs[0], nil
	}

	// Use k-way merge
	return NewMergeIterator(runs, es.compareFn), nil
}

// Cleanup removes temporary files
func (es *ExternalSort) Cleanup() {
	for _, file := range es.spillFiles {
		os.Remove(file)
	}
	es.spillFiles = nil
}

// MemoryIterator iterates over rows in memory
type MemoryIterator struct {
	rows     []*Row
	position int
}

// NewMemoryIterator creates a new memory iterator
func NewMemoryIterator(rows []*Row) *MemoryIterator {
	return &MemoryIterator{
		rows:     rows,
		position: 0,
	}
}

func (m *MemoryIterator) Next() (*Row, error) {
	if m.position >= len(m.rows) {
		return nil, nil
	}
	row := m.rows[m.position]
	m.position++
	return row, nil
}

func (m *MemoryIterator) Close() error {
	m.rows = nil
	return nil
}

// DiskIterator iterates over rows stored in a file
type DiskIterator struct {
	fileName string
	file     *os.File
	reader   *RowReader
}

// NewDiskIterator creates a new disk iterator
func NewDiskIterator(fileName string) (*DiskIterator, error) {
	return &DiskIterator{
		fileName: fileName,
	}, nil
}

func (d *DiskIterator) Next() (*Row, error) {
	// Open file on first access
	if d.file == nil {
		file, err := os.Open(d.fileName)
		if err != nil {
			return nil, fmt.Errorf("failed to open spill file: %w", err)
		}
		d.file = file
		d.reader = NewRowReader(file)
	}

	return d.reader.ReadRow()
}

func (d *DiskIterator) Close() error {
	if d.file != nil {
		err := d.file.Close()
		d.file = nil
		d.reader = nil
		return err
	}
	return nil
}

// MergeIterator performs k-way merge of sorted iterators
type MergeIterator struct {
	iterators []SimpleRowIterator
	compareFn func(*Row, *Row) int
	heap      *mergeHeap
}

// NewMergeIterator creates a new merge iterator
func NewMergeIterator(iterators []SimpleRowIterator, compareFn func(*Row, *Row) int) *MergeIterator {
	return &MergeIterator{
		iterators: iterators,
		compareFn: compareFn,
	}
}

func (m *MergeIterator) Next() (*Row, error) {
	// Initialize heap on first call
	if m.heap == nil {
		if err := m.initialize(); err != nil {
			return nil, err
		}
	}

	// Get minimum element
	if m.heap.Len() == 0 {
		return nil, nil // EOF
	}

	// Pop minimum
	item := heap.Pop(m.heap).(*mergeItem)

	// Try to get next row from same iterator
	nextRow, err := m.iterators[item.iterIdx].Next()
	if err != nil {
		return nil, err
	}

	if nextRow != nil {
		// Push new item
		heap.Push(m.heap, &mergeItem{
			row:     nextRow,
			iterIdx: item.iterIdx,
		})
	}

	return item.row, nil
}

func (m *MergeIterator) initialize() error {
	h := &mergeHeap{
		items:     make([]*mergeItem, 0, len(m.iterators)),
		compareFn: m.compareFn,
	}

	// Get first row from each iterator
	for i, iter := range m.iterators {
		row, err := iter.Next()
		if err != nil {
			return err
		}
		if row != nil {
			h.items = append(h.items, &mergeItem{
				row:     row,
				iterIdx: i,
			})
		}
	}

	heap.Init(h)
	m.heap = h

	return nil
}

func (m *MergeIterator) Close() error {
	for _, iter := range m.iterators {
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

// mergeItem represents an item in the merge heap
type mergeItem struct {
	row     *Row
	iterIdx int
}

// mergeHeap implements heap.Interface for k-way merge
type mergeHeap struct {
	items     []*mergeItem
	compareFn func(*Row, *Row) int
}

func (h *mergeHeap) Len() int { return len(h.items) }

func (h *mergeHeap) Less(i, j int) bool {
	return h.compareFn(h.items[i].row, h.items[j].row) < 0
}

func (h *mergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*mergeItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// Row serialization for spilling to disk

// RowWriter writes rows to a file
type RowWriter struct {
	writer io.Writer
}

// NewRowWriter creates a new row writer
func NewRowWriter(w io.Writer) *RowWriter {
	return &RowWriter{writer: w}
}

// WriteRow writes a row to the file
func (rw *RowWriter) WriteRow(row *Row) error {
	// Write number of values
	numValues := int32(len(row.Values))
	if err := binary.Write(rw.writer, binary.LittleEndian, numValues); err != nil {
		return err
	}

	// Write each value
	for _, val := range row.Values {
		if err := rw.writeValue(val); err != nil {
			return err
		}
	}

	return nil
}

func (rw *RowWriter) writeValue(val types.Value) error {
	// Write type
	var typeID int8
	switch val.Type() {
	case types.Integer:
		typeID = 1
	case types.Boolean:
		typeID = 2
	case types.Text:
		typeID = 3
	default:
		typeID = 0 // Unknown
	}

	if err := binary.Write(rw.writer, binary.LittleEndian, typeID); err != nil {
		return err
	}

	// Write null flag
	if err := binary.Write(rw.writer, binary.LittleEndian, val.IsNull()); err != nil {
		return err
	}

	if val.IsNull() {
		return nil
	}

	// Write value based on type
	switch val.Type() {
	case types.Integer:
		v, _ := val.AsInt()
		return binary.Write(rw.writer, binary.LittleEndian, v)

	case types.Boolean:
		v, _ := val.AsBool()
		return binary.Write(rw.writer, binary.LittleEndian, v)

	case types.Text:
		v, _ := val.AsString()
		// Write length then string
		strLen := int32(len(v))
		if err := binary.Write(rw.writer, binary.LittleEndian, strLen); err != nil {
			return err
		}
		_, err := rw.writer.Write([]byte(v))
		return err

	default:
		return fmt.Errorf("unsupported type for serialization: %v", val.Type())
	}
}

// RowReader reads rows from a file
type RowReader struct {
	reader io.Reader
}

// NewRowReader creates a new row reader
func NewRowReader(r io.Reader) *RowReader {
	return &RowReader{reader: r}
}

// ReadRow reads a row from the file
func (rr *RowReader) ReadRow() (*Row, error) {
	// Read number of values
	var numValues int32
	if err := binary.Read(rr.reader, binary.LittleEndian, &numValues); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	// Read values
	values := make([]types.Value, numValues)
	for i := range values {
		val, err := rr.readValue()
		if err != nil {
			return nil, err
		}
		values[i] = val
	}

	return &Row{Values: values}, nil
}

func (rr *RowReader) readValue() (types.Value, error) {
	// Read type
	var typ int8
	if err := binary.Read(rr.reader, binary.LittleEndian, &typ); err != nil {
		return types.Value{}, err
	}

	// Read null flag
	var isNull bool
	if err := binary.Read(rr.reader, binary.LittleEndian, &isNull); err != nil {
		return types.Value{}, err
	}

	if isNull {
		return types.NewNullValue(), nil
	}

	// Read value based on type
	switch typ {
	case 1: // Integer
		var v int32
		if err := binary.Read(rr.reader, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewIntegerValue(v), nil

	case 2: // Boolean
		var v bool
		if err := binary.Read(rr.reader, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewBooleanValue(v), nil

	case 3: // Text
		// Read length then string
		var length int32
		if err := binary.Read(rr.reader, binary.LittleEndian, &length); err != nil {
			return types.Value{}, err
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(rr.reader, buf); err != nil {
			return types.Value{}, err
		}
		return types.NewTextValue(string(buf)), nil

	default:
		return types.Value{}, fmt.Errorf("unsupported type for deserialization: %v", typ)
	}
}
