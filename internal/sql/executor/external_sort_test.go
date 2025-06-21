package executor

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestExternalSortBasic(t *testing.T) {
	// Create test data
	rows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Charlie")}},
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Alice")}},
		{Values: []types.Value{types.NewIntegerValue(4), types.NewTextValue("David")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Bob")}},
	}

	input := NewMemoryIterator(rows)

	// Create external sort
	sorter := NewExternalSort([]int{0}, func(a, b *Row) int {
		return compareJoinValues(a.Values[0], b.Values[0])
	})
	defer sorter.Cleanup()

	sorted, err := sorter.Sort(input)
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()

	// Verify sorted order
	expected := []int32{1, 2, 3, 4}
	i := 0
	for {
		row, err := sorted.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if row == nil {
			break
		}
		id, _ := row.Values[0].AsInt()
		if id != expected[i] {
			t.Errorf("Position %d: expected %d, got %d", i, expected[i], id)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), i)
	}
}

func TestExternalSortWithSpilling(t *testing.T) {
	// Create larger dataset
	rows := make([]*Row, 1000)
	for i := 0; i < 1000; i++ {
		rows[i] = &Row{
			Values: []types.Value{
				types.NewIntegerValue(int32(rand.Intn(1000))),
				types.NewTextValue(fmt.Sprintf("Row_%d", i)),
			},
		}
	}

	input := NewMemoryIterator(rows)

	// Create external sort with small memory limit to force spilling
	sorter := NewExternalSort([]int{0}, func(a, b *Row) int {
		return compareJoinValues(a.Values[0], b.Values[0])
	})
	sorter.memoryLimit = 1024 // 1KB - will force multiple spill files
	defer sorter.Cleanup()

	sorted, err := sorter.Sort(input)
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()

	// Verify sorted order
	var prev int32 = -1
	count := 0
	for {
		row, err := sorted.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if row == nil {
			break
		}
		curr, _ := row.Values[0].AsInt()
		if curr < prev {
			t.Errorf("Out of order: %d came after %d", curr, prev)
		}
		prev = curr
		count++
	}

	if count != 1000 {
		t.Errorf("Expected 1000 rows, got %d", count)
	}

	// Verify spill files were created
	if len(sorter.spillFiles) == 0 {
		t.Error("Expected spill files to be created")
	}
}

func TestExternalSortMultiColumn(t *testing.T) {
	// Test sorting by multiple columns
	rows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("B"), types.NewIntegerValue(10)}},
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("C"), types.NewIntegerValue(20)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("A"), types.NewIntegerValue(30)}},
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("A"), types.NewIntegerValue(40)}},
	}

	input := NewMemoryIterator(rows)

	// Sort by columns 0 and 1 (id, name)
	sorter := NewExternalSort([]int{0, 1}, func(a, b *Row) int {
		// Compare first by column 0
		cmp := compareJoinValues(a.Values[0], b.Values[0])
		if cmp != 0 {
			return cmp
		}
		// Then by column 1
		return compareJoinValues(a.Values[1], b.Values[1])
	})
	defer sorter.Cleanup()

	sorted, err := sorter.Sort(input)
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()

	// Expected order: (1,A,40), (1,C,20), (2,A,30), (2,B,10)
	expected := []struct {
		id   int32
		name string
	}{
		{1, "A"},
		{1, "C"},
		{2, "A"},
		{2, "B"},
	}

	i := 0
	for {
		row, err := sorted.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if row == nil {
			break
		}
		id, _ := row.Values[0].AsInt()
		name, _ := row.Values[1].AsString()
		if id != expected[i].id || name != expected[i].name {
			t.Errorf("Position %d: expected (%d,%s), got (%d,%s)",
				i, expected[i].id, expected[i].name, id, name)
		}
		i++
	}
}

func TestExternalSortEmpty(t *testing.T) {
	input := NewMemoryIterator([]*Row{})

	sorter := NewExternalSort([]int{0}, func(a, b *Row) int {
		return compareJoinValues(a.Values[0], b.Values[0])
	})
	defer sorter.Cleanup()

	sorted, err := sorter.Sort(input)
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()

	row, err := sorted.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row != nil {
		t.Error("Expected no rows from empty input")
	}
}

func TestExternalSortCleanup(t *testing.T) {
	// Test that cleanup removes spill files
	rows := make([]*Row, 100)
	for i := 0; i < 100; i++ {
		rows[i] = &Row{
			Values: []types.Value{types.NewIntegerValue(int32(i))},
		}
	}

	input := NewMemoryIterator(rows)

	sorter := NewExternalSort([]int{0}, func(a, b *Row) int {
		return compareJoinValues(a.Values[0], b.Values[0])
	})
	sorter.memoryLimit = 100 // Force spilling

	sorted, err := sorter.Sort(input)
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	sorted.Close()

	// Verify spill files exist
	spillCount := len(sorter.spillFiles)
	if spillCount == 0 {
		t.Fatal("Expected spill files to be created")
	}

	// Get spill file paths before cleanup
	spillPaths := make([]string, len(sorter.spillFiles))
	copy(spillPaths, sorter.spillFiles)

	// Cleanup
	sorter.Cleanup()

	// Verify spill files are removed
	for _, path := range spillPaths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("Spill file %s was not cleaned up", path)
		}
	}
}

func TestKWayMerge(t *testing.T) {
	// Test the k-way merge functionality
	// Create sorted runs
	run1 := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1)}},
		{Values: []types.Value{types.NewIntegerValue(4)}},
		{Values: []types.Value{types.NewIntegerValue(7)}},
	}

	run2 := []*Row{
		{Values: []types.Value{types.NewIntegerValue(2)}},
		{Values: []types.Value{types.NewIntegerValue(5)}},
		{Values: []types.Value{types.NewIntegerValue(8)}},
	}

	run3 := []*Row{
		{Values: []types.Value{types.NewIntegerValue(3)}},
		{Values: []types.Value{types.NewIntegerValue(6)}},
		{Values: []types.Value{types.NewIntegerValue(9)}},
	}

	iters := []SimpleRowIterator{
		NewMemoryIterator(run1),
		NewMemoryIterator(run2),
		NewMemoryIterator(run3),
	}

	merger := NewMergeIterator(iters, func(a, b *Row) int {
		return compareJoinValues(a.Values[0], b.Values[0])
	})

	// Verify merged output is sorted
	expected := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	i := 0
	for {
		row, err := merger.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if row == nil {
			break
		}
		val, _ := row.Values[0].AsInt()
		if val != expected[i] {
			t.Errorf("Position %d: expected %d, got %d", i, expected[i], val)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d values, got %d", len(expected), i)
	}
}

func TestBufferedIterator(t *testing.T) {
	// Test the buffered iterator used in merge join
	rows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1)}},
		{Values: []types.Value{types.NewIntegerValue(2)}},
		{Values: []types.Value{types.NewIntegerValue(3)}},
	}

	iter := NewMemoryIterator(rows)
	buffered := NewBufferedIterator(iter)

	// Test Peek
	peeked, err := buffered.Peek()
	if err != nil {
		t.Fatalf("Peek failed: %v", err)
	}
	if peeked == nil {
		t.Fatal("Expected non-nil from Peek")
	}
	val1, _ := peeked.Values[0].AsInt()
	if val1 != 1 {
		t.Errorf("Expected 1 from Peek, got %d", val1)
	}

	// Peek again should return same value
	peeked2, _ := buffered.Peek()
	val2, _ := peeked2.Values[0].AsInt()
	if val2 != 1 {
		t.Errorf("Expected 1 from second Peek, got %d", val2)
	}

	// Next should return the peeked value
	next, err := buffered.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	val3, _ := next.Values[0].AsInt()
	if val3 != 1 {
		t.Errorf("Expected 1 from Next, got %d", val3)
	}

	// Now peek should return 2
	peeked3, _ := buffered.Peek()
	val4, _ := peeked3.Values[0].AsInt()
	if val4 != 2 {
		t.Errorf("Expected 2 from Peek after Next, got %d", val4)
	}
}

// TestMemoryIterator is a simple iterator over a slice of rows for testing
type TestMemoryIterator struct {
	rows []*Row
	pos  int
}

func NewTestMemoryIterator(rows []*Row) *TestMemoryIterator {
	return &TestMemoryIterator{rows: rows}
}

func (m *TestMemoryIterator) Next() (*Row, error) {
	if m.pos >= len(m.rows) {
		return nil, nil
	}
	row := m.rows[m.pos]
	m.pos++
	return row, nil
}

func (m *TestMemoryIterator) Close() error {
	return nil
}

// Benchmark external sort
func BenchmarkExternalSort(b *testing.B) {
	// Create test data
	rows := make([]*Row, 10000)
	for i := 0; i < 10000; i++ {
		rows[i] = &Row{
			Values: []types.Value{
				types.NewIntegerValue(int32(rand.Intn(10000))),
				types.NewTextValue(fmt.Sprintf("Row_%d", i)),
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input := NewMemoryIterator(rows)
		sorter := NewExternalSort([]int{0}, func(a, b *Row) int {
			return compareJoinValues(a.Values[0], b.Values[0])
		})
		defer sorter.Cleanup()

		sorted, err := sorter.Sort(input)
		if err != nil {
			b.Fatalf("Sort failed: %v", err)
		}

		// Consume all rows
		for {
			row, err := sorted.Next()
			if err != nil {
				b.Fatalf("Next failed: %v", err)
			}
			if row == nil {
				break
			}
		}
		sorted.Close()
	}
}
