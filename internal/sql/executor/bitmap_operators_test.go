package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBitmapAndOperator tests the BitmapAndOperator functionality.
func TestBitmapAndOperator(t *testing.T) {
	// Create two bitmap sources
	bitmap1 := NewBitmap()
	bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 0})
	bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 1})
	bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 2})

	bitmap2 := NewBitmap()
	bitmap2.Add(RowID{PageID: storage.PageID(1), SlotID: 1})
	bitmap2.Add(RowID{PageID: storage.PageID(1), SlotID: 2})
	bitmap2.Add(RowID{PageID: storage.PageID(1), SlotID: 3})

	// Create mock bitmap operators
	mockOp1 := &mockBitmapOperator{bitmap: bitmap1}
	mockOp2 := &mockBitmapOperator{bitmap: bitmap2}

	// Create BitmapAndOperator
	andOp := NewBitmapAndOperator([]Operator{mockOp1, mockOp2})

	// Open the operator
	ctx := &ExecContext{Stats: &ExecStats{}}
	require.NoError(t, andOp.Open(ctx))

	// Get the result bitmap
	result := andOp.GetBitmap()
	assert.NotNil(t, result)
	assert.Equal(t, 2, result.Count())
	assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 1}))
	assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 2}))
	assert.False(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 0}))
	assert.False(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 3}))

	// Close the operator
	require.NoError(t, andOp.Close())
}

// TestBitmapOrOperator tests the BitmapOrOperator functionality.
func TestBitmapOrOperator(t *testing.T) {
	// Create two bitmap sources
	bitmap1 := NewBitmap()
	bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 0})
	bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 1})

	bitmap2 := NewBitmap()
	bitmap2.Add(RowID{PageID: storage.PageID(1), SlotID: 2})
	bitmap2.Add(RowID{PageID: storage.PageID(1), SlotID: 3})

	// Create mock bitmap operators
	mockOp1 := &mockBitmapOperator{bitmap: bitmap1}
	mockOp2 := &mockBitmapOperator{bitmap: bitmap2}

	// Create BitmapOrOperator
	orOp := NewBitmapOrOperator([]Operator{mockOp1, mockOp2})

	// Open the operator
	ctx := &ExecContext{Stats: &ExecStats{}}
	require.NoError(t, orOp.Open(ctx))

	// Get the result bitmap
	result := orOp.GetBitmap()
	assert.NotNil(t, result)
	assert.Equal(t, 4, result.Count())
	assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 0}))
	assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 1}))
	assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 2}))
	assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 3}))

	// Close the operator
	require.NoError(t, orOp.Close())
}

// TestBitmapHeapScanOperator tests the BitmapHeapScanOperator functionality.
func TestBitmapHeapScanOperator(t *testing.T) {
	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer, OrdinalPosition: 0},
			{Name: "name", DataType: types.Text, OrdinalPosition: 1},
			{Name: "age", DataType: types.Integer, OrdinalPosition: 2},
		},
	}

	// Create test bitmap
	bitmap := NewBitmap()
	bitmap.Add(RowID{PageID: storage.PageID(1), SlotID: 0})
	bitmap.Add(RowID{PageID: storage.PageID(1), SlotID: 2})

	// Create mock bitmap source
	mockSource := &mockBitmapOperator{bitmap: bitmap}

	// Create mock storage backend
	mockStorage := &mockBitmapStorageBackend{
		rows: map[RowID]*Row{
			{PageID: storage.PageID(1), SlotID: 0}: {
				Values: []types.Value{
					types.NewIntegerValue(1),
					types.NewTextValue("Alice"),
					types.NewIntegerValue(25),
				},
			},
			{PageID: storage.PageID(1), SlotID: 1}: {
				Values: []types.Value{
					types.NewIntegerValue(2),
					types.NewTextValue("Bob"),
					types.NewIntegerValue(30),
				},
			},
			{PageID: storage.PageID(1), SlotID: 2}: {
				Values: []types.Value{
					types.NewIntegerValue(3),
					types.NewTextValue("Charlie"),
					types.NewIntegerValue(35),
				},
			},
		},
	}

	// Create BitmapHeapScanOperator
	heapScan := NewBitmapHeapScanOperator(table, mockSource, mockStorage)

	// Open the operator
	ctx := &ExecContext{Stats: &ExecStats{}}
	require.NoError(t, heapScan.Open(ctx))

	// Read rows
	rows := make([]*Row, 0)
	for {
		row, err := heapScan.Next()
		require.NoError(t, err)
		if row == nil {
			break
		}
		rows = append(rows, row)
	}

	// Verify results
	assert.Equal(t, 2, len(rows))

	// First row (id=1)
	val, err := rows[0].Values[0].AsInt()
	require.NoError(t, err)
	assert.Equal(t, int32(1), val)

	strVal, err := rows[0].Values[1].AsString()
	require.NoError(t, err)
	assert.Equal(t, "Alice", strVal)

	val, err = rows[0].Values[2].AsInt()
	require.NoError(t, err)
	assert.Equal(t, int32(25), val)

	// Second row (id=3)
	val, err = rows[1].Values[0].AsInt()
	require.NoError(t, err)
	assert.Equal(t, int32(3), val)

	strVal, err = rows[1].Values[1].AsString()
	require.NoError(t, err)
	assert.Equal(t, "Charlie", strVal)

	val, err = rows[1].Values[2].AsInt()
	require.NoError(t, err)
	assert.Equal(t, int32(35), val)

	// Close the operator
	require.NoError(t, heapScan.Close())
}

// TestBitmapOperatorsIntegration tests the integration of bitmap operators.
func TestBitmapOperatorsIntegration(t *testing.T) {
	// Create three bitmap sources for a complex query:
	// WHERE age >= 25 AND age <= 35 AND (name = 'Alice' OR name = 'Charlie')

	// Bitmap 1: age >= 25 AND age <= 35
	bitmap1 := NewBitmap()
	bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 0}) // Alice, 25
	bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 1}) // Bob, 30
	bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 2}) // Charlie, 35

	// Bitmap 2: name = 'Alice'
	bitmap2 := NewBitmap()
	bitmap2.Add(RowID{PageID: storage.PageID(1), SlotID: 0}) // Alice

	// Bitmap 3: name = 'Charlie'
	bitmap3 := NewBitmap()
	bitmap3.Add(RowID{PageID: storage.PageID(1), SlotID: 2}) // Charlie

	// Create mock operators
	ageOp := &mockBitmapOperator{bitmap: bitmap1}
	aliceOp := &mockBitmapOperator{bitmap: bitmap2}
	charlieOp := &mockBitmapOperator{bitmap: bitmap3}

	// Create OR for (name = 'Alice' OR name = 'Charlie')
	nameOrOp := NewBitmapOrOperator([]Operator{aliceOp, charlieOp})

	// Create AND for the full predicate
	finalAndOp := NewBitmapAndOperator([]Operator{ageOp, nameOrOp})

	// Open and execute
	ctx := &ExecContext{Stats: &ExecStats{}}
	require.NoError(t, finalAndOp.Open(ctx))

	// Get result
	result := finalAndOp.GetBitmap()
	assert.NotNil(t, result)
	assert.Equal(t, 2, result.Count())
	assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 0}))  // Alice
	assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 2}))  // Charlie
	assert.False(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 1})) // Bob (excluded)

	// Close
	require.NoError(t, finalAndOp.Close())
}

// Helper types for testing

type mockBitmapOperator struct {
	baseOperator
	bitmap *Bitmap
	isOpen bool
}

func (m *mockBitmapOperator) Open(ctx *ExecContext) error {
	m.ctx = ctx
	m.isOpen = true
	return nil
}

func (m *mockBitmapOperator) Next() (*Row, error) {
	return nil, nil
}

func (m *mockBitmapOperator) Close() error {
	m.isOpen = false
	return nil
}

func (m *mockBitmapOperator) GetBitmap() *Bitmap {
	return m.bitmap
}

func (m *mockBitmapOperator) Schema() *Schema {
	return m.schema
}

func (m *mockBitmapOperator) Children() []Operator {
	return nil
}

type mockBitmapStorageBackend struct {
	rows map[RowID]*Row
}

func (m *mockBitmapStorageBackend) GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error) {
	row, ok := m.rows[rowID]
	if !ok {
		return nil, nil
	}
	return row, nil
}

// Implement remaining StorageBackend interface methods
func (m *mockBitmapStorageBackend) CreateTable(table *catalog.Table) error {
	return nil
}

func (m *mockBitmapStorageBackend) GetTable(tableID int64) (*catalog.Table, error) {
	return nil, nil
}

func (m *mockBitmapStorageBackend) InsertRow(tableID int64, row *Row) (RowID, error) {
	return RowID{}, nil
}

func (m *mockBitmapStorageBackend) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	return nil
}

func (m *mockBitmapStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	return nil
}

func (m *mockBitmapStorageBackend) ScanTable(tableID int64, snapshotTS int64) (RowIterator, error) {
	return nil, nil
}

func (m *mockBitmapStorageBackend) DropTable(tableID int64) error {
	return nil
}

func (m *mockBitmapStorageBackend) SetTransactionID(txnID uint64) {
	// No-op for mock
}
