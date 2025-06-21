package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestBitmap(t *testing.T) {
	// Test basic bitmap operations
	t.Run("Basic Operations", func(t *testing.T) {
		bitmap := NewBitmap()

		// Add some row IDs
		rowID1 := RowID{PageID: storage.PageID(1), SlotID: 10}
		rowID2 := RowID{PageID: storage.PageID(1), SlotID: 20}
		rowID3 := RowID{PageID: storage.PageID(2), SlotID: 5}

		bitmap.Add(rowID1)
		bitmap.Add(rowID2)
		bitmap.Add(rowID3)

		// Test Contains
		assert.True(t, bitmap.Contains(rowID1))
		assert.True(t, bitmap.Contains(rowID2))
		assert.True(t, bitmap.Contains(rowID3))
		assert.False(t, bitmap.Contains(RowID{PageID: storage.PageID(3), SlotID: 1}))

		// Test Count
		assert.Equal(t, 3, bitmap.Count())
	})

	// Test AND operation
	t.Run("AND Operation", func(t *testing.T) {
		bitmap1 := NewBitmap()
		bitmap2 := NewBitmap()

		// Set up bitmap1: {(1,10), (1,20), (2,5)}
		bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 10})
		bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 20})
		bitmap1.Add(RowID{PageID: storage.PageID(2), SlotID: 5})

		// Set up bitmap2: {(1,20), (2,5), (3,1)}
		bitmap2.Add(RowID{PageID: storage.PageID(1), SlotID: 20})
		bitmap2.Add(RowID{PageID: storage.PageID(2), SlotID: 5})
		bitmap2.Add(RowID{PageID: storage.PageID(3), SlotID: 1})

		// AND should give us: {(1,20), (2,5)}
		result := bitmap1.And(bitmap2)
		assert.Equal(t, 2, result.Count())
		assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 20}))
		assert.True(t, result.Contains(RowID{PageID: storage.PageID(2), SlotID: 5}))
		assert.False(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 10}))
		assert.False(t, result.Contains(RowID{PageID: storage.PageID(3), SlotID: 1}))
	})

	// Test OR operation
	t.Run("OR Operation", func(t *testing.T) {
		bitmap1 := NewBitmap()
		bitmap2 := NewBitmap()

		// Set up bitmap1: {(1,10), (1,20)}
		bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 10})
		bitmap1.Add(RowID{PageID: storage.PageID(1), SlotID: 20})

		// Set up bitmap2: {(2,5), (3,1)}
		bitmap2.Add(RowID{PageID: storage.PageID(2), SlotID: 5})
		bitmap2.Add(RowID{PageID: storage.PageID(3), SlotID: 1})

		// OR should give us all: {(1,10), (1,20), (2,5), (3,1)}
		result := bitmap1.Or(bitmap2)
		assert.Equal(t, 4, result.Count())
		assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 10}))
		assert.True(t, result.Contains(RowID{PageID: storage.PageID(1), SlotID: 20}))
		assert.True(t, result.Contains(RowID{PageID: storage.PageID(2), SlotID: 5}))
		assert.True(t, result.Contains(RowID{PageID: storage.PageID(3), SlotID: 1}))
	})

	// Test ToSlice with sorting
	t.Run("ToSlice Sorting", func(t *testing.T) {
		bitmap := NewBitmap()

		// Add in non-sorted order
		bitmap.Add(RowID{PageID: storage.PageID(2), SlotID: 5})
		bitmap.Add(RowID{PageID: storage.PageID(1), SlotID: 20})
		bitmap.Add(RowID{PageID: storage.PageID(1), SlotID: 10})
		bitmap.Add(RowID{PageID: storage.PageID(3), SlotID: 1})

		// Should be sorted by PageID, then SlotID
		slice := bitmap.ToSlice()
		assert.Equal(t, 4, len(slice))
		assert.Equal(t, RowID{PageID: storage.PageID(1), SlotID: 10}, slice[0])
		assert.Equal(t, RowID{PageID: storage.PageID(1), SlotID: 20}, slice[1])
		assert.Equal(t, RowID{PageID: storage.PageID(2), SlotID: 5}, slice[2])
		assert.Equal(t, RowID{PageID: storage.PageID(3), SlotID: 1}, slice[3])
	})

	// Test Clear
	t.Run("Clear", func(t *testing.T) {
		bitmap := NewBitmap()
		bitmap.Add(RowID{PageID: storage.PageID(1), SlotID: 10})
		bitmap.Add(RowID{PageID: storage.PageID(2), SlotID: 20})

		assert.Equal(t, 2, bitmap.Count())

		bitmap.Clear()
		assert.Equal(t, 0, bitmap.Count())
		assert.False(t, bitmap.Contains(RowID{PageID: storage.PageID(1), SlotID: 10}))
	})
}
