package executor

// BitmapOperator is the interface for operators that produce bitmaps.
type BitmapOperator interface {
	Operator
	GetBitmap() *Bitmap
}

// Bitmap represents a set of row IDs for index intersection operations.
// It uses a map for simplicity, but could be optimized with bit arrays.
type Bitmap struct {
	rowIDs map[RowID]bool
}

// NewBitmap creates a new empty bitmap.
func NewBitmap() *Bitmap {
	return &Bitmap{
		rowIDs: make(map[RowID]bool),
	}
}

// Add adds a row ID to the bitmap.
func (b *Bitmap) Add(rowID RowID) {
	b.rowIDs[rowID] = true
}

// Contains checks if a row ID is in the bitmap.
func (b *Bitmap) Contains(rowID RowID) bool {
	return b.rowIDs[rowID]
}

// Count returns the number of row IDs in the bitmap.
func (b *Bitmap) Count() int {
	return len(b.rowIDs)
}

// And performs a logical AND operation with another bitmap.
func (b *Bitmap) And(other *Bitmap) *Bitmap {
	result := NewBitmap()

	// Result contains only IDs present in both bitmaps
	for rowID := range b.rowIDs {
		if other.Contains(rowID) {
			result.Add(rowID)
		}
	}

	return result
}

// Or performs a logical OR operation with another bitmap.
func (b *Bitmap) Or(other *Bitmap) *Bitmap {
	result := NewBitmap()

	// Add all IDs from first bitmap
	for rowID := range b.rowIDs {
		result.Add(rowID)
	}

	// Add all IDs from second bitmap
	for rowID := range other.rowIDs {
		result.Add(rowID)
	}

	return result
}

// ToSlice returns all row IDs as a sorted slice.
func (b *Bitmap) ToSlice() []RowID {
	result := make([]RowID, 0, len(b.rowIDs))

	// Convert map to slice
	for rowID := range b.rowIDs {
		result = append(result, rowID)
	}

	// Sort by PageID, then SlotID for consistent ordering
	// This ensures deterministic iteration order
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].PageID > result[j].PageID ||
				(result[i].PageID == result[j].PageID && result[i].SlotID > result[j].SlotID) {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}

// Clear removes all row IDs from the bitmap.
func (b *Bitmap) Clear() {
	b.rowIDs = make(map[RowID]bool)
}
