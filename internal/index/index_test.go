package index

import (
	"bytes"
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestBTreeIndexBasicOperations(t *testing.T) {
	idx := NewBTreeIndex(false, false) // non-unique, non-nullable

	// Test insert and search
	testData := []struct {
		key   []byte
		rowID []byte
	}{
		{[]byte("key1"), []byte("row1")},
		{[]byte("key2"), []byte("row2")},
		{[]byte("key3"), []byte("row3")},
	}

	// Insert data
	for _, item := range testData {
		if err := idx.Insert(item.key, item.rowID); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Search for existing keys
	for _, item := range testData {
		rowIDs, err := idx.Search(item.key)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(rowIDs) != 1 {
			t.Errorf("Expected 1 row ID, got %d", len(rowIDs))
		}
		if !bytes.Equal(rowIDs[0], item.rowID) {
			t.Errorf("Expected row ID %s, got %s", item.rowID, rowIDs[0])
		}
	}

	// Search for non-existing key
	rowIDs, err := idx.Search([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(rowIDs) != 0 {
		t.Error("Expected no results for non-existing key")
	}

	// Check statistics
	stats := idx.Stats()
	if stats.TotalEntries != 3 {
		t.Errorf("Expected 3 entries, got %d", stats.TotalEntries)
	}
}

func TestBTreeIndexUnique(t *testing.T) {
	idx := NewBTreeIndex(true, false) // unique, non-nullable

	// Insert first value
	if err := idx.Insert([]byte("key1"), []byte("row1")); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Try to insert duplicate key with different row ID
	err := idx.Insert([]byte("key1"), []byte("row2"))
	if err == nil {
		t.Error("Expected error for duplicate key in unique index")
	}

	// Allow same key-rowID pair (idempotent)
	if err := idx.Insert([]byte("key1"), []byte("row1")); err != nil {
		t.Error("Should allow same key-rowID pair in unique index")
	}
}

func TestBTreeIndexNullHandling(t *testing.T) {
	// Test non-nullable index
	idx1 := NewBTreeIndex(false, false)
	err := idx1.Insert(nil, []byte("row1"))
	if err == nil {
		t.Error("Expected error for NULL in non-nullable index")
	}

	// Test nullable index
	idx2 := NewBTreeIndex(false, true)
	if err := idx2.Insert(nil, []byte("row1")); err != nil {
		t.Fatalf("Failed to insert NULL: %v", err)
	}

	// Search for NULL value
	rowIDs, err := idx2.Search(nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(rowIDs) != 1 {
		t.Error("Expected to find NULL value")
	}
}

func TestBTreeIndexRange(t *testing.T) {
	idx := NewBTreeIndex(false, false)

	// Insert sequential data
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		rowID := []byte{byte(i + 100)}
		if err := idx.Insert(key, rowID); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Range query
	entries, err := idx.Range([]byte{2}, []byte{5})
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}

	// Should return 4 entries (2, 3, 4, 5)
	if len(entries) != 4 {
		t.Errorf("Expected 4 entries, got %d", len(entries))
	}

	// Verify entries are in order
	for i := 0; i < len(entries); i++ {
		expectedKey := byte(i + 2)
		expectedRowID := byte(i + 102)
		if entries[i].Key[0] != expectedKey {
			t.Errorf("Expected key %d, got %d", expectedKey, entries[i].Key[0])
		}
		if entries[i].RowID[0] != expectedRowID {
			t.Errorf("Expected row ID %d, got %d", expectedRowID, entries[i].RowID[0])
		}
	}
}

func TestBTreeIndexDelete(t *testing.T) {
	idx := NewBTreeIndex(false, false)

	// Insert and delete
	key := []byte("test")
	rowID := []byte("row1")

	if err := idx.Insert(key, rowID); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify it exists
	rowIDs, _ := idx.Search(key)
	if len(rowIDs) != 1 {
		t.Error("Key not found after insert")
	}

	// Delete
	if err := idx.Delete(key); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify it's gone
	rowIDs, _ = idx.Search(key)
	if len(rowIDs) != 0 {
		t.Error("Key still found after delete")
	}

	// Check statistics
	stats := idx.Stats()
	if stats.TotalEntries != 0 {
		t.Errorf("Expected 0 entries after delete, got %d", stats.TotalEntries)
	}
}

func TestKeyEncoderBasicTypes(t *testing.T) {
	encoder := KeyEncoder{}

	tests := []struct {
		name  string
		value types.Value
	}{
		{
			name:  "Integer",
			value: types.NewValue(int32(42)),
		},
		{
			name:  "BigInt",
			value: types.NewValue(int64(12345678901234)),
		},
		{
			name:  "SmallInt",
			value: types.NewValue(int16(123)),
		},
		{
			name:  "Boolean true",
			value: types.NewValue(true),
		},
		{
			name:  "Boolean false",
			value: types.NewValue(false),
		},
		{
			name:  "Text",
			value: types.NewValue("hello world"),
		},
		{
			name:  "VarChar",
			value: types.NewValue("test"),
		},
		{
			name:  "Timestamp",
			value: types.NewValue(int64(1234567890)),
		},
		{
			name:  "NULL",
			value: types.NewNullValue(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encoder.EncodeValue(tt.value)
			if err != nil {
				t.Fatalf("Failed to encode %s: %v", tt.name, err)
			}

			// NULL values should encode to nil
			if tt.value.IsNull() && encoded != nil {
				t.Error("Expected nil for NULL value")
			}

			// Non-NULL values should encode to non-nil
			if !tt.value.IsNull() && encoded == nil {
				t.Error("Expected non-nil for non-NULL value")
			}
		})
	}
}

func TestKeyEncoderOrdering(t *testing.T) {
	encoder := KeyEncoder{}

	// Test integer ordering
	val1 := types.NewValue(int32(10))
	val2 := types.NewValue(int32(20))
	val3 := types.NewValue(int32(30))

	enc1, _ := encoder.EncodeValue(val1)
	enc2, _ := encoder.EncodeValue(val2)
	enc3, _ := encoder.EncodeValue(val3)

	if bytes.Compare(enc1, enc2) >= 0 {
		t.Error("Expected enc1 < enc2")
	}
	if bytes.Compare(enc2, enc3) >= 0 {
		t.Error("Expected enc2 < enc3")
	}

	// Test string ordering
	str1 := types.NewValue("apple")
	str2 := types.NewValue("banana")
	str3 := types.NewValue("cherry")

	strEnc1, _ := encoder.EncodeValue(str1)
	strEnc2, _ := encoder.EncodeValue(str2)
	strEnc3, _ := encoder.EncodeValue(str3)

	if bytes.Compare(strEnc1, strEnc2) >= 0 {
		t.Error("Expected apple < banana")
	}
	if bytes.Compare(strEnc2, strEnc3) >= 0 {
		t.Error("Expected banana < cherry")
	}
}

func TestKeyEncoderMultiColumn(t *testing.T) {
	encoder := KeyEncoder{}

	// Test composite key encoding
	values := []types.Value{
		types.NewValue(int32(42)),
		types.NewValue("hello"),
		types.NewValue(true),
	}

	encoded, err := encoder.EncodeMultiColumn(values)
	if err != nil {
		t.Fatalf("Failed to encode multi-column: %v", err)
	}

	if encoded == nil {
		t.Error("Expected non-nil encoding for multi-column")
	}

	// Test with NULL value
	valuesWithNull := []types.Value{
		types.NewValue(int32(42)),
		types.NewNullValue(),
		types.NewValue(true),
	}

	encodedWithNull, err := encoder.EncodeMultiColumn(valuesWithNull)
	if err != nil {
		t.Fatalf("Failed to encode multi-column with NULL: %v", err)
	}

	if encodedWithNull == nil {
		t.Error("Expected non-nil encoding for multi-column with NULL")
	}

	// Encodings should be different
	if bytes.Equal(encoded, encodedWithNull) {
		t.Error("Expected different encodings for different values")
	}
}

func BenchmarkIndexInsert(b *testing.B) {
	idx := NewBTreeIndex(false, false)
	encoder := KeyEncoder{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := types.NewValue(int32(i))
		key, _ := encoder.EncodeValue(val)
		rowID := []byte{byte(i % 256), byte((i >> 8) % 256)}
		idx.Insert(key, rowID)
	}
}

func BenchmarkIndexSearch(b *testing.B) {
	idx := NewBTreeIndex(false, false)
	encoder := KeyEncoder{}

	// Pre-populate index
	for i := 0; i < 100000; i++ {
		val := types.NewValue(int32(i))
		key, _ := encoder.EncodeValue(val)
		rowID := []byte{byte(i % 256), byte((i >> 8) % 256)}
		idx.Insert(key, rowID)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := types.NewValue(int32(i % 100000))
		key, _ := encoder.EncodeValue(val)
		idx.Search(key)
	}
}
