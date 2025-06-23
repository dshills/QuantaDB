// Package index provides indexing functionality for database tables.
package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// IndexType represents the type of index.
type IndexType int //nolint:revive // Established API

const (
	// BTreeIndex is a B+Tree based index.
	BTreeIndex IndexType = iota
	// HashIndex is a hash-based index (future).
	HashIndex
	// BitmapIndex is a bitmap index (future).
	BitmapIndex
)

// Index is the interface for database indexes.
type Index interface {
	// Insert adds a key-value pair to the index.
	// The key is the indexed column value(s), value is the row ID.
	Insert(key []byte, rowID []byte) error

	// Delete removes a key from the index.
	Delete(key []byte) error

	// Search finds all row IDs for a given key.
	Search(key []byte) ([][]byte, error)

	// Range returns all key-value pairs in the given range.
	Range(startKey, endKey []byte) ([]IndexEntry, error)

	// Type returns the index type.
	Type() IndexType

	// Stats returns index statistics.
	Stats() IndexStats
}

// IndexEntry represents a key-value pair in an index.
type IndexEntry struct { //nolint:revive // Established API
	Key   []byte
	RowID []byte
}

// IndexStats contains statistics about an index.
type IndexStats struct { //nolint:revive // Established API
	Type         IndexType
	TotalEntries int64
	StorageBytes int64
	Height       int // For tree-based indexes
	LastUpdated  int64
}

// BTreeIndexImpl implements Index using a B+Tree.
type BTreeIndexImpl struct {
	mu       sync.RWMutex
	tree     *BTree
	unique   bool
	nullable bool
	stats    IndexStats
}

// NewBTreeIndex creates a new B+Tree index.
func NewBTreeIndex(unique, nullable bool) *BTreeIndexImpl {
	return &BTreeIndexImpl{
		tree:     NewBTree(DefaultComparator),
		unique:   unique,
		nullable: nullable,
		stats: IndexStats{
			Type: BTreeIndex,
		},
	}
}

// Insert adds a key-value pair to the index.
func (idx *BTreeIndexImpl) Insert(key []byte, rowID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Handle NULL keys
	if key == nil {
		if !idx.nullable {
			return fmt.Errorf("NULL values not allowed in this index")
		}
		// Convert nil to special NULL marker
		key = []byte{0xFF, 0xFF, 0xFF, 0xFF}
	}

	if idx.unique {
		// Check for duplicate key
		existing, found := idx.tree.Search(key)
		if found && !bytes.Equal(existing, rowID) {
			return fmt.Errorf("duplicate key value violates unique constraint")
		}
	}

	err := idx.tree.Insert(key, rowID)
	if err == nil {
		idx.stats.TotalEntries++
	}
	return err
}

// Delete removes a key from the index.
func (idx *BTreeIndexImpl) Delete(key []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Handle NULL keys
	if key == nil {
		key = []byte{0xFF, 0xFF, 0xFF, 0xFF}
	}

	err := idx.tree.Delete(key)
	if err == nil {
		idx.stats.TotalEntries--
	}
	return err
}

// Search finds all row IDs for a given key.
func (idx *BTreeIndexImpl) Search(key []byte) ([][]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Handle NULL keys
	if key == nil {
		key = []byte{0xFF, 0xFF, 0xFF, 0xFF}
	}

	rowID, found := idx.tree.Search(key)
	if !found {
		return nil, nil
	}

	return [][]byte{rowID}, nil
}

// Range returns all key-value pairs in the given range.
func (idx *BTreeIndexImpl) Range(startKey, endKey []byte) ([]IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Handle NULL keys
	if startKey == nil {
		startKey = []byte{0xFF, 0xFF, 0xFF, 0xFF}
	}
	if endKey == nil {
		endKey = []byte{0xFF, 0xFF, 0xFF, 0xFF}
	}

	pairs, err := idx.tree.Range(startKey, endKey)
	if err != nil {
		return nil, err
	}

	entries := make([]IndexEntry, len(pairs))
	for i, pair := range pairs {
		entries[i] = IndexEntry{
			Key:   pair.Key,
			RowID: pair.Value,
		}
	}

	return entries, nil
}

// Type returns the index type.
func (idx *BTreeIndexImpl) Type() IndexType {
	return BTreeIndex
}

// Stats returns index statistics.
func (idx *BTreeIndexImpl) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	treeStats := idx.tree.Stats()
	idx.stats.Height = treeStats.Height

	return idx.stats
}

// KeyEncoder provides methods to encode SQL values into index keys.
type KeyEncoder struct{}

// EncodeValue encodes a SQL value into a byte slice for indexing.
func (ke *KeyEncoder) EncodeValue(val types.Value) ([]byte, error) {
	if val.IsNull() {
		return nil, nil
	}

	// Type switch on the actual data
	switch v := val.Data.(type) {
	case int32:
		// INTEGER - Encode as big-endian for proper ordering
		var buf [4]byte
		// The conversion from int32 to uint32 preserves the bit pattern
		// This is safe for encoding purposes as we're just storing the bits
		binary.BigEndian.PutUint32(buf[:], uint32(v)) //nolint:gosec // Safe bit pattern preservation
		return buf[:], nil

	case int64:
		// BIGINT/TIMESTAMP - Encode as big-endian for proper ordering
		var buf [8]byte
		// The conversion from int64 to uint64 preserves the bit pattern
		// This is safe for encoding purposes as we're just storing the bits
		binary.BigEndian.PutUint64(buf[:], uint64(v)) //nolint:gosec // Safe bit pattern preservation
		return buf[:], nil

	case int16:
		// SMALLINT - Encode as big-endian for proper ordering
		var buf [2]byte
		// The conversion from int16 to uint16 preserves the bit pattern
		// This is safe for encoding purposes as we're just storing the bits
		binary.BigEndian.PutUint16(buf[:], uint16(v)) //nolint:gosec // Safe bit pattern preservation
		return buf[:], nil

	case float32:
		// REAL - Float32 encoding (handle negative values)
		bits := float32ToBits(v)
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], bits)
		return buf[:], nil

	case float64:
		// DOUBLE PRECISION - Float64 encoding (handle negative values)
		bits := float64ToBits(v)
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], bits)
		return buf[:], nil

	case bool:
		// BOOLEAN
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil

	case string:
		// TEXT/VARCHAR - String values are already comparable
		return []byte(v), nil

	case time.Time:
		// DATE/TIMESTAMP - For indexing purposes, we encode all time.Time values
		// as Unix nanoseconds (8 bytes) to ensure consistent ordering
		// This works for both DATE and TIMESTAMP types
		nano := v.UnixNano()
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(nano))
		return buf[:], nil

	default:
		return nil, fmt.Errorf("unsupported type for indexing: %T", val.Data)
	}
}

// EncodeMultiColumn encodes multiple values into a composite key.
func (ke *KeyEncoder) EncodeMultiColumn(values []types.Value) ([]byte, error) {
	var result []byte

	for i, val := range values {
		encoded, err := ke.EncodeValue(val)
		if err != nil {
			return nil, err
		}

		// Add separator between columns (except for last)
		if i > 0 {
			result = append(result, 0x00) // NULL byte separator
		}

		if encoded == nil {
			// NULL value - use special marker
			result = append(result, 0xFF, 0xFF, 0xFF, 0xFF)
		} else {
			result = append(result, encoded...)
		}
	}

	return result, nil
}

// float32ToBits converts float32 to uint32 bits with sign adjustment for ordering.
func float32ToBits(f float32) uint32 {
	bits := *(*uint32)(unsafe.Pointer(&f))
	// If negative, flip all bits; if positive, flip only sign bit
	if bits&0x80000000 != 0 {
		bits = ^bits
	} else {
		bits ^= 0x80000000
	}
	return bits
}

// float64ToBits converts float64 to uint64 bits with sign adjustment for ordering.
func float64ToBits(f float64) uint64 {
	bits := *(*uint64)(unsafe.Pointer(&f))
	// If negative, flip all bits; if positive, flip only sign bit
	if bits&0x8000000000000000 != 0 {
		bits = ^bits
	} else {
		bits ^= 0x8000000000000000
	}
	return bits
}
