package index

import (
	"fmt"
	"sync"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CompositeBTreeIndex implements a B+Tree index for multi-column keys
type CompositeBTreeIndex struct {
	mu           sync.RWMutex
	tree         *BTree
	columnTypes  []types.DataType
	columnNames  []string
	unique       bool
	nullable     bool
	stats        IndexStats
}

// NewCompositeBTreeIndex creates a new composite B+Tree index
func NewCompositeBTreeIndex(columnTypes []types.DataType, columnNames []string, unique, nullable bool) *CompositeBTreeIndex {
	return &CompositeBTreeIndex{
		tree:        NewBTree(CompositeKeyComparator()),
		columnTypes: columnTypes,
		columnNames: columnNames,
		unique:      unique,
		nullable:    nullable,
		stats: IndexStats{
			Type: BTreeIndex,
		},
	}
}

// Insert adds a composite key-value pair to the index
func (idx *CompositeBTreeIndex) Insert(values []types.Value, rowID []byte) error {
	if len(values) != len(idx.columnTypes) {
		return fmt.Errorf("value count (%d) does not match column count (%d)", 
			len(values), len(idx.columnTypes))
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Create composite key
	compositeKey := NewCompositeKey(values)
	keyBytes, err := compositeKey.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode composite key: %w", err)
	}

	// Check for uniqueness if required
	if idx.unique {
		existing, found := idx.tree.Search(keyBytes)
		if found {
			// For composite keys, we need to compare the actual row IDs
			// since the same key shouldn't point to different rows
			if !bytesEqual(existing, rowID) {
				return fmt.Errorf("duplicate key value violates unique constraint")
			}
			// Same key, same row - this is an update, allow it
			return nil
		}
	}

	err = idx.tree.Insert(keyBytes, rowID)
	if err == nil {
		idx.stats.TotalEntries++
	}
	return err
}

// Search finds all row IDs for the given composite key values
func (idx *CompositeBTreeIndex) Search(values []types.Value) ([][]byte, error) {
	if len(values) > len(idx.columnTypes) {
		return nil, fmt.Errorf("too many values provided for search")
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Create composite key (potentially partial)
	compositeKey := NewCompositeKey(values)
	keyBytes, err := compositeKey.Encode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode search key: %w", err)
	}

	if len(values) == len(idx.columnTypes) {
		// Exact match search
		rowID, found := idx.tree.Search(keyBytes)
		if !found {
			return nil, nil
		}
		return [][]byte{rowID}, nil
	}

	// Prefix search - need to do a range scan
	return idx.prefixSearch(keyBytes)
}

// prefixSearch performs a range scan for partial composite keys
func (idx *CompositeBTreeIndex) prefixSearch(prefixBytes []byte) ([][]byte, error) {
	// For prefix search, we need to find all keys that start with the prefix
	// We do this by creating a range from prefix to prefix+1
	
	startKey := make([]byte, len(prefixBytes))
	copy(startKey, prefixBytes)
	
	// Create end key by incrementing the last byte
	endKey := make([]byte, len(prefixBytes))
	copy(endKey, prefixBytes)
	
	// Find the last non-0xFF byte and increment it
	carried := true
	for i := len(endKey) - 1; i >= 0 && carried; i-- {
		if endKey[i] < 0xFF {
			endKey[i]++
			carried = false
		} else {
			endKey[i] = 0x00
		}
	}
	
	if carried {
		// All bytes were 0xFF, so we scan to the end
		return idx.scanToEnd(startKey)
	}

	pairs, err := idx.tree.Range(startKey, endKey)
	if err != nil {
		return nil, err
	}

	rowIDs := make([][]byte, len(pairs))
	for i, pair := range pairs {
		rowIDs[i] = pair.Value
	}

	return rowIDs, nil
}

// scanToEnd scans from the given key to the end of the index
func (idx *CompositeBTreeIndex) scanToEnd(startKey []byte) ([][]byte, error) {
	// Create a very large end key
	endKey := make([]byte, len(startKey)+1)
	copy(endKey, startKey)
	for i := range endKey {
		endKey[i] = 0xFF
	}

	pairs, err := idx.tree.Range(startKey, endKey)
	if err != nil {
		return nil, err
	}

	rowIDs := make([][]byte, len(pairs))
	for i, pair := range pairs {
		rowIDs[i] = pair.Value
	}

	return rowIDs, nil
}

// RangeSearch performs a range scan with composite key bounds
func (idx *CompositeBTreeIndex) RangeSearch(startValues, endValues []types.Value) ([]IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var startKey, endKey []byte
	var err error

	if startValues != nil {
		startCompositeKey := NewCompositeKey(startValues)
		startKey, err = startCompositeKey.Encode()
		if err != nil {
			return nil, fmt.Errorf("failed to encode start key: %w", err)
		}
	}

	if endValues != nil {
		endCompositeKey := NewCompositeKey(endValues)
		endKey, err = endCompositeKey.Encode()
		if err != nil {
			return nil, fmt.Errorf("failed to encode end key: %w", err)
		}
		
		// If this is a partial key (fewer values than index columns), 
		// we need to make it inclusive by padding with maximum values
		if len(endValues) < len(idx.columnTypes) {
			// Append 0xFF bytes to make this key greater than any key with this prefix
			endKey = append(endKey, 0xFF, 0xFF, 0xFF, 0xFF)
		}
	}

	// If no bounds specified, scan everything
	if startKey == nil && endKey == nil {
		return idx.fullScan()
	}

	// Use tree range scan
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

// fullScan returns all entries in the index
func (idx *CompositeBTreeIndex) fullScan() ([]IndexEntry, error) {
	// Create minimal and maximal keys
	minKey := []byte{0x00}
	maxKey := []byte{0xFF, 0xFF, 0xFF, 0xFF}

	pairs, err := idx.tree.Range(minKey, maxKey)
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

// Delete removes a composite key from the index
func (idx *CompositeBTreeIndex) Delete(values []types.Value) error {
	if len(values) != len(idx.columnTypes) {
		return fmt.Errorf("value count (%d) does not match column count (%d)", 
			len(values), len(idx.columnTypes))
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	compositeKey := NewCompositeKey(values)
	keyBytes, err := compositeKey.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode key for deletion: %w", err)
	}

	err = idx.tree.Delete(keyBytes)
	if err == nil {
		idx.stats.TotalEntries--
	}
	return err
}

// ColumnCount returns the number of columns in this composite index
func (idx *CompositeBTreeIndex) ColumnCount() int {
	return len(idx.columnTypes)
}

// ColumnNames returns the names of the indexed columns
func (idx *CompositeBTreeIndex) ColumnNames() []string {
	return idx.columnNames
}

// ColumnTypes returns the types of the indexed columns
func (idx *CompositeBTreeIndex) ColumnTypes() []types.DataType {
	return idx.columnTypes
}

// IsUnique returns whether this is a unique index
func (idx *CompositeBTreeIndex) IsUnique() bool {
	return idx.unique
}

// Stats returns index statistics
func (idx *CompositeBTreeIndex) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	treeStats := idx.tree.Stats()
	idx.stats.Height = treeStats.Height

	return idx.stats
}

// MatchesPredicate evaluates how well this index matches a given set of predicates
func (idx *CompositeBTreeIndex) MatchesPredicate(predicates map[string]interface{}) *CompositeKeyMatch {
	match := &CompositeKeyMatch{
		MatchingColumns: 0,
		CanUseRange:     false,
		Selectivity:     1.0,
	}

	// Check how many leading columns we can use
	for _, colName := range idx.columnNames {
		if pred, exists := predicates[colName]; exists {
			match.MatchingColumns++
			
			// Check if this is a range predicate vs equality
			switch pred.(type) {
			case string: // Assuming string means equality for now
				match.Selectivity *= 0.1 // Rough estimate
			default:
				match.CanUseRange = true
				match.Selectivity *= 0.3 // Range predicates are less selective
			}
		} else {
			// Stop at first missing column (leftmost prefix rule)
			break
		}
	}

	return match
}

// Utility function to compare byte slices
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}