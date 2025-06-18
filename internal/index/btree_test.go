package index

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestBTreeBasicOperations(t *testing.T) {
	tree := NewBTree(DefaultComparator)

	// Test insert and search
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("fruit1")},
		{[]byte("banana"), []byte("fruit2")},
		{[]byte("cherry"), []byte("fruit3")},
		{[]byte("date"), []byte("fruit4")},
		{[]byte("elderberry"), []byte("fruit5")},
	}

	// Insert data
	for _, item := range testData {
		if err := tree.Insert(item.key, item.value); err != nil {
			t.Fatalf("Failed to insert %s: %v", item.key, err)
		}
	}

	// Search for existing keys
	for _, item := range testData {
		value, found := tree.Search(item.key)
		if !found {
			t.Errorf("Key %s not found", item.key)
		}
		if !bytes.Equal(value, item.value) {
			t.Errorf("Expected value %s, got %s", item.value, value)
		}
	}

	// Search for non-existing key
	_, found := tree.Search([]byte("grape"))
	if found {
		t.Error("Found non-existing key")
	}
}

func TestBTreeUpdate(t *testing.T) {
	tree := NewBTree(DefaultComparator)

	key := []byte("test")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Insert initial value
	if err := tree.Insert(key, value1); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify initial value
	result, found := tree.Search(key)
	if !found || !bytes.Equal(result, value1) {
		t.Error("Initial value not correct")
	}

	// Update value
	if err := tree.Insert(key, value2); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Verify updated value
	result, found = tree.Search(key)
	if !found || !bytes.Equal(result, value2) {
		t.Error("Updated value not correct")
	}
}

func TestBTreeDelete(t *testing.T) {
	tree := NewBTree(DefaultComparator)

	// Insert test data
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for i, k := range keys {
		if err := tree.Insert([]byte(k), []byte(fmt.Sprintf("value%d", i))); err != nil {
			t.Fatalf("Failed to insert %s: %v", k, err)
		}
	}

	// Delete some keys
	toDelete := []string{"b", "e", "h"}
	for _, k := range toDelete {
		if err := tree.Delete([]byte(k)); err != nil {
			t.Fatalf("Failed to delete %s: %v", k, err)
		}
	}

	// Verify deleted keys are gone
	for _, k := range toDelete {
		_, found := tree.Search([]byte(k))
		if found {
			t.Errorf("Deleted key %s still found", k)
		}
	}

	// Verify remaining keys are still there
	remaining := []string{"a", "c", "d", "f", "g", "i", "j"}
	for _, k := range remaining {
		_, found := tree.Search([]byte(k))
		if !found {
			t.Errorf("Key %s not found after deletion", k)
		}
	}

	// Try to delete non-existing key
	err := tree.Delete([]byte("z"))
	if err == nil {
		t.Error("Expected error when deleting non-existing key")
	}
}

func TestBTreeRange(t *testing.T) {
	tree := NewBTree(DefaultComparator)

	// Insert sequential data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%03d", i)
		value := fmt.Sprintf("value%d", i)
		if err := tree.Insert([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test range query
	results, err := tree.Range([]byte("020"), []byte("030"))
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}

	// Should return 11 items (020-030 inclusive)
	if len(results) != 11 {
		t.Errorf("Expected 11 results, got %d", len(results))
	}

	// Verify results are in order
	for i := 0; i < len(results)-1; i++ {
		if bytes.Compare(results[i].Key, results[i+1].Key) >= 0 {
			t.Error("Results not in order")
		}
	}

	// Test empty range
	results, err = tree.Range([]byte("200"), []byte("300"))
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}
	if len(results) != 0 {
		t.Error("Expected empty results for out-of-range query")
	}

	// Test invalid range
	_, err = tree.Range([]byte("100"), []byte("000"))
	if err == nil {
		t.Error("Expected error for invalid range")
	}
}

func TestBTreeSplitAndMerge(t *testing.T) {
	tree := NewBTree(DefaultComparator)

	// Insert enough data to force splits
	n := degree * 10
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%08d", i)
		value := fmt.Sprintf("value%d", i)
		if err := tree.Insert([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Verify all data is still searchable
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%08d", i)
		_, found := tree.Search([]byte(key))
		if !found {
			t.Errorf("Key %s not found after splits", key)
		}
	}

	// Check tree statistics
	stats := tree.Stats()
	if stats.Height < 2 {
		t.Error("Expected tree height > 1 after many insertions")
	}
	if stats.TotalKeys != n {
		t.Errorf("Expected %d total keys, got %d", n, stats.TotalKeys)
	}

	// Delete half the data to potentially trigger merges
	for i := 0; i < n/2; i++ {
		key := fmt.Sprintf("%08d", i*2)
		if err := tree.Delete([]byte(key)); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}
	}

	// Verify remaining data
	for i := 0; i < n/2; i++ {
		key := fmt.Sprintf("%08d", i*2+1)
		_, found := tree.Search([]byte(key))
		if !found {
			t.Errorf("Key %s not found after deletions", key)
		}
	}
}

func TestBTreeConcurrency(t *testing.T) {
	tree := NewBTree(DefaultComparator)

	// Concurrent inserts
	done := make(chan bool)
	numGoroutines := 10
	itemsPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		go func(goroutine int) {
			for i := 0; i < itemsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d-i%d", goroutine, i)
				value := fmt.Sprintf("value-%d-%d", goroutine, i)
				if err := tree.Insert([]byte(key), []byte(value)); err != nil {
					t.Errorf("Concurrent insert failed: %v", err)
				}
			}
			done <- true
		}(g)
	}

	// Wait for all inserts to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all data is present
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < itemsPerGoroutine; i++ {
			key := fmt.Sprintf("g%d-i%d", g, i)
			_, found := tree.Search([]byte(key))
			if !found {
				t.Errorf("Key %s not found after concurrent inserts", key)
			}
		}
	}

	stats := tree.Stats()
	expectedKeys := numGoroutines * itemsPerGoroutine
	if stats.TotalKeys != expectedKeys {
		t.Errorf("Expected %d keys, got %d", expectedKeys, stats.TotalKeys)
	}
}

func TestBTreeRandomOperations(t *testing.T) {
	tree := NewBTree(DefaultComparator)
	reference := make(map[string]string)

	// Perform random operations
	r := rand.New(rand.NewSource(42))
	numOps := 10000

	for i := 0; i < numOps; i++ {
		op := r.Intn(3)
		key := fmt.Sprintf("key%d", r.Intn(1000))

		switch op {
		case 0, 1: // Insert (higher probability)
			value := fmt.Sprintf("value%d", i)
			tree.Insert([]byte(key), []byte(value))
			reference[key] = value

		case 2: // Delete
			tree.Delete([]byte(key))
			delete(reference, key)
		}
	}

	// Verify tree matches reference
	for key, expectedValue := range reference {
		value, found := tree.Search([]byte(key))
		if !found {
			t.Errorf("Key %s not found in tree", key)
		} else if string(value) != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s", key, expectedValue, value)
		}
	}

	// Verify no extra keys in tree
	allKeys := make([]string, 0, len(reference))
	for k := range reference {
		allKeys = append(allKeys, k)
	}
	sort.Strings(allKeys)

	if len(allKeys) > 0 {
		// Range scan entire tree
		results, _ := tree.Range([]byte(""), []byte("key999"))
		if len(results) != len(reference) {
			t.Errorf("Tree has %d keys, expected %d", len(results), len(reference))
		}
	}
}

func BenchmarkBTreeInsert(b *testing.B) {
	tree := NewBTree(DefaultComparator)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		tree.Insert([]byte(key), []byte(value))
	}
}

func BenchmarkBTreeSearch(b *testing.B) {
	tree := NewBTree(DefaultComparator)

	// Pre-populate tree
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		tree.Insert([]byte(key), []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%100000)
		tree.Search([]byte(key))
	}
}

func BenchmarkBTreeRange(b *testing.B) {
	tree := NewBTree(DefaultComparator)

	// Pre-populate tree
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("%08d", i)
		value := fmt.Sprintf("value%d", i)
		tree.Insert([]byte(key), []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := fmt.Sprintf("%08d", i%90000)
		end := fmt.Sprintf("%08d", i%90000+100)
		tree.Range([]byte(start), []byte(end))
	}
}
