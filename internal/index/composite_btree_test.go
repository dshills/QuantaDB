package index

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestCompositeBTreeIndex_Basic(t *testing.T) {
	// Create a composite index on (id, name)
	columnTypes := []types.DataType{types.Integer, types.Text}
	columnNames := []string{"id", "name"}
	
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, false, false)

	// Test basic insert and search
	values1 := []types.Value{types.NewIntegerValue(1), types.NewTextValue("alice")}
	rowID1 := []byte("row1")

	err := idx.Insert(values1, rowID1)
	if err != nil {
		t.Fatalf("Failed to insert into composite index: %v", err)
	}

	// Search for exact match
	results, err := idx.Search(values1)
	if err != nil {
		t.Fatalf("Failed to search composite index: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if !bytesEqual(results[0], rowID1) {
		t.Error("Retrieved row ID doesn't match inserted row ID")
	}
}

func TestCompositeBTreeIndex_MultipleRows(t *testing.T) {
	columnTypes := []types.DataType{types.Integer, types.Text, types.Boolean}
	columnNames := []string{"id", "name", "active"}
	
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, false, false)

	// Insert multiple rows
	testData := []struct {
		values []types.Value
		rowID  []byte
	}{
		{
			[]types.Value{types.NewIntegerValue(1), types.NewTextValue("alice"), types.NewBooleanValue(true)},
			[]byte("row1"),
		},
		{
			[]types.Value{types.NewIntegerValue(1), types.NewTextValue("bob"), types.NewBooleanValue(false)},
			[]byte("row2"),
		},
		{
			[]types.Value{types.NewIntegerValue(2), types.NewTextValue("alice"), types.NewBooleanValue(true)},
			[]byte("row3"),
		},
		{
			[]types.Value{types.NewIntegerValue(2), types.NewTextValue("charlie"), types.NewBooleanValue(false)},
			[]byte("row4"),
		},
	}

	// Insert all rows
	for _, data := range testData {
		err := idx.Insert(data.values, data.rowID)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Test exact searches
	for _, data := range testData {
		results, err := idx.Search(data.values)
		if err != nil {
			t.Fatalf("Failed to search for %v: %v", data.values, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for %v, got %d", data.values, len(results))
		}

		if !bytesEqual(results[0], data.rowID) {
			t.Errorf("Wrong row ID for %v", data.values)
		}
	}

	// Test prefix searches
	// Search for id=1 (should return rows 1 and 2)
	prefixResults, err := idx.Search([]types.Value{types.NewIntegerValue(1)})
	if err != nil {
		t.Fatalf("Failed to do prefix search: %v", err)
	}

	if len(prefixResults) != 2 {
		t.Errorf("Expected 2 results for id=1 prefix search, got %d", len(prefixResults))
	}

	// Search for id=1, name="alice" (should return row 1)
	prefixResults2, err := idx.Search([]types.Value{
		types.NewIntegerValue(1), 
		types.NewTextValue("alice"),
	})
	if err != nil {
		t.Fatalf("Failed to do 2-column prefix search: %v", err)
	}

	if len(prefixResults2) != 1 {
		t.Errorf("Expected 1 result for id=1,name=alice search, got %d", len(prefixResults2))
	}
}

func TestCompositeBTreeIndex_UniqueConstraint(t *testing.T) {
	columnTypes := []types.DataType{types.Integer, types.Text}
	columnNames := []string{"id", "email"}
	
	// Create unique index
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, true, false)

	values1 := []types.Value{types.NewIntegerValue(1), types.NewTextValue("alice@example.com")}
	rowID1 := []byte("row1")
	rowID2 := []byte("row2")

	// First insert should succeed
	err := idx.Insert(values1, rowID1)
	if err != nil {
		t.Fatalf("First insert should succeed: %v", err)
	}

	// Second insert with same key but different row should fail
	err = idx.Insert(values1, rowID2)
	if err == nil {
		t.Error("Expected error for duplicate key in unique index")
	}

	// Insert with same key and same row should succeed (update case)
	err = idx.Insert(values1, rowID1)
	if err != nil {
		t.Errorf("Update with same row ID should succeed: %v", err)
	}
}

func TestCompositeBTreeIndex_RangeSearch(t *testing.T) {
	columnTypes := []types.DataType{types.Integer, types.Text}
	columnNames := []string{"score", "name"}
	
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, false, false)

	// Insert test data with scores
	testData := []struct {
		score int32
		name  string
		rowID string
	}{
		{10, "alice", "row1"},
		{20, "bob", "row2"},
		{25, "charlie", "row3"},
		{30, "david", "row4"},
		{35, "eve", "row5"},
		{40, "frank", "row6"},
	}

	for _, data := range testData {
		values := []types.Value{
			types.NewIntegerValue(data.score),
			types.NewTextValue(data.name),
		}
		err := idx.Insert(values, []byte(data.rowID))
		if err != nil {
			t.Fatalf("Failed to insert %v: %v", data, err)
		}
	}

	// Test range search: score between 20 and 35 (inclusive)
	startValues := []types.Value{types.NewIntegerValue(20)}
	endValues := []types.Value{types.NewIntegerValue(35)}

	// Debug: print encoded start and end keys
	startKey := NewCompositeKey(startValues)
	endKey := NewCompositeKey(endValues)
	startEncoded, _ := startKey.Encode()
	endEncoded, _ := endKey.Encode()
	t.Logf("Start key encoded: %v", startEncoded)
	t.Logf("End key encoded: %v", endEncoded)

	entries, err := idx.RangeSearch(startValues, endValues)
	if err != nil {
		t.Fatalf("Range search failed: %v", err)
	}

	// Debug: print actual entries found
	t.Logf("Found %d entries in range search", len(entries))
	for _, entry := range entries {
		t.Logf("  Key bytes: %v", entry.Key)
		decodedKey, _ := DecodeCompositeKey(entry.Key, columnTypes)
		if len(decodedKey.Values) > 0 {
			score, _ := decodedKey.Values[0].AsInt()
			name, _ := decodedKey.Values[1].AsString()
			t.Logf("  Score: %d, Name: %s", score, name)
		}
	}

	// Debug: Let's also try to see what key=35 would encode to
	exactKey35 := NewCompositeKey([]types.Value{types.NewIntegerValue(35), types.NewTextValue("eve")})
	exactEncoded35, _ := exactKey35.Encode()
	t.Logf("Exact key for score=35: %v", exactEncoded35)

	// Should return rows with scores 20, 25, 30, 35
	expectedCount := 4
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries in range, got %d", expectedCount, len(entries))
	}

	// Verify the entries are in order
	lastScore := int32(0)
	for _, entry := range entries {
		// Decode the first column (score) from the key
		decodedKey, err := DecodeCompositeKey(entry.Key, columnTypes)
		if err != nil {
			t.Fatalf("Failed to decode key: %v", err)
		}

		score, err := decodedKey.Values[0].AsInt()
		if err != nil {
			t.Fatalf("Failed to extract score: %v", err)
		}

		if score < lastScore {
			t.Error("Range results should be in ascending order")
		}
		lastScore = score

		if score < 20 || score > 35 {
			t.Errorf("Score %d is outside expected range [20, 35]", score)
		}
	}
}

func TestCompositeBTreeIndex_Delete(t *testing.T) {
	columnTypes := []types.DataType{types.Integer, types.Text}
	columnNames := []string{"id", "name"}
	
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, false, false)

	// Insert a row
	values := []types.Value{types.NewIntegerValue(1), types.NewTextValue("alice")}
	rowID := []byte("row1")

	err := idx.Insert(values, rowID)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify it exists
	results, err := idx.Search(values)
	if err != nil {
		t.Fatalf("Failed to search before delete: %v", err)
	}
	if len(results) != 1 {
		t.Error("Row should exist before delete")
	}

	// Delete the row
	err = idx.Delete(values)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify it's gone
	results, err = idx.Search(values)
	if err != nil {
		t.Fatalf("Failed to search after delete: %v", err)
	}
	if len(results) != 0 {
		t.Error("Row should not exist after delete")
	}

	// Try to delete again (should fail)
	err = idx.Delete(values)
	if err == nil {
		t.Error("Expected error when deleting non-existent key")
	}
}

func TestCompositeBTreeIndex_Stats(t *testing.T) {
	columnTypes := []types.DataType{types.Integer, types.Text}
	columnNames := []string{"id", "name"}
	
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, false, false)

	// Check initial stats
	stats := idx.Stats()
	if stats.TotalEntries != 0 {
		t.Error("New index should have 0 entries")
	}

	// Insert some data
	for i := 0; i < 10; i++ {
		values := []types.Value{
			types.NewIntegerValue(int32(i)),
			types.NewTextValue("name" + string(rune('0'+i))),
		}
		err := idx.Insert(values, []byte("row"+string(rune('0'+i))))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Check stats after insertion
	stats = idx.Stats()
	if stats.TotalEntries != 10 {
		t.Errorf("Expected 10 entries, got %d", stats.TotalEntries)
	}

	if stats.Type != BTreeIndex {
		t.Error("Index type should be BTreeIndex")
	}

	if stats.Height <= 0 {
		t.Error("Index height should be positive")
	}
}

func TestCompositeBTreeIndex_ErrorCases(t *testing.T) {
	columnTypes := []types.DataType{types.Integer, types.Text}
	columnNames := []string{"id", "name"}
	
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, false, false)

	// Test wrong number of values for insert
	wrongValues := []types.Value{types.NewIntegerValue(1)} // Missing second column
	err := idx.Insert(wrongValues, []byte("row1"))
	if err == nil {
		t.Error("Expected error for wrong number of values in insert")
	}

	// Test wrong number of values for delete
	err = idx.Delete(wrongValues)
	if err == nil {
		t.Error("Expected error for wrong number of values in delete")
	}

	// Test too many values for search (should not error, but should handle gracefully)
	tooManyValues := []types.Value{
		types.NewIntegerValue(1),
		types.NewTextValue("name"),
		types.NewBooleanValue(true), // Extra value
	}
	_, err = idx.Search(tooManyValues)
	if err == nil {
		t.Error("Expected error for too many values in search")
	}
}

func TestCompositeBTreeIndex_NullHandling(t *testing.T) {
	columnTypes := []types.DataType{types.Integer, types.Text}
	columnNames := []string{"id", "name"}
	
	// Test with nullable index
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, false, true)

	// Insert row with NULL
	values := []types.Value{types.NewIntegerValue(1), types.NewNullValue()}
	rowID := []byte("row1")

	err := idx.Insert(values, rowID)
	if err != nil {
		t.Fatalf("Failed to insert row with NULL: %v", err)
	}

	// Search for the row
	results, err := idx.Search(values)
	if err != nil {
		t.Fatalf("Failed to search for row with NULL: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result for NULL search, got %d", len(results))
	}

	if !bytesEqual(results[0], rowID) {
		t.Error("Retrieved row ID doesn't match for NULL case")
	}
}

func TestCompositeBTreeIndex_MatchesPredicate(t *testing.T) {
	columnTypes := []types.DataType{types.Integer, types.Text, types.Boolean}
	columnNames := []string{"id", "name", "active"}
	
	idx := NewCompositeBTreeIndex(columnTypes, columnNames, false, false)

	// Test matching with exact predicates
	predicates := map[string]interface{}{
		"id":   42,
		"name": "alice",
	}

	match := idx.MatchesPredicate(predicates)
	if match.MatchingColumns != 2 {
		t.Errorf("Expected 2 matching columns, got %d", match.MatchingColumns)
	}

	if !match.IsPrefixMatch() {
		t.Error("Should be a prefix match")
	}

	// Test with only first column
	predicates2 := map[string]interface{}{
		"id": 42,
	}

	match2 := idx.MatchesPredicate(predicates2)
	if match2.MatchingColumns != 1 {
		t.Errorf("Expected 1 matching column, got %d", match2.MatchingColumns)
	}

	// Test with gap in columns (should stop at gap)
	predicates3 := map[string]interface{}{
		"id":     42,
		"active": true, // Skip "name" column
	}

	match3 := idx.MatchesPredicate(predicates3)
	if match3.MatchingColumns != 1 {
		t.Errorf("Expected 1 matching column due to gap, got %d", match3.MatchingColumns)
	}
}