package index

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestCompositeKey_Basic(t *testing.T) {
	// Test basic composite key creation and operations
	values1 := []types.Value{
		types.NewIntegerValue(1),
		types.NewTextValue("alice"),
		types.NewBooleanValue(true),
	}

	values2 := []types.Value{
		types.NewIntegerValue(1),
		types.NewTextValue("alice"),
		types.NewBooleanValue(true),
	}

	values3 := []types.Value{
		types.NewIntegerValue(2),
		types.NewTextValue("bob"),
		types.NewBooleanValue(false),
	}

	key1 := NewCompositeKey(values1)
	key2 := NewCompositeKey(values2)
	key3 := NewCompositeKey(values3)

	// Test equality
	if key1.Compare(key2) != 0 {
		t.Error("Expected key1 == key2")
	}

	// Test ordering
	if key1.Compare(key3) >= 0 {
		t.Error("Expected key1 < key3")
	}

	if key3.Compare(key1) <= 0 {
		t.Error("Expected key3 > key1")
	}
}

func TestCompositeKey_Encoding(t *testing.T) {
	values := []types.Value{
		types.NewIntegerValue(42),
		types.NewTextValue("test"),
		types.NewBooleanValue(true),
	}

	key := NewCompositeKey(values)
	
	// Test encoding
	encoded, err := key.Encode()
	if err != nil {
		t.Fatalf("Failed to encode composite key: %v", err)
	}

	if len(encoded) == 0 {
		t.Error("Expected non-empty encoded key")
	}

	// Test that encoding is deterministic
	encoded2, err := key.Encode()
	if err != nil {
		t.Fatalf("Failed to encode composite key second time: %v", err)
	}

	if len(encoded) != len(encoded2) {
		t.Error("Encoded key length should be consistent")
	}

	for i := range encoded {
		if encoded[i] != encoded2[i] {
			t.Error("Encoded key should be deterministic")
			break
		}
	}
}

func TestCompositeKey_NullValues(t *testing.T) {
	values1 := []types.Value{
		types.NewIntegerValue(1),
		types.NewNullValue(),
		types.NewTextValue("test"),
	}

	values2 := []types.Value{
		types.NewIntegerValue(1),
		types.NewNullValue(),
		types.NewTextValue("test"),
	}

	values3 := []types.Value{
		types.NewIntegerValue(1),
		types.NewTextValue("abc"),
		types.NewTextValue("test"),
	}

	key1 := NewCompositeKey(values1)
	key2 := NewCompositeKey(values2)
	key3 := NewCompositeKey(values3)

	// Test NULL equality
	if key1.Compare(key2) != 0 {
		t.Error("Keys with same NULL values should be equal")
	}

	// Test NULL ordering (NULL < non-NULL)
	if key1.Compare(key3) >= 0 {
		t.Error("Key with NULL should be less than key without NULL")
	}

	// Test encoding with NULLs
	encoded, err := key1.Encode()
	if err != nil {
		t.Fatalf("Failed to encode key with NULL: %v", err)
	}

	if len(encoded) == 0 {
		t.Error("Expected non-empty encoded key even with NULLs")
	}
}

func TestCompositeKey_Prefix(t *testing.T) {
	fullKey := NewCompositeKey([]types.Value{
		types.NewIntegerValue(1),
		types.NewTextValue("alice"),
		types.NewBooleanValue(true),
	})

	prefixKey := NewCompositeKey([]types.Value{
		types.NewIntegerValue(1),
		types.NewTextValue("alice"),
	})

	if !prefixKey.IsPrefix(fullKey) {
		t.Error("prefixKey should be a prefix of fullKey")
	}

	if fullKey.IsPrefix(prefixKey) {
		t.Error("fullKey should not be a prefix of prefixKey")
	}

	// Test truncation
	truncated := fullKey.Truncate(2)
	if truncated.Compare(prefixKey) != 0 {
		t.Error("Truncated key should equal prefix key")
	}
}

func TestCompositeKey_String(t *testing.T) {
	key := NewCompositeKey([]types.Value{
		types.NewIntegerValue(42),
		types.NewTextValue("hello"),
		types.NewNullValue(),
	})

	str := key.String()
	expected := "(42, hello, NULL)"
	if str != expected {
		t.Errorf("Expected string %q, got %q", expected, str)
	}

	// Test empty key
	emptyKey := NewCompositeKey([]types.Value{})
	if emptyKey.String() != "()" {
		t.Errorf("Expected empty key string to be '()', got %q", emptyKey.String())
	}
}

func TestCompositeKey_Decoding(t *testing.T) {
	originalValues := []types.Value{
		types.NewIntegerValue(123),
		types.NewTextValue("test"),
		types.NewBooleanValue(false),
	}

	columnTypes := []types.DataType{
		types.Integer,
		types.Text,
		types.Boolean,
	}

	originalKey := NewCompositeKey(originalValues)
	encoded, err := originalKey.Encode()
	if err != nil {
		t.Fatalf("Failed to encode key: %v", err)
	}

	// Decode the key
	decodedKey, err := DecodeCompositeKey(encoded, columnTypes)
	if err != nil {
		t.Fatalf("Failed to decode key: %v", err)
	}

	// Compare original and decoded
	if originalKey.Compare(decodedKey) != 0 {
		t.Error("Decoded key should equal original key")
	}

	// Check individual values
	if len(decodedKey.Values) != len(originalValues) {
		t.Error("Decoded key should have same number of values")
	}

	for i, val := range decodedKey.Values {
		if types.CompareValues(val, originalValues[i]) != 0 {
			t.Errorf("Value %d differs after decode: expected %v, got %v", 
				i, originalValues[i], val)
		}
	}
}

func TestCompositeKey_DecodingWithNulls(t *testing.T) {
	originalValues := []types.Value{
		types.NewIntegerValue(456),
		types.NewNullValue(),
		types.NewTextValue("after_null"),
	}

	columnTypes := []types.DataType{
		types.Integer,
		types.Text,
		types.Text,
	}

	originalKey := NewCompositeKey(originalValues)
	encoded, err := originalKey.Encode()
	if err != nil {
		t.Fatalf("Failed to encode key with NULL: %v", err)
	}

	decodedKey, err := DecodeCompositeKey(encoded, columnTypes)
	if err != nil {
		t.Fatalf("Failed to decode key with NULL: %v", err)
	}

	if originalKey.Compare(decodedKey) != 0 {
		t.Error("Decoded key with NULL should equal original")
	}

	// Specifically check the NULL value
	if !decodedKey.Values[1].IsNull() {
		t.Error("Second value should be NULL after decoding")
	}
}

func TestCompositeKeyMatch_Basic(t *testing.T) {
	match := &CompositeKeyMatch{
		MatchingColumns: 2,
		CanUseRange:     false,
		Selectivity:     0.01,
	}

	if !match.IsPrefixMatch() {
		t.Error("Match with 2 columns should be a prefix match")
	}

	if match.IsExactMatch(3) {
		t.Error("Match with 2 columns should not be exact match for 3-column index")
	}

	if !match.IsExactMatch(2) {
		t.Error("Match with 2 columns should be exact match for 2-column index")
	}
}

func TestCompositeKey_LexicographicOrdering(t *testing.T) {
	// Test that composite keys maintain proper lexicographic ordering
	testCases := []struct {
		values1 []types.Value
		values2 []types.Value
		expected int // -1, 0, 1
	}{
		// Same first column, different second
		{
			[]types.Value{types.NewIntegerValue(1), types.NewTextValue("a")},
			[]types.Value{types.NewIntegerValue(1), types.NewTextValue("b")},
			-1,
		},
		// Different first column
		{
			[]types.Value{types.NewIntegerValue(1), types.NewTextValue("z")},
			[]types.Value{types.NewIntegerValue(2), types.NewTextValue("a")},
			-1,
		},
		// Same values
		{
			[]types.Value{types.NewIntegerValue(5), types.NewTextValue("equal")},
			[]types.Value{types.NewIntegerValue(5), types.NewTextValue("equal")},
			0,
		},
		// Different lengths, shorter is prefix
		{
			[]types.Value{types.NewIntegerValue(3)},
			[]types.Value{types.NewIntegerValue(3), types.NewTextValue("extra")},
			-1,
		},
	}

	for i, tc := range testCases {
		key1 := NewCompositeKey(tc.values1)
		key2 := NewCompositeKey(tc.values2)
		
		result := key1.Compare(key2)
		if result != tc.expected {
			t.Errorf("Test case %d: expected %d, got %d", i, tc.expected, result)
		}

		// Test symmetry
		if tc.expected != 0 {
			reverse := key2.Compare(key1)
			if reverse != -tc.expected {
				t.Errorf("Test case %d: comparison should be symmetric", i)
			}
		}
	}
}