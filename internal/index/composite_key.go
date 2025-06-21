package index

import (
	"bytes"
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// CompositeKey represents a multi-column index key
type CompositeKey struct {
	Values []types.Value
	Types  []types.DataType
}

// NewCompositeKey creates a new composite key from the given values
func NewCompositeKey(values []types.Value) *CompositeKey {
	types := make([]types.DataType, len(values))
	for i, val := range values {
		types[i] = val.Type()
	}

	return &CompositeKey{
		Values: values,
		Types:  types,
	}
}

// Encode serializes the composite key to bytes for storage in B+Tree
func (ck *CompositeKey) Encode() ([]byte, error) {
	encoder := &KeyEncoder{}
	return encoder.EncodeMultiColumn(ck.Values)
}

// Compare compares two composite keys lexicographically
// Returns: -1 if ck < other, 0 if ck == other, 1 if ck > other
func (ck *CompositeKey) Compare(other *CompositeKey) int {
	minLen := len(ck.Values)
	if len(other.Values) < minLen {
		minLen = len(other.Values)
	}

	// Compare each column in order
	for i := 0; i < minLen; i++ {
		cmp := types.CompareValues(ck.Values[i], other.Values[i])
		if cmp != 0 {
			return cmp
		}
	}

	// If all compared columns are equal, shorter key comes first
	if len(ck.Values) < len(other.Values) {
		return -1
	} else if len(ck.Values) > len(other.Values) {
		return 1
	}

	return 0
}

// IsPrefix returns true if this composite key is a prefix of the other key
func (ck *CompositeKey) IsPrefix(other *CompositeKey) bool {
	if len(ck.Values) > len(other.Values) {
		return false
	}

	for i := 0; i < len(ck.Values); i++ {
		if types.CompareValues(ck.Values[i], other.Values[i]) != 0 {
			return false
		}
	}

	return true
}

// Truncate returns a new composite key with only the first n columns
func (ck *CompositeKey) Truncate(n int) *CompositeKey {
	if n >= len(ck.Values) {
		return ck
	}

	return &CompositeKey{
		Values: ck.Values[:n],
		Types:  ck.Types[:n],
	}
}

// String returns a string representation of the composite key
func (ck *CompositeKey) String() string {
	if len(ck.Values) == 0 {
		return "()"
	}

	result := "("
	for i, val := range ck.Values {
		if i > 0 {
			result += ", "
		}
		result += val.String()
	}
	result += ")"

	return result
}

// CompositeKeyComparator creates a comparator function for composite keys
// that can be used with the existing B+Tree implementation
func CompositeKeyComparator() Comparator {
	return func(a, b []byte) int {
		return bytes.Compare(a, b)
	}
}

// DecodeCompositeKey deserializes a composite key from bytes
func DecodeCompositeKey(data []byte, columnTypes []types.DataType) (*CompositeKey, error) {
	if len(data) == 0 {
		return &CompositeKey{}, nil
	}

	values := make([]types.Value, len(columnTypes))
	offset := 0

	for i, colType := range columnTypes {
		if offset >= len(data) {
			return nil, fmt.Errorf("insufficient data to decode column %d", i)
		}

		// Skip separator before this column (except for first column)
		if i > 0 && offset < len(data) && data[offset] == 0x00 {
			offset++
		}

		if offset >= len(data) {
			return nil, fmt.Errorf("insufficient data after separator for column %d", i)
		}

		// Handle NULL marker
		if offset+4 <= len(data) &&
			data[offset] == 0xFF && data[offset+1] == 0xFF &&
			data[offset+2] == 0xFF && data[offset+3] == 0xFF {
			values[i] = types.NewNullValue()
			offset += 4
			continue
		}

		// Decode based on column type
		var bytesRead int

		switch colType {
		case types.Integer:
			if offset+4 > len(data) {
				return nil, fmt.Errorf("insufficient data for integer at column %d", i)
			}
			// Decode int32 from big-endian
			intVal := int32(uint32(data[offset])<<24 | uint32(data[offset+1])<<16 |
				uint32(data[offset+2])<<8 | uint32(data[offset+3]))
			values[i] = types.NewIntegerValue(intVal)
			bytesRead = 4

		case types.Text:
			// Find the next separator or end of data
			end := len(data)
			if i < len(columnTypes)-1 { // Not the last column
				for j := offset; j < len(data); j++ {
					if data[j] == 0x00 { // NULL byte separator
						end = j
						break
					}
				}
			}
			values[i] = types.NewTextValue(string(data[offset:end]))
			bytesRead = end - offset

		case types.Boolean:
			if offset+1 > len(data) {
				return nil, fmt.Errorf("insufficient data for boolean at column %d", i)
			}
			values[i] = types.NewBooleanValue(data[offset] != 0)
			bytesRead = 1

		default:
			return nil, fmt.Errorf("unsupported column type for decoding: %v", colType)
		}

		offset += bytesRead
	}

	return &CompositeKey{
		Values: values,
		Types:  columnTypes,
	}, nil
}

// CompositeKeyMatch represents how well a composite key matches a set of predicates
type CompositeKeyMatch struct {
	// Number of leading columns that can be used
	MatchingColumns int

	// Whether the match allows for range scans
	CanUseRange bool

	// Estimated selectivity (0.0 to 1.0)
	Selectivity float64

	// The prefix key for point lookups
	PrefixKey *CompositeKey

	// Range bounds if applicable
	StartKey *CompositeKey
	EndKey   *CompositeKey
}

// IsExactMatch returns true if all columns in the index can be used for equality
func (match *CompositeKeyMatch) IsExactMatch(indexColumnCount int) bool {
	return match.MatchingColumns == indexColumnCount && !match.CanUseRange
}

// IsPrefixMatch returns true if only some leading columns can be used
func (match *CompositeKeyMatch) IsPrefixMatch() bool {
	return match.MatchingColumns > 0
}
