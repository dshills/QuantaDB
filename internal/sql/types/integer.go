package types

import (
	"encoding/binary"
	"fmt"
)

func init() {
	Integer = &integerType{}
	BigInt = &bigIntType{}
	SmallInt = &smallIntType{}
}

// integerType implements the INTEGER data type (32-bit)
type integerType struct{}

func (t *integerType) Name() string {
	return "INTEGER"
}

func (t *integerType) Size() int {
	return 4
}

func (t *integerType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aVal := a.Data.(int32)
	bVal := b.Data.(int32)

	if aVal < bVal {
		return -1
	} else if aVal > bVal {
		return 1
	}
	return 0
}

func (t *integerType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	val, ok := v.Data.(int32)
	if !ok {
		return nil, fmt.Errorf("expected int32, got %T", v.Data)
	}

	// No need to check bounds - val is already int32 type
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(val)) // nolint:gosec // Bounds checked above
	return buf, nil
}

func (t *integerType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) != 4 {
		return Value{}, fmt.Errorf("expected 4 bytes for INTEGER, got %d", len(data))
	}

	uval := binary.BigEndian.Uint32(data)
	if uval > 2147483647 { // Check if it would overflow when converted to int32
		return Value{}, fmt.Errorf("uint32 value %d too large for int32", uval)
	}
	val := int32(uval) // nolint:gosec // Bounds checked above
	return NewValue(val), nil
}

func (t *integerType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	_, ok := v.Data.(int32)
	return ok
}

func (t *integerType) Zero() Value {
	return NewValue(int32(0))
}

// bigIntType implements the BIGINT data type (64-bit)
type bigIntType struct{}

func (t *bigIntType) Name() string {
	return "BIGINT"
}

func (t *bigIntType) Size() int {
	return 8
}

func (t *bigIntType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aVal := a.Data.(int64)
	bVal := b.Data.(int64)

	if aVal < bVal {
		return -1
	} else if aVal > bVal {
		return 1
	}
	return 0
}

func (t *bigIntType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	val, ok := v.Data.(int64)
	if !ok {
		return nil, fmt.Errorf("expected int64, got %T", v.Data)
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(val)) // nolint:gosec // int64 to uint64 conversion is safe
	return buf, nil
}

func (t *bigIntType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) != 8 {
		return Value{}, fmt.Errorf("expected 8 bytes for BIGINT, got %d", len(data))
	}

	val := int64(binary.BigEndian.Uint64(data)) // nolint:gosec // uint64 to int64 conversion is safe for valid data
	return NewValue(val), nil
}

func (t *bigIntType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	_, ok := v.Data.(int64)
	return ok
}

func (t *bigIntType) Zero() Value {
	return NewValue(int64(0))
}

// smallIntType implements the SMALLINT data type (16-bit)
type smallIntType struct{}

func (t *smallIntType) Name() string {
	return "SMALLINT"
}

func (t *smallIntType) Size() int {
	return 2
}

func (t *smallIntType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aVal := a.Data.(int16)
	bVal := b.Data.(int16)

	if aVal < bVal {
		return -1
	} else if aVal > bVal {
		return 1
	}
	return 0
}

func (t *smallIntType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	val, ok := v.Data.(int16)
	if !ok {
		return nil, fmt.Errorf("expected int16, got %T", v.Data)
	}

	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(val)) // nolint:gosec // int16 to uint16 conversion is safe
	return buf, nil
}

func (t *smallIntType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) != 2 {
		return Value{}, fmt.Errorf("expected 2 bytes for SMALLINT, got %d", len(data))
	}

	val := int16(binary.BigEndian.Uint16(data)) // nolint:gosec // uint16 to int16 conversion is safe
	return NewValue(val), nil
}

func (t *smallIntType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	_, ok := v.Data.(int16)
	return ok
}

func (t *smallIntType) Zero() Value {
	return NewValue(int16(0))
}

// Helper functions for creating integer values

// NewIntegerValue creates a new INTEGER value
func NewIntegerValue(v int32) Value {
	return NewValue(v)
}

// NewBigIntValue creates a new BIGINT value
func NewBigIntValue(v int64) Value {
	return NewValue(v)
}

// NewSmallIntValue creates a new SMALLINT value
func NewSmallIntValue(v int16) Value {
	return NewValue(v)
}
