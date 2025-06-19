package types

import (
	"fmt"
)

func init() {
	Boolean = &booleanType{}
}

// booleanType implements the BOOLEAN data type.
type booleanType struct{}

func (t *booleanType) Name() string {
	return "BOOLEAN"
}

func (t *booleanType) Size() int {
	return 1
}

func (t *booleanType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aVal := a.Data.(bool)
	bVal := b.Data.(bool)
	// false < true
	if !aVal && bVal {
		return -1
	} else if aVal && !bVal {
		return 1
	}
	return 0
}

func (t *booleanType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	val, ok := v.Data.(bool)
	if !ok {
		return nil, fmt.Errorf("expected bool, got %T", v.Data)
	}
	if val {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func (t *booleanType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) != 1 {
		return Value{}, fmt.Errorf("expected 1 byte for BOOLEAN, got %d", len(data))
	}

	return NewValue(data[0] != 0), nil
}

func (t *booleanType) IsValid(v Value) bool {
	if v.Null {
		return true
	}

	_, ok := v.Data.(bool)
	return ok
}

func (t *booleanType) Zero() Value {
	return NewValue(false)
}

// NewBooleanValue creates a new BOOLEAN value.
func NewBooleanValue(b bool) Value {
	return NewValue(b)
}
