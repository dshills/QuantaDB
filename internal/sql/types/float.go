package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

func init() {
	Float = &floatType{}
	Double = &doubleType{}
}

// floatType implements the FLOAT data type (32-bit IEEE 754)
type floatType struct{}

func (t *floatType) Name() string {
	return "FLOAT"
}

func (t *floatType) Size() int {
	return 4
}

func (t *floatType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}
	
	aVal, err := a.AsFloat()
	if err != nil {
		panic(fmt.Sprintf("float comparison: %v", err))
	}
	
	bVal, err := b.AsFloat()
	if err != nil {
		panic(fmt.Sprintf("float comparison: %v", err))
	}
	
	if aVal < bVal {
		return -1
	} else if aVal > bVal {
		return 1
	}
	return 0
}

func (t *floatType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}
	
	val, err := v.AsFloat()
	if err != nil {
		return nil, err
	}
	
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, math.Float32bits(val))
	return buf, nil
}

func (t *floatType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NullValue(Float), nil
	}
	
	if len(data) != 4 {
		return Value{}, fmt.Errorf("expected 4 bytes for FLOAT, got %d", len(data))
	}
	
	val := math.Float32frombits(binary.BigEndian.Uint32(data))
	return NewValue(val), nil
}

func (t *floatType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	_, ok := v.Data.(float32)
	return ok
}

func (t *floatType) Zero() Value {
	return NewValue(float32(0))
}

func (t *floatType) String(v Value) string {
	if v.Null {
		return "NULL"
	}
	val, err := v.AsFloat()
	if err != nil {
		return "ERROR"
	}
	return strconv.FormatFloat(float64(val), 'g', -1, 32)
}

// doubleType implements the DOUBLE PRECISION data type (64-bit IEEE 754)
type doubleType struct{}

func (t *doubleType) Name() string {
	return "DOUBLE PRECISION"
}

func (t *doubleType) Size() int {
	return 8
}

func (t *doubleType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}
	
	aVal, err := a.AsDouble()
	if err != nil {
		panic(fmt.Sprintf("double comparison: %v", err))
	}
	
	bVal, err := b.AsDouble()
	if err != nil {
		panic(fmt.Sprintf("double comparison: %v", err))
	}
	
	if aVal < bVal {
		return -1
	} else if aVal > bVal {
		return 1
	}
	return 0
}

func (t *doubleType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}
	
	val, err := v.AsDouble()
	if err != nil {
		return nil, err
	}
	
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(val))
	return buf, nil
}

func (t *doubleType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NullValue(Double), nil
	}
	
	if len(data) != 8 {
		return Value{}, fmt.Errorf("expected 8 bytes for DOUBLE PRECISION, got %d", len(data))
	}
	
	val := math.Float64frombits(binary.BigEndian.Uint64(data))
	return NewValue(val), nil
}

func (t *doubleType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	_, ok := v.Data.(float64)
	return ok
}

func (t *doubleType) Zero() Value {
	return NewValue(float64(0))
}

func (t *doubleType) String(v Value) string {
	if v.Null {
		return "NULL"
	}
	val, err := v.AsDouble()
	if err != nil {
		return "ERROR"
	}
	return strconv.FormatFloat(val, 'g', -1, 64)
}