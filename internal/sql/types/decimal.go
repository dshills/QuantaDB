package types

import (
	"fmt"
	"math/big"
	"strings"
)

func init() {
	// Decimal is a function that returns a DECIMAL type with specified precision and scale
	Decimal = func(precision, scale int) DataType {
		return &decimalType{
			precision: precision,
			scale:     scale,
		}
	}
}

// decimalType implements the DECIMAL(p,s) data type
type decimalType struct {
	precision int // Total number of digits
	scale     int // Number of digits after decimal point
}

func (t *decimalType) Name() string {
	return fmt.Sprintf("DECIMAL(%d,%d)", t.precision, t.scale)
}

func (t *decimalType) Size() int {
	// Variable size, depends on precision
	// For now, use a simple calculation
	return t.precision + 2 // Extra bytes for sign and metadata
}

func (t *decimalType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}
	
	aVal := a.Data.(*big.Rat)
	bVal := b.Data.(*big.Rat)
	
	return aVal.Cmp(bVal)
}

func (t *decimalType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}
	
	val, ok := v.Data.(*big.Rat)
	if !ok {
		return nil, fmt.Errorf("expected *big.Rat, got %T", v.Data)
	}
	
	// For now, serialize as string
	// TODO: Implement more efficient binary serialization
	str := val.FloatString(t.scale)
	
	// Validate precision
	parts := strings.Split(str, ".")
	totalDigits := len(strings.ReplaceAll(parts[0], "-", ""))
	if len(parts) > 1 {
		totalDigits += len(parts[1])
	}
	
	if totalDigits > t.precision {
		return nil, fmt.Errorf("value exceeds precision %d", t.precision)
	}
	
	return []byte(str), nil
}

func (t *decimalType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}
	
	str := string(data)
	rat := new(big.Rat)
	
	if _, ok := rat.SetString(str); !ok {
		return Value{}, fmt.Errorf("invalid decimal value: %s", str)
	}
	
	return NewValue(rat), nil
}

func (t *decimalType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	
	rat, ok := v.Data.(*big.Rat)
	if !ok {
		return false
	}
	
	// Check precision
	str := rat.FloatString(t.scale)
	parts := strings.Split(str, ".")
	totalDigits := len(strings.ReplaceAll(parts[0], "-", ""))
	if len(parts) > 1 {
		totalDigits += len(parts[1])
	}
	
	return totalDigits <= t.precision
}

func (t *decimalType) Zero() Value {
	return NewValue(new(big.Rat))
}

// NewDecimalValue creates a new DECIMAL value from a string
func NewDecimalValue(s string) (Value, error) {
	rat := new(big.Rat)
	if _, ok := rat.SetString(s); !ok {
		return Value{}, fmt.Errorf("invalid decimal value: %s", s)
	}
	return NewValue(rat), nil
}

// NewDecimalValueFromFloat creates a new DECIMAL value from a float64
func NewDecimalValueFromFloat(f float64) Value {
	rat := new(big.Rat).SetFloat64(f)
	return NewValue(rat)
}

// NewDecimalValueFromInt creates a new DECIMAL value from an int64
func NewDecimalValueFromInt(i int64) Value {
	rat := new(big.Rat).SetInt64(i)
	return NewValue(rat)
}