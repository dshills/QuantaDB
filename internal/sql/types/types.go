package types

import (
	"fmt"
	"time"
)

// DataType represents a SQL data type
type DataType interface {
	// Name returns the SQL name of the type (e.g., "INTEGER", "VARCHAR")
	Name() string

	// Size returns the storage size in bytes (-1 for variable size)
	Size() int

	// Compare compares two values of this type
	// Returns: -1 if a < b, 0 if a == b, 1 if a > b
	Compare(a, b Value) int

	// Serialize converts a value to bytes for storage
	Serialize(v Value) ([]byte, error)

	// Deserialize converts bytes back to a value
	Deserialize(data []byte) (Value, error)

	// IsValid checks if a value is valid for this type
	IsValid(v Value) bool

	// Zero returns the zero value for this type
	Zero() Value
}

// Value represents a SQL value that can be NULL
type Value struct {
	Data interface{}
	Null bool
}

// NewValue creates a non-null value
func NewValue(data interface{}) Value {
	return Value{Data: data, Null: false}
}

// NewNullValue creates a null value
func NewNullValue() Value {
	return Value{Data: nil, Null: true}
}

// IsNull returns true if the value is NULL
func (v Value) IsNull() bool {
	return v.Null
}

// String returns a string representation of the value
func (v Value) String() string {
	if v.Null {
		return "NULL"
	}
	return fmt.Sprintf("%v", v.Data)
}

// AsBool returns the value as a boolean
func (v Value) AsBool() (bool, error) {
	if v.Null {
		return false, fmt.Errorf("cannot convert NULL to bool")
	}
	if b, ok := v.Data.(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("cannot convert %T to bool", v.Data)
}

// AsInt returns the value as an int32
func (v Value) AsInt() (int32, error) {
	if v.Null {
		return 0, fmt.Errorf("cannot convert NULL to int")
	}
	switch val := v.Data.(type) {
	case int32:
		return val, nil
	case int64:
		return int32(val), nil
	case int:
		return int32(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v.Data)
	}
}

// AsString returns the value as a string
func (v Value) AsString() (string, error) {
	if v.Null {
		return "", fmt.Errorf("cannot convert NULL to string")
	}
	if s, ok := v.Data.(string); ok {
		return s, nil
	}
	return "", fmt.Errorf("cannot convert %T to string", v.Data)
}

// Type returns the DataType of the value based on its underlying type
func (v Value) Type() DataType {
	if v.Null {
		return Unknown
	}
	switch v.Data.(type) {
	case int32:
		return Integer
	case int64:
		return BigInt
	case int16:
		return SmallInt
	case string:
		return Text
	case bool:
		return Boolean
	default:
		return Unknown
	}
}

// CompareValues compares two values, handling NULLs
// NULL is considered less than any non-NULL value
func CompareValues(a, b Value) int {
	if a.Null && b.Null {
		return 0
	}
	if a.Null {
		return -1
	}
	if b.Null {
		return 1
	}
	// Both non-null, compare actual values based on type
	switch v1 := a.Data.(type) {
	case int32:
		if v2, ok := b.Data.(int32); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case int64:
		if v2, ok := b.Data.(int64); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case int:
		if v2, ok := b.Data.(int); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case string:
		if v2, ok := b.Data.(string); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case bool:
		if v2, ok := b.Data.(bool); ok {
			if !v1 && v2 {
				return -1
			} else if v1 && !v2 {
				return 1
			}
			return 0
		}
	case float64:
		if v2, ok := b.Data.(float64); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case time.Time:
		if v2, ok := b.Data.(time.Time); ok {
			if v1.Before(v2) {
				return -1
			} else if v1.After(v2) {
				return 1
			}
			return 0
		}
	}
	// For unsupported types or type mismatches, panic to catch bugs early
	panic(fmt.Sprintf("CompareValues: unsupported or mismatched types: %T vs %T", a.Data, b.Data))
}

// Common SQL types
var (
	Integer   DataType
	BigInt    DataType
	SmallInt  DataType
	Boolean   DataType
	Varchar   func(size int) DataType
	Char      func(size int) DataType
	Text      DataType
	Timestamp DataType
	Date      DataType
	Decimal   func(precision, scale int) DataType
)

// TypeID represents the internal ID of a data type
type TypeID uint16

const (
	TypeIDInvalid TypeID = iota
	TypeIDInteger
	TypeIDBigInt
	TypeIDSmallInt
	TypeIDBoolean
	TypeIDVarchar
	TypeIDChar
	TypeIDText
	TypeIDTimestamp
	TypeIDDate
	TypeIDDecimal
)

// Column represents a column definition
type Column struct {
	Name        string
	Type        DataType
	Nullable    bool
	DefaultExpr string // SQL expression for default value
	IsPrimary   bool
	IsUnique    bool
}

// Table represents a table schema
type Table struct {
	Name        string
	Columns     []Column
	PrimaryKey  []string // Column names in primary key
	Indexes     []Index
	Constraints []Constraint
}

// Index represents an index definition
type Index struct {
	Name    string
	Columns []string
	Unique  bool
}

// Constraint represents a table constraint
type Constraint struct {
	Name string
	Type ConstraintType
	Def  string // SQL definition
}

// ConstraintType represents the type of constraint
type ConstraintType int

const (
	ConstraintCheck ConstraintType = iota
	ConstraintForeignKey
	ConstraintUnique
)

// Row represents a row of data
type Row struct {
	Values []Value
}

// NewRow creates a new row with the given values
func NewRow(values ...Value) Row {
	return Row{Values: values}
}

// Get returns the value at the given index
func (r Row) Get(index int) Value {
	if index < 0 || index >= len(r.Values) {
		return NewNullValue()
	}
	return r.Values[index]
}

// Comparator is a function that compares two values
type Comparator func(a, b Value) int

// Serializer is a function that serializes a value
type Serializer func(v Value) ([]byte, error)

// Deserializer is a function that deserializes a value
type Deserializer func(data []byte) (Value, error)

// Validator is a function that validates a value
type Validator func(v Value) bool

// TimeValue wraps time.Time for SQL timestamp/date types
type TimeValue time.Time

// DecimalValue represents a decimal number with precision and scale
type DecimalValue struct {
	// Store as string for now, can optimize later
	Value     string
	Precision int
	Scale     int
}
