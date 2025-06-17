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
	// Both non-null, compare actual values
	return 0 // Will be overridden by specific type
}

// Common SQL types
var (
	Integer    DataType
	BigInt     DataType
	SmallInt   DataType
	Boolean    DataType
	Varchar    func(size int) DataType
	Char       func(size int) DataType
	Text       DataType
	Timestamp  DataType
	Date       DataType
	Decimal    func(precision, scale int) DataType
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

// baseType provides common functionality for data types
type baseType struct {
	name         string
	size         int
	comparator   Comparator
	serializer   Serializer
	deserializer Deserializer
	validator    Validator
	zero         Value
}

func (t *baseType) Name() string {
	return t.name
}

func (t *baseType) Size() int {
	return t.size
}

func (t *baseType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}
	return t.comparator(a, b)
}

func (t *baseType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}
	return t.serializer(v)
}

func (t *baseType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}
	return t.deserializer(data)
}

func (t *baseType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	return t.validator(v)
}

func (t *baseType) Zero() Value {
	return t.zero
}

// Helper functions for creating base types
func newBaseType(name string, size int, zero Value) *baseType {
	return &baseType{
		name: name,
		size: size,
		zero: zero,
		validator: func(v Value) bool {
			return !v.Null
		},
	}
}

// TimeValue wraps time.Time for SQL timestamp/date types
type TimeValue time.Time

// DecimalValue represents a decimal number with precision and scale
type DecimalValue struct {
	// Store as string for now, can optimize later
	Value string
	Precision int
	Scale int
}