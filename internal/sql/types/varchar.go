package types

import (
	"encoding/binary"
	"fmt"
	"strings"
)

func init() {
	// Varchar is a function that returns a VARCHAR type with specified max length
	Varchar = func(maxLen int) DataType {
		return &varcharType{maxLen: maxLen}
	}

	// Char is a function that returns a CHAR type with specified length
	Char = func(length int) DataType {
		return &charType{length: length}
	}

	// Text is unbounded text
	Text = &textType{}
}

// varcharType implements the VARCHAR(n) data type
type varcharType struct {
	maxLen int
}

func (t *varcharType) Name() string {
	return fmt.Sprintf("VARCHAR(%d)", t.maxLen)
}

func (t *varcharType) Size() int {
	return -1 // Variable size
}

func (t *varcharType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aStr := a.Data.(string)
	bStr := b.Data.(string)

	return strings.Compare(aStr, bStr)
}

func (t *varcharType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	str, ok := v.Data.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", v.Data)
	}

	strLen := len(str)
	if strLen > t.maxLen {
		return nil, fmt.Errorf("string length %d exceeds maximum %d", strLen, t.maxLen)
	}
	if strLen > 4294967295 { // uint32 max
		return nil, fmt.Errorf("string too long: %d bytes", strLen)
	}

	// Serialize as: [4 bytes length][string data]
	buf := make([]byte, 4+strLen)
	binary.BigEndian.PutUint32(buf[:4], uint32(strLen)) // nolint:gosec // Length checked above
	copy(buf[4:], str)

	return buf, nil
}

func (t *varcharType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) < 4 {
		return Value{}, fmt.Errorf("invalid VARCHAR data: too short")
	}

	length := binary.BigEndian.Uint32(data[:4])
	if int(length) > t.maxLen {
		return Value{}, fmt.Errorf("stored string length %d exceeds maximum %d", length, t.maxLen)
	}

	if len(data) < 4+int(length) {
		return Value{}, fmt.Errorf("invalid VARCHAR data: expected %d bytes, got %d", 4+length, len(data))
	}

	str := string(data[4 : 4+length])
	return NewValue(str), nil
}

func (t *varcharType) IsValid(v Value) bool {
	if v.Null {
		return true
	}

	str, ok := v.Data.(string)
	if !ok {
		return false
	}

	return len(str) <= t.maxLen
}

func (t *varcharType) Zero() Value {
	return NewValue("")
}

// charType implements the CHAR(n) data type (fixed-length, padded)
type charType struct {
	length int
}

func (t *charType) Name() string {
	return fmt.Sprintf("CHAR(%d)", t.length)
}

func (t *charType) Size() int {
	return t.length
}

func (t *charType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aStr := a.Data.(string)
	bStr := b.Data.(string)

	// CHAR comparison ignores trailing spaces
	aStr = strings.TrimRight(aStr, " ")
	bStr = strings.TrimRight(bStr, " ")

	return strings.Compare(aStr, bStr)
}

func (t *charType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	str, ok := v.Data.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", v.Data)
	}

	if len(str) > t.length {
		return nil, fmt.Errorf("string length %d exceeds CHAR(%d)", len(str), t.length)
	}

	// Pad with spaces to fixed length
	padded := str + strings.Repeat(" ", t.length-len(str))
	return []byte(padded), nil
}

func (t *charType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) != t.length {
		return Value{}, fmt.Errorf("expected %d bytes for CHAR(%d), got %d", t.length, t.length, len(data))
	}

	// Remove trailing spaces for storage
	str := strings.TrimRight(string(data), " ")
	return NewValue(str), nil
}

func (t *charType) IsValid(v Value) bool {
	if v.Null {
		return true
	}

	str, ok := v.Data.(string)
	if !ok {
		return false
	}

	return len(str) <= t.length
}

func (t *charType) Zero() Value {
	return NewValue("")
}

// textType implements the TEXT data type (unbounded)
type textType struct{}

func (t *textType) Name() string {
	return "TEXT"
}

func (t *textType) Size() int {
	return -1 // Variable size
}

func (t *textType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aStr := a.Data.(string)
	bStr := b.Data.(string)

	return strings.Compare(aStr, bStr)
}

func (t *textType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	str, ok := v.Data.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", v.Data)
	}

	strLen := len(str)
	if strLen > 4294967295 { // uint32 max
		return nil, fmt.Errorf("string too long: %d bytes", strLen)
	}

	// Serialize as: [4 bytes length][string data]
	buf := make([]byte, 4+strLen)
	binary.BigEndian.PutUint32(buf[:4], uint32(strLen)) // nolint:gosec // Length checked above
	copy(buf[4:], str)

	return buf, nil
}

func (t *textType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) < 4 {
		return Value{}, fmt.Errorf("invalid TEXT data: too short")
	}

	length := binary.BigEndian.Uint32(data[:4])

	if len(data) < 4+int(length) {
		return Value{}, fmt.Errorf("invalid TEXT data: expected %d bytes, got %d", 4+length, len(data))
	}

	str := string(data[4 : 4+length])
	return NewValue(str), nil
}

func (t *textType) IsValid(v Value) bool {
	if v.Null {
		return true
	}

	_, ok := v.Data.(string)
	return ok
}

func (t *textType) Zero() Value {
	return NewValue("")
}

// Helper functions for creating string values

// NewVarcharValue creates a new VARCHAR value
func NewVarcharValue(s string) Value {
	return NewValue(s)
}

// NewCharValue creates a new CHAR value
func NewCharValue(s string) Value {
	return NewValue(s)
}

// NewTextValue creates a new TEXT value
func NewTextValue(s string) Value {
	return NewValue(s)
}
