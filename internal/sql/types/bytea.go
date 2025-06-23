package types

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
)

func init() {
	Bytea = &byteaType{}
}

// byteaType implements the BYTEA data type for binary data
type byteaType struct{}

func (t *byteaType) Name() string {
	return "BYTEA"
}

func (t *byteaType) Size() int {
	return -1 // Variable size
}

func (t *byteaType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aVal, ok := a.Data.([]byte)
	if !ok {
		panic(fmt.Sprintf("BYTEA Compare: expected []byte, got %T", a.Data))
	}
	bVal, ok := b.Data.([]byte)
	if !ok {
		panic(fmt.Sprintf("BYTEA Compare: expected []byte, got %T", b.Data))
	}

	return bytes.Compare(aVal, bVal)
}

func (t *byteaType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	val, ok := v.Data.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected []byte, got %T", v.Data)
	}

	// Serialize as length-prefixed byte array
	// Format: [4 bytes length][n bytes data]
	length := len(val)
	if length > math.MaxUint32 {
		return nil, fmt.Errorf("bytea value too large: %d bytes", length)
	}
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[:4], uint32(length))
	copy(buf[4:], val)
	return buf, nil
}

func (t *byteaType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) < 4 {
		return Value{}, fmt.Errorf("expected at least 4 bytes for BYTEA length, got %d", len(data))
	}

	length := binary.BigEndian.Uint32(data[:4])
	if int(length) > len(data)-4 {
		return Value{}, fmt.Errorf("BYTEA length %d exceeds available data %d", length, len(data)-4)
	}

	// Make a copy of the data to avoid referencing the original buffer
	val := make([]byte, length)
	copy(val, data[4:4+length])
	return NewValue(val), nil
}

func (t *byteaType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	_, ok := v.Data.([]byte)
	return ok
}

func (t *byteaType) Zero() Value {
	return NewValue([]byte{})
}

// Helper functions for creating and manipulating bytea values

// NewByteaValue creates a new BYTEA value
func NewByteaValue(v []byte) Value {
	if v == nil {
		return NewNullValue()
	}
	// Make a copy to avoid aliasing issues
	val := make([]byte, len(v))
	copy(val, v)
	return NewValue(val)
}

// ParseByteaLiteral parses a PostgreSQL-style bytea literal
// Supports hex format: \x48656c6c6f
func ParseByteaLiteral(s string) ([]byte, error) {
	// Remove quotes if present
	s = strings.Trim(s, "'\"")

	// Check for hex format
	if strings.HasPrefix(s, "\\x") {
		hexStr := s[2:]
		return hex.DecodeString(hexStr)
	}

	// For now, only support hex format
	// TODO: Add support for escape format
	return nil, fmt.Errorf("unsupported bytea format, use \\xHEX format")
}

// FormatByteaLiteral formats bytes as a PostgreSQL-style bytea literal
func FormatByteaLiteral(data []byte) string {
	return "\\x" + hex.EncodeToString(data)
}
