package types

import (
	"bytes"
	"testing"
)

func TestByteaType(t *testing.T) {
	t.Run("Name", func(t *testing.T) {
		if Bytea.Name() != "BYTEA" {
			t.Errorf("expected BYTEA, got %s", Bytea.Name())
		}
	})

	t.Run("Size", func(t *testing.T) {
		if Bytea.Size() != -1 {
			t.Errorf("expected -1 (variable size), got %d", Bytea.Size())
		}
	})

	t.Run("SerializeDeserialize", func(t *testing.T) {
		testCases := [][]byte{
			[]byte("Hello World"),
			[]byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
			[]byte{}, // empty
			make([]byte, 1000), // large
		}

		for _, data := range testCases {
			val := NewByteaValue(data)
			
			// Serialize
			serialized, err := Bytea.Serialize(val)
			if err != nil {
				t.Fatalf("Failed to serialize: %v", err)
			}

			// Deserialize
			deserialized, err := Bytea.Deserialize(serialized)
			if err != nil {
				t.Fatalf("Failed to deserialize: %v", err)
			}

			// Verify
			if deserialized.IsNull() {
				t.Fatal("Deserialized value should not be null")
			}

			deserializedData, ok := deserialized.Data.([]byte)
			if !ok {
				t.Fatalf("Expected []byte, got %T", deserialized.Data)
			}

			if !bytes.Equal(data, deserializedData) {
				t.Errorf("Data mismatch: expected %v, got %v", data, deserializedData)
			}
		}
	})

	t.Run("SerializeNull", func(t *testing.T) {
		val := NewNullValue()
		serialized, err := Bytea.Serialize(val)
		if err != nil {
			t.Fatalf("Failed to serialize NULL: %v", err)
		}
		if serialized != nil {
			t.Errorf("Expected nil for NULL value, got %v", serialized)
		}
	})

	t.Run("DeserializeNull", func(t *testing.T) {
		val, err := Bytea.Deserialize(nil)
		if err != nil {
			t.Fatalf("Failed to deserialize NULL: %v", err)
		}
		if !val.IsNull() {
			t.Error("Expected NULL value")
		}
	})

	t.Run("Compare", func(t *testing.T) {
		val1 := NewByteaValue([]byte("ABC"))
		val2 := NewByteaValue([]byte("DEF"))
		val3 := NewByteaValue([]byte("ABC"))

		if Bytea.Compare(val1, val2) >= 0 {
			t.Error("ABC should be less than DEF")
		}

		if Bytea.Compare(val2, val1) <= 0 {
			t.Error("DEF should be greater than ABC")
		}

		if Bytea.Compare(val1, val3) != 0 {
			t.Error("ABC should equal ABC")
		}
	})

	t.Run("ParseByteaLiteral", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected []byte
		}{
			{"\\x48656c6c6f", []byte("Hello")},
			{"'\\x48656c6c6f'", []byte("Hello")}, // with quotes
			{"\\x", []byte{}}, // empty
			{"\\x00FF", []byte{0x00, 0xFF}}, // binary
		}

		for _, tc := range testCases {
			result, err := ParseByteaLiteral(tc.input)
			if err != nil {
				t.Errorf("Failed to parse %s: %v", tc.input, err)
				continue
			}

			if !bytes.Equal(result, tc.expected) {
				t.Errorf("Parse %s: expected %v, got %v", tc.input, tc.expected, result)
			}
		}
	})

	t.Run("FormatByteaLiteral", func(t *testing.T) {
		testCases := []struct {
			input    []byte
			expected string
		}{
			{[]byte("Hello"), "\\x48656c6c6f"},
			{[]byte{}, "\\x"},
			{[]byte{0x00, 0xFF}, "\\x00ff"},
		}

		for _, tc := range testCases {
			result := FormatByteaLiteral(tc.input)
			if result != tc.expected {
				t.Errorf("Format %v: expected %s, got %s", tc.input, tc.expected, result)
			}
		}
	})

	t.Run("Zero", func(t *testing.T) {
		zero := Bytea.Zero()
		if zero.IsNull() {
			t.Error("Zero value should not be null")
		}

		data, ok := zero.Data.([]byte)
		if !ok {
			t.Fatalf("Expected []byte, got %T", zero.Data)
		}

		if len(data) != 0 {
			t.Errorf("Expected empty byte array, got %v", data)
		}
	})
}