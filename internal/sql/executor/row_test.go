package executor

import (
	"fmt"
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestRowSerialization(t *testing.T) {
	// Create test schema
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer, Nullable: false},
			{Name: "name", Type: types.Text, Nullable: false},
			{Name: "age", Type: types.Integer, Nullable: true},
			{Name: "active", Type: types.Boolean, Nullable: false},
			{Name: "salary", Type: types.Decimal(10, 2), Nullable: true},
		},
	}

	rowFormat := NewRowFormat(schema)

	tests := []struct {
		name string
		row  *Row
	}{
		{
			name: "All non-null values",
			row: &Row{
				Values: []types.Value{
					types.NewValue(int64(1)),
					types.NewValue("John Doe"),
					types.NewValue(int64(30)),
					types.NewValue(true),
					types.NewValue(float64(75000.50)), // For decimal, we'll use float64 for now
				},
			},
		},
		{
			name: "With null values",
			row: &Row{
				Values: []types.Value{
					types.NewValue(int64(2)),
					types.NewValue("Jane Smith"),
					types.NewNullValue(), // null age
					types.NewValue(false),
					types.NewNullValue(), // null salary
				},
			},
		},
		{
			name: "Empty string",
			row: &Row{
				Values: []types.Value{
					types.NewValue(int64(3)),
					types.NewValue(""),
					types.NewValue(int64(25)),
					types.NewValue(true),
					types.NewValue(float64(0.0)),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			data, err := rowFormat.Serialize(tt.row)
			if err != nil {
				t.Fatalf("Failed to serialize row: %v", err)
			}

			// Deserialize
			deserialized, err := rowFormat.Deserialize(data)
			if err != nil {
				t.Fatalf("Failed to deserialize row: %v", err)
			}

			// Compare
			if len(deserialized.Values) != len(tt.row.Values) {
				t.Fatalf("Expected %d values, got %d", len(tt.row.Values), len(deserialized.Values))
			}

			for i, expected := range tt.row.Values {
				actual := deserialized.Values[i]

				// Compare null status
				if expected.IsNull() != actual.IsNull() {
					t.Errorf("Value %d: expected IsNull=%v, got %v", i, expected.IsNull(), actual.IsNull())
					continue
				}

				// If both are null, continue
				if expected.IsNull() && actual.IsNull() {
					continue
				}

				// Compare data
				if expected.Data != actual.Data {
					t.Errorf("Value %d: expected %v (%T), got %v (%T)",
						i, expected.Data, expected.Data, actual.Data, actual.Data)
				}
			}
		})
	}
}

func TestRowKeyFormat(t *testing.T) {
	keyFormat := &RowKeyFormat{
		TableName:  "users",
		SchemaName: "public",
	}

	tests := []struct {
		name       string
		primaryKey interface{}
		wantKey    string
	}{
		{
			name:       "Integer primary key",
			primaryKey: 42,
			wantKey:    "table:public:users:row:42",
		},
		{
			name:       "String primary key",
			primaryKey: "user123",
			wantKey:    "table:public:users:row:user123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Generate key
			key := keyFormat.GenerateRowKey(tt.primaryKey)
			if string(key) != tt.wantKey {
				t.Errorf("Expected key %q, got %q", tt.wantKey, string(key))
			}

			// Check if it's recognized as a row key
			if !keyFormat.IsRowKey(key) {
				t.Errorf("Key should be recognized as row key")
			}

			// Parse key
			pk, err := keyFormat.ParseRowKey(key)
			if err != nil {
				t.Fatalf("Failed to parse key: %v", err)
			}

			expectedPK := fmt.Sprintf("%v", tt.primaryKey)
			if pk != expectedPK {
				t.Errorf("Expected primary key %q, got %q", expectedPK, pk)
			}
		})
	}

	// Test non-row keys
	nonRowKeys := [][]byte{
		[]byte("table:other:users:row:1"),
		[]byte("table:public:other:row:1"),
		[]byte("index:public:users:idx:1"),
		[]byte("metadata:table:users"),
	}

	for _, key := range nonRowKeys {
		if keyFormat.IsRowKey(key) {
			t.Errorf("Key %q should not be recognized as row key", string(key))
		}
	}
}

func TestSerializationErrors(t *testing.T) {
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer, Nullable: false},
			{Name: "name", Type: types.Text, Nullable: false},
		},
	}

	rowFormat := NewRowFormat(schema)

	t.Run("Wrong number of values", func(t *testing.T) {
		row := &Row{
			Values: []types.Value{
				types.NewValue(int64(1)),
				// Missing name value
			},
		}

		_, err := rowFormat.Serialize(row)
		if err == nil {
			t.Fatal("Expected error for wrong number of values")
		}
	})

	t.Run("Invalid data for deserialization", func(t *testing.T) {
		invalidData := []byte{0x00, 0x01, 0x02} // Too short
		_, err := rowFormat.Deserialize(invalidData)
		if err == nil {
			t.Fatal("Expected error for invalid data")
		}
	})
}
