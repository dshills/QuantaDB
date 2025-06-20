package executor

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestMVCCRowSerialization(t *testing.T) {
	// Create a test schema
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer, Nullable: false},
			{Name: "name", Type: types.Text, Nullable: false},
			{Name: "age", Type: types.Integer, Nullable: true},
		},
	}

	// Create a test row
	row := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue("Alice"),
			types.NewValue(int32(30)),
		},
	}

	// Create MVCC row with metadata
	mvccRow := &MVCCRow{
		Header: MVCCRowHeader{
			RowID:        12345,
			CreatedByTxn: 100,
			CreatedAt:    time.Now().Unix(),
			DeletedByTxn: 0,
			DeletedAt:    0,
			NextVersion:  0,
		},
		Data: row,
	}

	// Test serialization
	format := NewMVCCRowFormat(schema)
	data, err := format.Serialize(mvccRow)
	if err != nil {
		t.Fatalf("Failed to serialize MVCC row: %v", err)
	}

	// Test deserialization
	deserializedRow, err := format.Deserialize(data)
	if err != nil {
		t.Fatalf("Failed to deserialize MVCC row: %v", err)
	}

	// Verify header fields
	if deserializedRow.Header.RowID != mvccRow.Header.RowID {
		t.Errorf("RowID mismatch: expected %d, got %d", mvccRow.Header.RowID, deserializedRow.Header.RowID)
	}
	if deserializedRow.Header.CreatedByTxn != mvccRow.Header.CreatedByTxn {
		t.Errorf("CreatedByTxn mismatch: expected %d, got %d", mvccRow.Header.CreatedByTxn, deserializedRow.Header.CreatedByTxn)
	}
	if deserializedRow.Header.CreatedAt != mvccRow.Header.CreatedAt {
		t.Errorf("CreatedAt mismatch: expected %d, got %d", mvccRow.Header.CreatedAt, deserializedRow.Header.CreatedAt)
	}

	// Verify data values
	if len(deserializedRow.Data.Values) != len(row.Values) {
		t.Fatalf("Value count mismatch: expected %d, got %d", len(row.Values), len(deserializedRow.Data.Values))
	}

	// Check first value (ID)
	if v, ok := deserializedRow.Data.Values[0].Data.(int32); !ok || v != 1 {
		t.Errorf("ID value mismatch: expected 1, got %v", deserializedRow.Data.Values[0].Data)
	}

	// Check second value (Name)
	if v, ok := deserializedRow.Data.Values[1].Data.(string); !ok || v != "Alice" {
		t.Errorf("Name value mismatch: expected 'Alice', got %v", deserializedRow.Data.Values[1].Data)
	}
}

func TestMVCCRowDeleted(t *testing.T) {
	// Create a deleted row
	mvccRow := &MVCCRow{
		Header: MVCCRowHeader{
			CreatedByTxn: 100,
			CreatedAt:    time.Now().Unix(),
			DeletedByTxn: 200,
			DeletedAt:    time.Now().Unix(),
		},
		Data: &Row{},
	}

	if !mvccRow.IsDeleted() {
		t.Error("Expected row to be marked as deleted")
	}

	// Create a non-deleted row
	mvccRow2 := &MVCCRow{
		Header: MVCCRowHeader{
			CreatedByTxn: 100,
			CreatedAt:    time.Now().Unix(),
			DeletedByTxn: 0,
			DeletedAt:    0,
		},
		Data: &Row{},
	}

	if mvccRow2.IsDeleted() {
		t.Error("Expected row to not be marked as deleted")
	}
}

func TestVersionPointerEncoding(t *testing.T) {
	tests := []struct {
		pageID uint32
		slotID uint16
	}{
		{0, 0},
		{1, 2},
		{12345, 678},
		{4294967295, 65535}, // Max values
	}

	for _, test := range tests {
		encoded := EncodeVersionPointer(test.pageID, test.slotID)
		decodedPageID, decodedSlotID := DecodeVersionPointer(encoded)

		if decodedPageID != test.pageID {
			t.Errorf("PageID mismatch: expected %d, got %d", test.pageID, decodedPageID)
		}
		if decodedSlotID != test.slotID {
			t.Errorf("SlotID mismatch: expected %d, got %d", test.slotID, decodedSlotID)
		}
	}
}

func TestMVCCVisibility(t *testing.T) {
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer, Nullable: false},
		},
	}

	baseTime := time.Now().Unix()

	tests := []struct {
		name          string
		createdAt     int64
		deletedAt     int64
		snapshotTime  int64
		expectVisible bool
	}{
		{
			name:          "Row created before snapshot",
			createdAt:     baseTime - 100,
			deletedAt:     0,
			snapshotTime:  baseTime,
			expectVisible: true,
		},
		{
			name:          "Row created at snapshot",
			createdAt:     baseTime,
			deletedAt:     0,
			snapshotTime:  baseTime,
			expectVisible: true,
		},
		{
			name:          "Row created after snapshot",
			createdAt:     baseTime + 100,
			deletedAt:     0,
			snapshotTime:  baseTime,
			expectVisible: false,
		},
		{
			name:          "Row deleted before snapshot",
			createdAt:     baseTime - 200,
			deletedAt:     baseTime - 100,
			snapshotTime:  baseTime,
			expectVisible: false,
		},
		{
			name:          "Row deleted at snapshot",
			createdAt:     baseTime - 100,
			deletedAt:     baseTime,
			snapshotTime:  baseTime,
			expectVisible: false,
		},
		{
			name:          "Row deleted after snapshot",
			createdAt:     baseTime - 100,
			deletedAt:     baseTime + 100,
			snapshotTime:  baseTime,
			expectVisible: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := &MVCCRow{
				Header: MVCCRowHeader{
					CreatedAt: test.createdAt,
					DeletedAt: test.deletedAt,
				},
				Data: &Row{},
			}

			iterator := &MVCCRowIterator{
				txnID:     1, // Not used in visibility check anymore
				timestamp: test.snapshotTime,
				rowFormat: NewMVCCRowFormat(schema),
			}

			visible := iterator.isVisible(row)
			if visible != test.expectVisible {
				t.Errorf("Visibility mismatch: expected %v, got %v", test.expectVisible, visible)
			}
		})
	}
}
