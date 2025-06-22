package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestInsertWithParameters(t *testing.T) {
	// Create a mock storage backend
	storage := &mockInsertStorageBackend{
		insertedRows: make([]*Row, 0),
	}

	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{
				Name:       "id",
				DataType:   types.Integer,
				IsNullable: false,
			},
			{
				Name:       "name",
				DataType:   types.Text,
				IsNullable: false,
			},
		},
	}

	// Test cases
	tests := []struct {
		name     string
		values   [][]parser.Expression
		params   []types.Value
		expected []types.Value
		wantErr  bool
		errMsg   string
	}{
		{
			name: "insert with literal and parameter",
			values: [][]parser.Expression{
				{
					&parser.Literal{Value: types.NewValue(int64(1))},
					&parser.ParameterRef{Index: 1},
				},
			},
			params: []types.Value{
				types.NewValue("John"),
			},
			expected: []types.Value{
				types.NewValue(int64(1)),
				types.NewValue("John"),
			},
			wantErr: false,
		},
		{
			name: "insert with multiple parameters",
			values: [][]parser.Expression{
				{
					&parser.ParameterRef{Index: 1},
					&parser.ParameterRef{Index: 2},
				},
			},
			params: []types.Value{
				types.NewValue(int64(2)),
				types.NewValue("Jane"),
			},
			expected: []types.Value{
				types.NewValue(int64(2)),
				types.NewValue("Jane"),
			},
			wantErr: false,
		},
		{
			name: "parameter out of range",
			values: [][]parser.Expression{
				{
					&parser.Literal{Value: types.NewValue(int64(3))},
					&parser.ParameterRef{Index: 2}, // Only 1 param provided
				},
			},
			params: []types.Value{
				types.NewValue("Test"),
			},
			wantErr: true,
			errMsg:  "parameter $2 out of range",
		},
		{
			name: "no parameters provided",
			values: [][]parser.Expression{
				{
					&parser.ParameterRef{Index: 1},
					&parser.Literal{Value: types.NewValue("NoParam")},
				},
			},
			params:  nil,
			wantErr: true,
			errMsg:  "no parameters available for parameter $1",
		},
		{
			name: "unsupported expression type",
			values: [][]parser.Expression{
				{
					&parser.Literal{Value: types.NewValue(int64(1))},
					// Use a simple identifier which is not supported
					&parser.Identifier{Name: "column_name"},
				},
			},
			params:  nil,
			wantErr: true,
			errMsg:  "INSERT only supports literal values and parameters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset storage
			storage.insertedRows = make([]*Row, 0)

			// Create INSERT operator
			insertOp := NewInsertOperator(table, storage, tt.values)

			// Create execution context
			ctx := &ExecContext{
				Params: tt.params,
			}

			// Execute
			err := insertOp.Open(ctx)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errMsg != "" && !containsStr(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing '%s', got '%s'", tt.errMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Check inserted row
			if len(storage.insertedRows) != 1 {
				t.Errorf("expected 1 inserted row, got %d", len(storage.insertedRows))
				return
			}

			insertedRow := storage.insertedRows[0]
			if len(insertedRow.Values) != len(tt.expected) {
				t.Errorf("expected %d values, got %d", len(tt.expected), len(insertedRow.Values))
				return
			}

			for i, expectedVal := range tt.expected {
				if !insertedRow.Values[i].Equal(expectedVal) {
					t.Errorf("value mismatch at index %d: expected %v, got %v",
						i, expectedVal, insertedRow.Values[i])
				}
			}
		})
	}
}

// mockInsertStorageBackend for testing
type mockInsertStorageBackend struct {
	insertedRows []*Row
	txnID        uint64
}

func (m *mockInsertStorageBackend) ScanTable(tableID int64, snapshotTS int64) (RowIterator, error) {
	return nil, nil
}

func (m *mockInsertStorageBackend) InsertRow(tableID int64, row *Row) (RowID, error) {
	m.insertedRows = append(m.insertedRows, row)
	return RowID{PageID: 1, SlotID: uint16(len(m.insertedRows))}, nil
}

func (m *mockInsertStorageBackend) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	return nil
}

func (m *mockInsertStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	return nil
}

func (m *mockInsertStorageBackend) GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error) {
	return nil, nil
}

func (m *mockInsertStorageBackend) SetTransactionID(txnID uint64) {
	m.txnID = txnID
}

func (m *mockInsertStorageBackend) CreateTable(table *catalog.Table) error {
	return nil
}

func (m *mockInsertStorageBackend) DropTable(tableID int64) error {
	return nil
}

// Helper function
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || containsStr(s[1:], substr)))
}
