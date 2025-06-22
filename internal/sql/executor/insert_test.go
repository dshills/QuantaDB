package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestInsertOperatorWithParameters(t *testing.T) {
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

	// Create a simple mock storage
	var insertedRow *Row
	storage := &testInsertStorage{
		insertFunc: func(tableID int64, row *Row) (RowID, error) {
			insertedRow = row
			return RowID{PageID: 1, SlotID: 1}, nil
		},
	}

	// Test with parameter reference
	values := [][]parser.Expression{
		{
			&parser.Literal{Value: types.NewValue(int64(1))},
			&parser.ParameterRef{Index: 1},
		},
	}

	insertOp := NewInsertOperator(table, storage, values)

	// Create execution context with parameter
	ctx := &ExecContext{
		Params: []types.Value{
			types.NewValue("John Doe"),
		},
	}

	// Execute
	err := insertOp.Open(ctx)
	if err != nil {
		t.Fatalf("failed to execute INSERT: %v", err)
	}

	// Check inserted row
	if insertedRow == nil {
		t.Fatal("no row was inserted")
	}

	if len(insertedRow.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(insertedRow.Values))
	}

	// Check values
	if v, ok := insertedRow.Values[0].Data.(int64); !ok || v != 1 {
		t.Errorf("expected first value to be int64(1), got %v", insertedRow.Values[0])
	}

	if v, ok := insertedRow.Values[1].Data.(string); !ok || v != "John Doe" {
		t.Errorf("expected second value to be 'John Doe', got %v", insertedRow.Values[1])
	}
}

// testInsertStorage is a minimal storage implementation for testing
type testInsertStorage struct {
	insertFunc func(int64, *Row) (RowID, error)
}

func (s *testInsertStorage) CreateTable(table *catalog.Table) error {
	return nil
}

func (s *testInsertStorage) DropTable(tableID int64) error {
	return nil
}

func (s *testInsertStorage) InsertRow(tableID int64, row *Row) (RowID, error) {
	if s.insertFunc != nil {
		return s.insertFunc(tableID, row)
	}
	return RowID{}, nil
}

func (s *testInsertStorage) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	return nil
}

func (s *testInsertStorage) DeleteRow(tableID int64, rowID RowID) error {
	return nil
}

func (s *testInsertStorage) ScanTable(tableID int64, snapshotTS int64) (RowIterator, error) {
	return nil, nil
}

func (s *testInsertStorage) GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error) {
	return nil, nil
}

func (s *testInsertStorage) SetTransactionID(txnID uint64) {
}