package executor

import (
	"strings"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestCopyOperatorTextFormat(t *testing.T) {
	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Varchar(50), IsNullable: true},
			{Name: "age", DataType: types.Integer, IsNullable: true},
		},
	}

	// Create mock storage backend
	storage := &copyTestStorageBackend{
		insertedRows: make([]*Row, 0),
	}

	// Create COPY plan
	plan := &planner.LogicalCopy{
		TableName: "test_table",
		Columns:   []string{"id", "name", "age"},
		Direction: parser.CopyFrom,
		Source:    "STDIN",
		Options:   map[string]string{},
		TableRef:  table,
	}

	// Create operator
	op := NewCopyOperator(plan, nil, storage, nil)

	// Test data
	testData := `1	John	30
2	Jane	25
3	\N	\N
\.
`

	// Set reader
	op.SetReader(strings.NewReader(testData))

	// Execute
	ctx := &ExecContext{}
	if err := op.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	row, err := op.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	// Check row count
	if row == nil || len(row.Values) != 1 {
		t.Fatal("Expected one value in result row")
	}

	rowCount, err := row.Values[0].AsInt()
	if err != nil {
		t.Fatalf("Failed to get row count: %v", err)
	}

	if rowCount != 3 {
		t.Errorf("Expected 3 rows copied, got %d", rowCount)
	}

	// Verify inserted data
	if len(storage.insertedRows) != 3 {
		t.Fatalf("Expected 3 inserted rows, got %d", len(storage.insertedRows))
	}

	// Check first row
	row1 := storage.insertedRows[0]
	if val, _ := row1.Values[0].AsInt(); val != 1 {
		t.Errorf("Row 1: expected id=1, got %d", val)
	}
	if val, _ := row1.Values[1].AsString(); val != "John" {
		t.Errorf("Row 1: expected name=John, got %s", val)
	}
	if val, _ := row1.Values[2].AsInt(); val != 30 {
		t.Errorf("Row 1: expected age=30, got %d", val)
	}

	// Check third row (with NULLs)
	row3 := storage.insertedRows[2]
	if val, _ := row3.Values[0].AsInt(); val != 3 {
		t.Errorf("Row 3: expected id=3, got %d", val)
	}
	if !row3.Values[1].IsNull() {
		t.Error("Row 3: expected name to be NULL")
	}
	if !row3.Values[2].IsNull() {
		t.Error("Row 3: expected age to be NULL")
	}
}

func TestCopyOperatorCSVFormat(t *testing.T) {
	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Varchar(50), IsNullable: true},
			{Name: "active", DataType: types.Boolean, IsNullable: true},
		},
	}

	// Create mock storage backend
	storage := &copyTestStorageBackend{
		insertedRows: make([]*Row, 0),
	}

	// Create COPY plan
	plan := &planner.LogicalCopy{
		TableName: "test_table",
		Columns:   []string{"id", "name", "active"},
		Direction: parser.CopyFrom,
		Source:    "STDIN",
		Options: map[string]string{
			"FORMAT":    "CSV",
			"DELIMITER": ",",
			"HEADER":    "",
		},
		TableRef: table,
	}

	// Create operator
	op := NewCopyOperator(plan, nil, storage, nil)

	// Test data with header
	testData := `id,name,active
1,Alice,true
2,Bob,false
3,Charlie,
`

	// Set reader
	op.SetReader(strings.NewReader(testData))

	// Execute
	ctx := &ExecContext{}
	if err := op.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	row, err := op.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	// Check row count
	rowCount, _ := row.Values[0].AsInt()
	if rowCount != 3 {
		t.Errorf("Expected 3 rows copied, got %d", rowCount)
	}

	// Verify inserted data
	if len(storage.insertedRows) != 3 {
		t.Fatalf("Expected 3 inserted rows, got %d", len(storage.insertedRows))
	}

	// Check boolean values
	row1 := storage.insertedRows[0]
	if val, _ := row1.Values[2].AsBool(); !val {
		t.Error("Row 1: expected active=true")
	}

	row2 := storage.insertedRows[1]
	if val, _ := row2.Values[2].AsBool(); val {
		t.Error("Row 2: expected active=false")
	}
}

func TestCopyOperatorColumnSubset(t *testing.T) {
	// Create test table
	table := &catalog.Table{
		ID:        1,
		TableName: "test_table",
		Columns: []*catalog.Column{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Varchar(50), IsNullable: true},
			{Name: "age", DataType: types.Integer, IsNullable: true},
			{Name: "city", DataType: types.Varchar(50), IsNullable: true},
		},
	}

	// Create mock storage backend
	storage := &copyTestStorageBackend{
		insertedRows: make([]*Row, 0),
	}

	// Create COPY plan with subset of columns
	plan := &planner.LogicalCopy{
		TableName: "test_table",
		Columns:   []string{"id", "name"}, // Only id and name
		Direction: parser.CopyFrom,
		Source:    "STDIN",
		Options:   map[string]string{},
		TableRef:  table,
	}

	// Create operator
	op := NewCopyOperator(plan, nil, storage, nil)

	// Test data
	testData := `1	Alice
2	Bob
\.
`

	// Set reader
	op.SetReader(strings.NewReader(testData))

	// Execute
	ctx := &ExecContext{}
	if err := op.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	row, err := op.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	// Check row count
	rowCount, _ := row.Values[0].AsInt()
	if rowCount != 2 {
		t.Errorf("Expected 2 rows copied, got %d", rowCount)
	}

	// Verify inserted data
	if len(storage.insertedRows) != 2 {
		t.Fatalf("Expected 2 inserted rows, got %d", len(storage.insertedRows))
	}

	// Check that unspecified columns are NULL
	row1 := storage.insertedRows[0]
	if !row1.Values[2].IsNull() {
		t.Error("Row 1: expected age to be NULL")
	}
	if !row1.Values[3].IsNull() {
		t.Error("Row 1: expected city to be NULL")
	}
}

// copyTestStorageBackend is a mock implementation of StorageBackend for testing
type copyTestStorageBackend struct {
	insertedRows []*Row
}

func (m *copyTestStorageBackend) CreateTable(table *catalog.Table) error {
	return nil
}

func (m *copyTestStorageBackend) DropTable(tableID int64) error {
	return nil
}

func (m *copyTestStorageBackend) InsertRow(tableID int64, row *Row) (RowID, error) {
	m.insertedRows = append(m.insertedRows, row)
	return RowID{PageID: 1, SlotID: uint16(len(m.insertedRows))}, nil
}

func (m *copyTestStorageBackend) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	return nil
}

func (m *copyTestStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	return nil
}

func (m *copyTestStorageBackend) ScanTable(tableID int64, snapshotTS int64) (RowIterator, error) {
	return nil, nil
}

func (m *copyTestStorageBackend) GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error) {
	return nil, nil
}

func (m *copyTestStorageBackend) SetTransactionID(txnID uint64) {
}