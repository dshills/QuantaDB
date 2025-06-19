package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// mockStorageBackend implements StorageBackend for testing
type mockStorageBackend struct {
	tables map[int64][]*Row
}

func newMockStorageBackend() *mockStorageBackend {
	return &mockStorageBackend{
		tables: make(map[int64][]*Row),
	}
}

func (m *mockStorageBackend) CreateTable(table *catalog.Table) error {
	m.tables[table.ID] = []*Row{}
	return nil
}

func (m *mockStorageBackend) DropTable(tableID int64) error {
	delete(m.tables, tableID)
	return nil
}

func (m *mockStorageBackend) InsertRow(tableID int64, row *Row) (RowID, error) {
	m.tables[tableID] = append(m.tables[tableID], row)
	return RowID{PageID: 1, SlotID: uint16(len(m.tables[tableID]))}, nil
}

func (m *mockStorageBackend) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	// Not implemented for this test
	return nil
}

func (m *mockStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	// Not implemented for this test
	return nil
}

func (m *mockStorageBackend) ScanTable(tableID int64) (RowIterator, error) {
	rows, ok := m.tables[tableID]
	if !ok {
		return nil, nil //nolint:nilnil // No table found
	}
	return &mockRowIterator{rows: rows, pos: 0}, nil
}

func (m *mockStorageBackend) GetRow(tableID int64, rowID RowID) (*Row, error) {
	// Not implemented for this test
	return nil, nil //nolint:nilnil // Not implemented for test
}

func (m *mockStorageBackend) SetTransactionID(txnID uint64) {
	// Not implemented for this test
}

// mockRowIterator implements RowIterator
type mockRowIterator struct {
	rows []*Row
	pos  int
}

func (m *mockRowIterator) Next() (*Row, RowID, error) {
	if m.pos >= len(m.rows) {
		return nil, RowID{}, nil
	}
	row := m.rows[m.pos]
	rowID := RowID{PageID: 1, SlotID: uint16(m.pos + 1)}
	m.pos++
	return row, rowID, nil
}

func (m *mockRowIterator) Close() error {
	return nil
}

func TestAnalyzeOperator(t *testing.T) {
	// Create a memory catalog
	cat := catalog.NewMemoryCatalog()

	// Create a test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{
				Name:       "id",
				DataType:   types.Integer,
				IsNullable: false,
			},
			{
				Name:       "name",
				DataType:   types.Text,
				IsNullable: true,
			},
			{
				Name:       "score",
				DataType:   types.Integer,
				IsNullable: true,
			},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create mock storage with test data
	storage := newMockStorageBackend()
	storage.CreateTable(table)

	// Insert test data
	testData := []struct {
		id    int64
		name  *string
		score *int64
	}{
		{1, strPtr("Alice"), int64Ptr(100)},
		{2, strPtr("Bob"), int64Ptr(85)},
		{3, nil, int64Ptr(90)},      // NULL name
		{4, strPtr("Charlie"), nil}, // NULL score
		{5, strPtr("David"), int64Ptr(95)},
		{6, strPtr("Eve"), int64Ptr(88)},
		{7, strPtr("Frank"), int64Ptr(92)},
		{8, strPtr("Grace"), int64Ptr(87)},
		{9, strPtr("Henry"), int64Ptr(91)},
		{10, strPtr("Iris"), int64Ptr(89)},
	}

	for _, data := range testData {
		values := []types.Value{
			types.NewValue(data.id),
		}

		// Handle nullable columns properly
		if data.name != nil {
			values = append(values, types.NewValue(*data.name))
		} else {
			values = append(values, types.NewNullValue()) // NULL value
		}

		if data.score != nil {
			values = append(values, types.NewValue(*data.score))
		} else {
			values = append(values, types.NewNullValue()) // NULL value
		}

		row := &Row{Values: values}
		storage.InsertRow(table.ID, row)
	}

	// Create ANALYZE plan
	analyzePlan := planner.NewLogicalAnalyze("public", "test_table", []string{})

	// Create ANALYZE operator
	analyzeOp := NewAnalyzeOperator(analyzePlan, cat, storage)

	// Open the operator
	ctx := &ExecContext{
		Catalog: cat,
		Stats:   &ExecStats{},
	}
	if err := analyzeOp.Open(ctx); err != nil {
		t.Fatalf("failed to open analyze operator: %v", err)
	}

	// Execute ANALYZE
	result, err := analyzeOp.Next()
	if err != nil {
		t.Fatalf("failed to execute analyze: %v", err)
	}

	if result == nil {
		t.Fatal("expected result from analyze")
	}

	// Check that result has expected message
	if len(result.Values) != 1 {
		t.Fatalf("expected 1 value in result, got %d", len(result.Values))
	}

	msg := result.Values[0].Data.(string)
	expected := "ANALYZE completed for table public.test_table"
	if msg != expected {
		t.Errorf("expected message %q, got %q", expected, msg)
	}

	// Verify no more results
	result2, err := analyzeOp.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result2 != nil {
		t.Fatal("expected no more results")
	}

	// Close the operator
	if err := analyzeOp.Close(); err != nil {
		t.Fatalf("failed to close operator: %v", err)
	}

	// Verify statistics were updated
	table2, err := cat.GetTable("public", "test_table")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Check table statistics
	if table2.Stats == nil {
		t.Fatal("expected table statistics to be set")
	}

	if table2.Stats.RowCount != 10 {
		t.Errorf("expected row count 10, got %d", table2.Stats.RowCount)
	}

	// Check column statistics
	for _, col := range table2.Columns {
		if col.Stats == nil {
			t.Errorf("expected statistics for column %s", col.Name)
			continue
		}

		t.Logf("Column %s: NullCount=%d, DistinctCount=%d", col.Name, col.Stats.NullCount, col.Stats.DistinctCount)

		switch col.Name {
		case "id":
			if col.Stats.NullCount != 0 {
				t.Errorf("expected 0 nulls in id column, got %d", col.Stats.NullCount)
			}
			if col.Stats.DistinctCount != 10 {
				t.Errorf("expected 10 distinct values in id column, got %d", col.Stats.DistinctCount)
			}
		case "name":
			if col.Stats.NullCount != 1 {
				t.Errorf("expected 1 null in name column, got %d", col.Stats.NullCount)
			}
			// Distinct count should be 9 (not counting NULL)
			if col.Stats.DistinctCount != 9 {
				t.Errorf("expected 9 distinct non-null values in name column, got %d", col.Stats.DistinctCount)
			}
		case "score":
			if col.Stats.NullCount != 1 {
				t.Errorf("expected 1 null in score column, got %d", col.Stats.NullCount)
			}
			// Check histogram exists
			if col.Stats.Histogram == nil {
				t.Error("expected histogram for score column")
			}
		}
	}
}

func TestAnalyzeOperatorWithSpecificColumns(t *testing.T) {
	// Create a memory catalog
	cat := catalog.NewMemoryCatalog()

	// Create a test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{
				Name:       "id",
				DataType:   types.Integer,
				IsNullable: false,
			},
			{
				Name:       "name",
				DataType:   types.Text,
				IsNullable: true,
			},
			{
				Name:       "score",
				DataType:   types.Integer,
				IsNullable: true,
			},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create mock storage
	storage := newMockStorageBackend()
	storage.CreateTable(table)

	// Insert minimal test data
	row := &Row{
		Values: []types.Value{
			types.NewValue(int64(1)),
			types.NewValue("test"),
			types.NewValue(int64(100)),
		},
	}
	storage.InsertRow(table.ID, row)

	// Create ANALYZE plan for specific columns
	analyzePlan := planner.NewLogicalAnalyze("public", "test_table", []string{"id", "score"})

	// Create ANALYZE operator
	analyzeOp := NewAnalyzeOperator(analyzePlan, cat, storage)

	// Open and execute
	ctx := &ExecContext{
		Catalog: cat,
		Stats:   &ExecStats{},
	}
	if err := analyzeOp.Open(ctx); err != nil {
		t.Fatalf("failed to open analyze operator: %v", err)
	}

	result, err := analyzeOp.Next()
	if err != nil {
		t.Fatalf("failed to execute analyze: %v", err)
	}

	if result == nil {
		t.Fatal("expected result from analyze")
	}

	// Close the operator
	if err := analyzeOp.Close(); err != nil {
		t.Fatalf("failed to close operator: %v", err)
	}

	// Verify statistics were updated only for specified columns
	table2, err := cat.GetTable("public", "test_table")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	for _, col := range table2.Columns {
		switch col.Name {
		case "id", "score":
			if col.Stats == nil {
				t.Errorf("expected statistics for column %s", col.Name)
			}
		case "name":
			// Name column should not have been analyzed
			if col.Stats != nil {
				t.Errorf("expected no statistics for column %s", col.Name)
			}
		}
	}
}

// Helper functions
func strPtr(s string) *string {
	return &s
}

func int64Ptr(i int64) *int64 {
	return &i
}
