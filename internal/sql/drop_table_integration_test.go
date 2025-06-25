package sql

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/storage"
)

func TestDropTableIntegration(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewMemoryCatalog()
	storage := &mockStorageForDropTest{
		tables: make(map[int64][]*executor.Row),
	}
	exec := executor.NewBasicExecutor(cat, nil)
	exec.SetStorageBackend(storage)

	// Create a test table first
	createSQL := "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))"

	// Parse CREATE TABLE
	p := parser.NewParser(createSQL)
	createStmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Plan CREATE TABLE
	plnr := planner.NewBasicPlannerWithCatalog(cat)
	createPlan, err := plnr.Plan(createStmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	// Execute CREATE TABLE
	ctx := &executor.ExecContext{
		Catalog: cat,
		Stats:   &executor.ExecStats{},
	}

	result, err := exec.Execute(createPlan, ctx)
	if err != nil {
		t.Fatalf("Execute CREATE TABLE failed: %v", err)
	}

	// Consume CREATE TABLE result
	row, err := result.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row == nil {
		t.Fatal("Expected CREATE TABLE result")
	}

	result.Close()

	// Verify table exists
	_, err = cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("Table should exist after creation: %v", err)
	}

	// Now test DROP TABLE
	dropSQL := "DROP TABLE users"

	// Parse DROP TABLE
	p = parser.NewParser(dropSQL)
	dropStmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse DROP TABLE failed: %v", err)
	}

	// Plan DROP TABLE
	dropPlan, err := plnr.Plan(dropStmt)
	if err != nil {
		t.Fatalf("Plan DROP TABLE failed: %v", err)
	}

	// Execute DROP TABLE
	result, err = exec.Execute(dropPlan, ctx)
	if err != nil {
		t.Fatalf("Execute DROP TABLE failed: %v", err)
	}

	// Check result
	row, err = result.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if row == nil {
		t.Fatal("Expected DROP TABLE result")
	}

	if len(row.Values) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(row.Values))
	}

	resultMsg, ok := row.Values[0].Data.(string)
	if !ok {
		t.Fatalf("Expected string result, got %T", row.Values[0].Data)
	}

	expected := "Table 'public.users' dropped"
	if resultMsg != expected {
		t.Fatalf("Expected result '%s', got '%s'", expected, resultMsg)
	}

	// Should be EOF on next call
	row, err = result.Next()
	if err != nil {
		t.Fatalf("Second Next failed: %v", err)
	}
	if row != nil {
		t.Fatal("Expected EOF on second Next")
	}

	result.Close()

	// Verify table no longer exists
	_, err = cat.GetTable("public", "users")
	if err == nil {
		t.Fatal("Table should not exist after drop")
	}
}

func TestDropTableIntegration_NonExistent(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewMemoryCatalog()
	storage := &mockStorageForDropTest{
		tables: make(map[int64][]*executor.Row),
	}
	exec := executor.NewBasicExecutor(cat, nil)
	exec.SetStorageBackend(storage)

	// Test DROP TABLE on non-existent table
	dropSQL := "DROP TABLE non_existent"

	// Parse DROP TABLE
	p := parser.NewParser(dropSQL)
	dropStmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse DROP TABLE failed: %v", err)
	}

	// Plan DROP TABLE
	plnr := planner.NewBasicPlannerWithCatalog(cat)
	dropPlan, err := plnr.Plan(dropStmt)
	if err != nil {
		t.Fatalf("Plan DROP TABLE failed: %v", err)
	}

	// Execute DROP TABLE - should fail
	ctx := &executor.ExecContext{
		Catalog: cat,
		Stats:   &executor.ExecStats{},
	}

	_, err = exec.Execute(dropPlan, ctx)
	if err == nil {
		t.Fatal("Expected error when dropping non-existent table")
	}

	if err.Error() != "failed to open operator: table 'public.non_existent' does not exist" {
		t.Fatalf("Expected error about non-existent table, got '%s'", err.Error())
	}
}

// mockStorageForDropTest implements StorageBackend for testing
type mockStorageForDropTest struct {
	tables map[int64][]*executor.Row
}

func (m *mockStorageForDropTest) CreateTable(table *catalog.Table) error {
	m.tables[table.ID] = []*executor.Row{}
	return nil
}

func (m *mockStorageForDropTest) DropTable(tableID int64) error {
	delete(m.tables, tableID)
	return nil
}

func (m *mockStorageForDropTest) InsertRow(tableID int64, row *executor.Row) (executor.RowID, error) {
	m.tables[tableID] = append(m.tables[tableID], row)
	return executor.RowID{PageID: 1, SlotID: uint16(len(m.tables[tableID]))}, nil
}

func (m *mockStorageForDropTest) UpdateRow(tableID int64, rowID executor.RowID, row *executor.Row) error {
	return nil
}

func (m *mockStorageForDropTest) DeleteRow(tableID int64, rowID executor.RowID) error {
	return nil
}

func (m *mockStorageForDropTest) ScanTable(tableID int64, snapshotTS int64) (executor.RowIterator, error) {
	rows, ok := m.tables[tableID]
	if !ok {
		return nil, nil //nolint:nilnil // No table found
	}
	return &mockRowIteratorForDropTest{rows: rows, pos: 0}, nil
}

func (m *mockStorageForDropTest) GetRow(tableID int64, rowID executor.RowID, snapshotTS int64) (*executor.Row, error) {
	return nil, nil //nolint:nilnil // Not implemented for test
}

func (m *mockStorageForDropTest) SetTransactionID(txnID uint64) {
	// Not implemented for this test
}

func (m *mockStorageForDropTest) GetBufferPoolStats() *storage.BufferPoolStats {
	return nil
}

// mockRowIteratorForDropTest implements RowIterator
type mockRowIteratorForDropTest struct {
	rows []*executor.Row
	pos  int
}

func (m *mockRowIteratorForDropTest) Next() (*executor.Row, executor.RowID, error) {
	if m.pos >= len(m.rows) {
		return nil, executor.RowID{}, nil // EOF
	}
	row := m.rows[m.pos]
	m.pos++
	rowID := executor.RowID{PageID: 1, SlotID: uint16(m.pos)}
	return row, rowID, nil
}

func (m *mockRowIteratorForDropTest) Close() error {
	return nil
}
