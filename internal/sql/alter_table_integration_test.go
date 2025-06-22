package sql

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
)

func TestAlterTableIntegration(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewMemoryCatalog()
	storage := &mockStorageForAlterTest{
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

	// Verify table exists with 2 columns
	table, err := cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("Table should exist after creation: %v", err)
	}

	if len(table.Columns) != 2 {
		t.Fatalf("Expected 2 columns initially, got %d", len(table.Columns))
	}

	// Test ALTER TABLE ADD COLUMN
	addSQL := "ALTER TABLE users ADD COLUMN email VARCHAR(100) NOT NULL"

	// Parse ALTER TABLE ADD COLUMN
	p = parser.NewParser(addSQL)
	addStmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse ALTER TABLE ADD failed: %v", err)
	}

	// Plan ALTER TABLE ADD COLUMN
	addPlan, err := plnr.Plan(addStmt)
	if err != nil {
		t.Fatalf("Plan ALTER TABLE ADD failed: %v", err)
	}

	// Execute ALTER TABLE ADD COLUMN
	result, err = exec.Execute(addPlan, ctx)
	if err != nil {
		t.Fatalf("Execute ALTER TABLE ADD failed: %v", err)
	}

	// Check result
	row, err = result.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if row == nil {
		t.Fatal("Expected ALTER TABLE ADD result")
	}

	if len(row.Values) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(row.Values))
	}

	resultMsg, ok := row.Values[0].Data.(string)
	if !ok {
		t.Fatalf("Expected string result, got %T", row.Values[0].Data)
	}

	expected := "Column 'email' added to table 'public.users'"
	if resultMsg != expected {
		t.Fatalf("Expected result '%s', got '%s'", expected, resultMsg)
	}

	result.Close()

	// Verify column was added
	table, err = cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("Failed to get table after ADD COLUMN: %v", err)
	}

	if len(table.Columns) != 3 {
		t.Fatalf("Expected 3 columns after ADD, got %d", len(table.Columns))
	}

	emailColumn := table.Columns[2]
	if emailColumn.Name != "email" {
		t.Errorf("Expected email column, got %s", emailColumn.Name)
	}

	if emailColumn.IsNullable {
		t.Error("Expected NOT NULL column")
	}

	// Test ALTER TABLE DROP COLUMN
	dropSQL := "ALTER TABLE users DROP COLUMN email"

	// Parse ALTER TABLE DROP COLUMN
	p = parser.NewParser(dropSQL)
	dropStmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse ALTER TABLE DROP failed: %v", err)
	}

	// Plan ALTER TABLE DROP COLUMN
	dropPlan, err := plnr.Plan(dropStmt)
	if err != nil {
		t.Fatalf("Plan ALTER TABLE DROP failed: %v", err)
	}

	// Execute ALTER TABLE DROP COLUMN
	result, err = exec.Execute(dropPlan, ctx)
	if err != nil {
		t.Fatalf("Execute ALTER TABLE DROP failed: %v", err)
	}

	// Check result
	row, err = result.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if row == nil {
		t.Fatal("Expected ALTER TABLE DROP result")
	}

	resultMsg, ok = row.Values[0].Data.(string)
	if !ok {
		t.Fatalf("Expected string result, got %T", row.Values[0].Data)
	}

	expected = "Column 'email' dropped from table 'public.users'"
	if resultMsg != expected {
		t.Fatalf("Expected result '%s', got '%s'", expected, resultMsg)
	}

	result.Close()

	// Verify column was removed
	table, err = cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("Failed to get table after DROP COLUMN: %v", err)
	}

	if len(table.Columns) != 2 {
		t.Fatalf("Expected 2 columns after DROP, got %d", len(table.Columns))
	}

	// Verify email column is gone
	for _, col := range table.Columns {
		if col.Name == "email" {
			t.Error("Email column should have been dropped")
		}
	}
}

func TestAlterTableIntegration_Errors(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewMemoryCatalog()
	storage := &mockStorageForAlterTest{
		tables: make(map[int64][]*executor.Row),
	}
	exec := executor.NewBasicExecutor(cat, nil)
	exec.SetStorageBackend(storage)

	// Test ALTER TABLE on non-existent table
	alterSQL := "ALTER TABLE non_existent ADD COLUMN email VARCHAR(100)"

	// Parse ALTER TABLE
	p := parser.NewParser(alterSQL)
	alterStmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse ALTER TABLE failed: %v", err)
	}

	// Plan ALTER TABLE
	plnr := planner.NewBasicPlannerWithCatalog(cat)
	alterPlan, err := plnr.Plan(alterStmt)
	if err != nil {
		t.Fatalf("Plan ALTER TABLE failed: %v", err)
	}

	// Execute ALTER TABLE - should fail
	ctx := &executor.ExecContext{
		Catalog: cat,
		Stats:   &executor.ExecStats{},
	}

	_, err = exec.Execute(alterPlan, ctx)
	if err == nil {
		t.Fatal("Expected error when altering non-existent table")
	}

	if err.Error() != "failed to open operator: table 'public.non_existent' does not exist" {
		t.Fatalf("Expected error about non-existent table, got '%s'", err.Error())
	}
}

// mockStorageForAlterTest implements StorageBackend for testing
type mockStorageForAlterTest struct {
	tables map[int64][]*executor.Row
}

func (m *mockStorageForAlterTest) CreateTable(table *catalog.Table) error {
	m.tables[table.ID] = []*executor.Row{}
	return nil
}

func (m *mockStorageForAlterTest) DropTable(tableID int64) error {
	delete(m.tables, tableID)
	return nil
}

func (m *mockStorageForAlterTest) InsertRow(tableID int64, row *executor.Row) (executor.RowID, error) {
	m.tables[tableID] = append(m.tables[tableID], row)
	return executor.RowID{PageID: 1, SlotID: uint16(len(m.tables[tableID]))}, nil
}

func (m *mockStorageForAlterTest) UpdateRow(tableID int64, rowID executor.RowID, row *executor.Row) error {
	return nil
}

func (m *mockStorageForAlterTest) DeleteRow(tableID int64, rowID executor.RowID) error {
	return nil
}

func (m *mockStorageForAlterTest) ScanTable(tableID int64, snapshotTS int64) (executor.RowIterator, error) {
	rows, ok := m.tables[tableID]
	if !ok {
		return nil, nil //nolint:nilnil // No table found
	}
	return &mockRowIteratorForAlterTest{rows: rows, pos: 0}, nil
}

func (m *mockStorageForAlterTest) GetRow(tableID int64, rowID executor.RowID, snapshotTS int64) (*executor.Row, error) {
	return nil, nil //nolint:nilnil // Not implemented for test
}

func (m *mockStorageForAlterTest) SetTransactionID(txnID uint64) {
	// Not implemented for this test
}

// mockRowIteratorForAlterTest implements RowIterator
type mockRowIteratorForAlterTest struct {
	rows []*executor.Row
	pos  int
}

func (m *mockRowIteratorForAlterTest) Next() (*executor.Row, executor.RowID, error) {
	if m.pos >= len(m.rows) {
		return nil, executor.RowID{}, nil // EOF
	}
	row := m.rows[m.pos]
	m.pos++
	rowID := executor.RowID{PageID: 1, SlotID: uint16(m.pos)}
	return row, rowID, nil
}

func (m *mockRowIteratorForAlterTest) Close() error {
	return nil
}
