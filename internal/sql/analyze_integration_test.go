package sql

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// TestAnalyzeIntegration tests the full ANALYZE flow from parsing to execution
func TestAnalyzeIntegration(t *testing.T) {
	// Create a memory catalog
	cat := catalog.NewMemoryCatalog()

	// Create planner
	p := planner.NewBasicPlannerWithCatalog(cat)

	// Create a test table first
	createSQL := `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT UNIQUE,
		age INTEGER
	)`

	createParser := parser.NewParser(createSQL)
	createStmt, err := createParser.Parse()
	if err != nil {
		t.Fatalf("failed to parse CREATE TABLE: %v", err)
	}

	createPlan, err := p.Plan(createStmt)
	if err != nil {
		t.Fatalf("failed to plan CREATE TABLE: %v", err)
	}

	// Execute CREATE TABLE
	exec := executor.NewBasicExecutor(cat, nil)
	storage := newMockStorageForTest()
	exec.SetStorageBackend(storage)

	ctx := &executor.ExecContext{
		Catalog: cat,
		Stats:   &executor.ExecStats{},
	}

	result, err := exec.Execute(createPlan, ctx)
	if err != nil {
		t.Fatalf("failed to execute CREATE TABLE: %v", err)
	}
	result.Close()

	// Get the created table
	table, err := cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Insert some test data
	storage.tables[table.ID] = []*executor.Row{
		{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("alice@example.com"), types.NewValue(int64(25))}},
		{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("bob@example.com"), types.NewValue(int64(30))}},
		{Values: []types.Value{types.NewValue(int64(3)), types.NewValue("charlie@example.com"), types.NewNullValue()}}, // NULL age
		{Values: []types.Value{types.NewValue(int64(4)), types.NewValue("david@example.com"), types.NewValue(int64(35))}},
		{Values: []types.Value{types.NewValue(int64(5)), types.NewValue("eve@example.com"), types.NewValue(int64(28))}},
	}

	// Test 1: Parse and execute ANALYZE on entire table
	analyzeSQL := "ANALYZE users"
	analyzeParser := parser.NewParser(analyzeSQL)
	analyzeStmt, err := analyzeParser.Parse()
	if err != nil {
		t.Fatalf("failed to parse ANALYZE: %v", err)
	}

	analyzePlan, err := p.Plan(analyzeStmt)
	if err != nil {
		t.Fatalf("failed to plan ANALYZE: %v", err)
	}

	analyzeResult, err := exec.Execute(analyzePlan, ctx)
	if err != nil {
		t.Fatalf("failed to execute ANALYZE: %v", err)
	}

	// Read result
	row, err := analyzeResult.Next()
	if err != nil {
		t.Fatalf("failed to get ANALYZE result: %v", err)
	}
	if row == nil {
		t.Fatal("expected result from ANALYZE")
	}

	msg := row.Values[0].Data.(string)
	expected := "ANALYZE completed for table public.users"
	if msg != expected {
		t.Errorf("expected message %q, got %q", expected, msg)
	}

	analyzeResult.Close()

	// Verify statistics were collected
	table, err = cat.GetTable("public", "users")
	if err != nil {
		t.Fatalf("failed to get table after ANALYZE: %v", err)
	}

	if table.Stats == nil {
		t.Fatal("expected table statistics after ANALYZE")
	}

	if table.Stats.RowCount != 5 {
		t.Errorf("expected row count 5, got %d", table.Stats.RowCount)
	}

	// Check column statistics
	for _, col := range table.Columns {
		if col.Stats == nil {
			t.Errorf("expected statistics for column %s", col.Name)
			continue
		}

		switch col.Name {
		case "id":
			if col.Stats.DistinctCount != 5 {
				t.Errorf("expected 5 distinct values for id, got %d", col.Stats.DistinctCount)
			}
		case "email":
			if col.Stats.DistinctCount != 5 {
				t.Errorf("expected 5 distinct values for email, got %d", col.Stats.DistinctCount)
			}
		case "age":
			if col.Stats.NullCount != 1 {
				t.Errorf("expected 1 null for age, got %d", col.Stats.NullCount)
			}
			if col.Stats.DistinctCount != 4 {
				t.Errorf("expected 4 distinct non-null values for age, got %d", col.Stats.DistinctCount)
			}
		}
	}

	// Test 2: ANALYZE specific columns
	analyzeColsSQL := "ANALYZE users (id, age)"
	analyzeColsParser := parser.NewParser(analyzeColsSQL)
	analyzeColsStmt, err := analyzeColsParser.Parse()
	if err != nil {
		t.Fatalf("failed to parse ANALYZE with columns: %v", err)
	}

	analyzeColsPlan, err := p.Plan(analyzeColsStmt)
	if err != nil {
		t.Fatalf("failed to plan ANALYZE with columns: %v", err)
	}

	analyzeColsResult, err := exec.Execute(analyzeColsPlan, ctx)
	if err != nil {
		t.Fatalf("failed to execute ANALYZE with columns: %v", err)
	}

	row, err = analyzeColsResult.Next()
	if err != nil {
		t.Fatalf("failed to get ANALYZE result: %v", err)
	}
	if row == nil {
		t.Fatal("expected result from ANALYZE")
	}

	analyzeColsResult.Close()

	// Test 3: Verify query planner uses statistics
	// Create a query that should benefit from statistics
	selectSQL := "SELECT * FROM users WHERE age = 30"
	selectParser := parser.NewParser(selectSQL)
	selectStmt, err := selectParser.Parse()
	if err != nil {
		t.Fatalf("failed to parse SELECT: %v", err)
	}

	selectPlan, err := p.Plan(selectStmt)
	if err != nil {
		t.Fatalf("failed to plan SELECT: %v", err)
	}

	// The planner should use statistics to estimate selectivity
	// This is already tested in the planner tests, but we verify the flow works
	t.Logf("SELECT plan: %s", selectPlan.String())
}

// TestAnalyzeSyntaxVariations tests different ANALYZE syntax variations
func TestAnalyzeSyntaxVariations(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string // expected table name
		cols []string
	}{
		{
			name: "simple ANALYZE",
			sql:  "ANALYZE users",
			want: "users",
			cols: []string{},
		},
		{
			name: "ANALYZE with TABLE keyword",
			sql:  "ANALYZE TABLE users",
			want: "users",
			cols: []string{},
		},
		{
			name: "ANALYZE with column list",
			sql:  "ANALYZE users (id, email, age)",
			want: "users",
			cols: []string{"id", "email", "age"},
		},
		{
			name: "ANALYZE with semicolon",
			sql:  "ANALYZE users;",
			want: "users",
			cols: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			analyzeStmt, ok := stmt.(*parser.AnalyzeStmt)
			if !ok {
				t.Fatalf("expected AnalyzeStmt, got %T", stmt)
			}

			if analyzeStmt.TableName != tt.want {
				t.Errorf("expected table name %q, got %q", tt.want, analyzeStmt.TableName)
			}

			if len(analyzeStmt.Columns) != len(tt.cols) {
				t.Errorf("expected %d columns, got %d", len(tt.cols), len(analyzeStmt.Columns))
			}

			for i, col := range tt.cols {
				if i < len(analyzeStmt.Columns) && analyzeStmt.Columns[i] != col {
					t.Errorf("expected column[%d] = %q, got %q", i, col, analyzeStmt.Columns[i])
				}
			}
		})
	}
}

// mockStorageForTest is a simple storage backend for testing
type mockStorageForTest struct {
	tables map[int64][]*executor.Row
}

func newMockStorageForTest() *mockStorageForTest {
	return &mockStorageForTest{
		tables: make(map[int64][]*executor.Row),
	}
}

func (m *mockStorageForTest) CreateTable(table *catalog.Table) error {
	m.tables[table.ID] = []*executor.Row{}
	return nil
}

func (m *mockStorageForTest) DropTable(tableID int64) error {
	delete(m.tables, tableID)
	return nil
}

func (m *mockStorageForTest) InsertRow(tableID int64, row *executor.Row) (executor.RowID, error) {
	m.tables[tableID] = append(m.tables[tableID], row)
	return executor.RowID{PageID: 1, SlotID: uint16(len(m.tables[tableID]))}, nil
}

func (m *mockStorageForTest) UpdateRow(tableID int64, rowID executor.RowID, row *executor.Row) error {
	return nil
}

func (m *mockStorageForTest) DeleteRow(tableID int64, rowID executor.RowID) error {
	return nil
}

func (m *mockStorageForTest) ScanTable(tableID int64) (executor.RowIterator, error) {
	rows := m.tables[tableID]
	return &mockRowIteratorForTest{rows: rows, pos: 0}, nil
}

func (m *mockStorageForTest) GetRow(tableID int64, rowID executor.RowID) (*executor.Row, error) {
	return nil, nil //nolint:nilnil // Not implemented for test
}

func (m *mockStorageForTest) SetTransactionID(txnID uint64) {}

type mockRowIteratorForTest struct {
	rows []*executor.Row
	pos  int
}

func (m *mockRowIteratorForTest) Next() (*executor.Row, executor.RowID, error) {
	if m.pos >= len(m.rows) {
		return nil, executor.RowID{}, nil
	}
	row := m.rows[m.pos]
	rowID := executor.RowID{PageID: 1, SlotID: uint16(m.pos + 1)}
	m.pos++
	return row, rowID, nil
}

func (m *mockRowIteratorForTest) Close() error {
	return nil
}
