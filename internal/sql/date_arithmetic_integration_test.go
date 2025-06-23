package sql

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestDateArithmeticIntegration(t *testing.T) {
	// Set up catalog and storage
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Create transaction manager and storage backend
	txnManager := txn.NewManager(eng, nil)
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := executor.NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create a test table with date and timestamp columns
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "events",
		Columns: []catalog.ColumnDef{
			{
				Name:       "id",
				DataType:   types.Integer,
				IsNullable: false,
			},
			{
				Name:       "event_date",
				DataType:   types.Date,
				IsNullable: false,
			},
			{
				Name:       "event_time",
				DataType:   types.Timestamp,
				IsNullable: false,
			},
			{
				Name:       "name",
				DataType:   types.Varchar(100),
				IsNullable: false,
			},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create table in storage backend
	err = storageBackend.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table in storage: %v", err)
	}

	// Insert test data
	testData := []struct {
		id        int
		eventDate time.Time
		eventTime time.Time
		name      string
	}{
		{1, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC), "New Year"},
		{2, time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, 2, 15, 14, 45, 0, 0, time.UTC), "Valentine"},
		{3, time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC), time.Date(2024, 12, 25, 8, 0, 0, 0, time.UTC), "Christmas"},
	}

	// Insert test data using the storage backend
	for _, testRow := range testData {
		// Create row for storage backend
		row := &executor.Row{
			Values: []types.Value{
				types.NewIntegerValue(int32(testRow.id)),
				types.NewDateValue(testRow.eventDate),
				types.NewTimestampValue(testRow.eventTime),
				types.NewTextValue(testRow.name),
			},
		}

		// Insert the row
		_, err = storageBackend.InsertRow(table.ID, row)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Define test queries
	queries := []struct {
		name     string
		query    string
		expected int // expected number of rows
		validate func(t *testing.T, rows []*executor.Row)
	}{
		{
			name:     "Date + Interval",
			query:    "SELECT id, event_date + INTERVAL '5 days' AS future_date FROM events WHERE id = 1",
			expected: 1,
			validate: func(t *testing.T, rows []*executor.Row) {
				if len(rows) != 1 {
					return
				}
				expectedDate := time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC)
				actualDate := rows[0].Values[1].Data.(time.Time)
				if !actualDate.Equal(expectedDate) {
					t.Errorf("expected %v, got %v", expectedDate, actualDate)
				}
			},
		},
		{
			name:     "Timestamp + Interval",
			query:    "SELECT id, event_time + INTERVAL '2 hours' AS future_time FROM events WHERE id = 1",
			expected: 1,
			validate: func(t *testing.T, rows []*executor.Row) {
				if len(rows) != 1 {
					return
				}
				expectedTime := time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)
				actualTime := rows[0].Values[1].Data.(time.Time)
				if !actualTime.Equal(expectedTime) {
					t.Errorf("expected %v, got %v", expectedTime, actualTime)
				}
			},
		},
		{
			name:     "Date comparison with interval",
			query:    "SELECT id, name FROM events WHERE event_date > DATE '2024-01-01' + INTERVAL '30 days'",
			expected: 2,
		},
		{
			name:     "Interval multiplication",
			query:    "SELECT INTERVAL '1 day' * 3 AS result",
			expected: 1,
			validate: func(t *testing.T, rows []*executor.Row) {
				if len(rows) != 1 {
					return
				}
				interval := rows[0].Values[0].Data.(types.Interval)
				if interval.Days != 3 || interval.Months != 0 || interval.Duration != 0 {
					t.Errorf("expected 3 days, got %v", interval)
				}
			},
		},
	}

	// Execute and test each query
	for _, test := range queries {
		t.Run(test.name, func(t *testing.T) {
			// Parse
			p := parser.NewParser(test.query)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			// Plan
			pl := planner.NewBasicPlannerWithCatalog(cat)
			plan, err := pl.Plan(stmt)
			if err != nil {
				t.Fatalf("Failed to plan query: %v", err)
			}

			// Execute
			exec := executor.NewBasicExecutor(cat, nil)
			exec.SetStorageBackend(storageBackend)

			// Create execution context
			execCtx := &executor.ExecContext{
				Catalog: cat,
			}

			// Execute query
			result, err := exec.Execute(plan, execCtx)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Convert result to rows
			var rows []*executor.Row
			for {
				row, err := result.Next()
				if err != nil {
					t.Fatalf("Error getting next row: %v", err)
				}
				if row == nil {
					break
				}
				rows = append(rows, row)
			}
			result.Close()

			// Check row count
			if len(rows) != test.expected {
				t.Errorf("Expected %d rows, got %d", test.expected, len(rows))
			}

			// Run custom validation if provided
			if test.validate != nil {
				test.validate(t, rows)
			}
		})
	}
}
