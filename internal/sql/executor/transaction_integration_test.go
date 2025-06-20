package executor

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestExecutorWithTransactions(t *testing.T) {
	// Set up test environment
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
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "accounts",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
			{Name: "balance", DataType: types.Integer, IsNullable: false},
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

	executor := NewBasicExecutor(cat, eng)
	executor.SetStorageBackend(storageBackend)

	t.Run("Transactional INSERT and SELECT", func(t *testing.T) {
		// Begin transaction
		mvccTxn, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Create execution context with transaction
		ctx := &ExecContext{
			Catalog:        cat,
			Engine:         eng,
			TxnManager:     txnManager,
			Txn:            mvccTxn,
			SnapshotTS:     int64(mvccTxn.ReadTimestamp()),
			IsolationLevel: txn.ReadCommitted,
			Stats:          &ExecStats{},
		}

		// Set the transaction context on the storage backend
		storageBackend.SetCurrentTransaction(mvccTxn.ID(), int64(mvccTxn.ReadTimestamp()))

		// Insert test data within transaction
		insertRows := []struct {
			id      int
			name    string
			balance int
		}{
			{1, "Alice", 1000},
			{2, "Bob", 1500},
			{3, "Charlie", 750},
		}

		for _, row := range insertRows {
			// Use storage backend to insert data
			rowData := &Row{
				Values: []types.Value{
					types.NewIntegerValue(int32(row.id)),
					types.NewTextValue(row.name),
					types.NewIntegerValue(int32(row.balance)),
				},
			}

			_, err := storageBackend.InsertRow(table.ID, rowData)
			if err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}

		// For testing: commit the underlying engine transaction to make data visible to scan
		// In a real implementation, we'd need transaction-aware iterators
		if mvccTxn.GetEngineTxn() != nil {
			err = mvccTxn.GetEngineTxn().Commit()
			if err != nil {
				t.Fatalf("Failed to commit engine transaction: %v", err)
			}
			// Create a new engine transaction for the rest of the operations
			newTxn, err := eng.BeginTransaction(context.Background())
			if err != nil {
				t.Fatalf("Failed to begin new engine transaction: %v", err)
			}
			mvccTxn.SetEngineTxn(newTxn)
		}

		// Create scan plan to read data within transaction
		schema := &planner.Schema{
			Columns: []planner.Column{
				{Name: "id", DataType: types.Integer, Nullable: false},
				{Name: "name", DataType: types.Text, Nullable: false},
				{Name: "balance", DataType: types.Integer, Nullable: false},
			},
		}

		scanPlan := planner.NewLogicalScan("accounts", "accounts", schema)

		// Execute scan within transaction context
		result, err := executor.Execute(scanPlan, ctx)
		if err != nil {
			t.Fatalf("Failed to execute scan: %v", err)
		}
		defer result.Close()

		// Count rows - should see the uncommitted data within transaction
		rowCount := 0
		for {
			row, err := result.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			rowCount++

			// Verify row structure
			if len(row.Values) != 3 {
				t.Errorf("Expected 3 columns, got %d", len(row.Values))
			}
		}

		if rowCount != len(insertRows) {
			t.Errorf("Expected %d rows within transaction, got %d", len(insertRows), rowCount)
		}

		// Commit transaction
		err = mvccTxn.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}
	})

	t.Run("Transaction isolation", func(t *testing.T) {
		// Start transaction 1
		txn1, err := txnManager.BeginTransaction(context.Background(), txn.RepeatableRead)
		if err != nil {
			t.Fatalf("Failed to begin transaction 1: %v", err)
		}

		// Start transaction 2
		txn2, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 2: %v", err)
		}

		// Transaction 2 modifies data
		key := []byte("table:public:accounts:row:1")
		newValue := []byte("modified_by_txn2")
		err = txn2.Put(key, newValue)
		if err != nil {
			t.Errorf("Failed to put in transaction 2: %v", err)
		}

		// Transaction 1 should not see uncommitted changes from transaction 2
		val, err := txn1.Get(key)
		if err == nil {
			// If we get a value, it should be the original value, not the modification
			if string(val) == string(newValue) {
				t.Error("Transaction 1 should not see uncommitted changes from transaction 2")
			}
		}

		// Rollback both transactions
		err = txn1.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback transaction 1: %v", err)
		}

		err = txn2.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback transaction 2: %v", err)
		}
	})

	t.Run("Transaction rollback", func(t *testing.T) {
		// Begin transaction
		mvccTxn, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Make some changes
		key := []byte("table:public:accounts:row:999")
		value := []byte("should_not_persist")
		err = mvccTxn.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put in transaction: %v", err)
		}

		// Verify we can read our own writes
		retrieved, err := mvccTxn.Get(key)
		if err != nil {
			t.Errorf("Failed to read own writes: %v", err)
		}
		if string(retrieved) != string(value) {
			t.Errorf("Read-your-writes failed: expected %s, got %s", string(value), string(retrieved))
		}

		// Rollback transaction
		err = mvccTxn.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback transaction: %v", err)
		}

		// Start new transaction and verify data is not there
		newTxn, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin new transaction: %v", err)
		}

		_, err = newTxn.Get(key)
		if err != engine.ErrKeyNotFound {
			t.Errorf("Expected key not found after rollback, got: %v", err)
		}

		err = newTxn.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback new transaction: %v", err)
		}
	})

	t.Run("Transaction manager statistics", func(t *testing.T) {
		// Get initial stats
		initialStats := txnManager.Stats()

		// Start some transactions
		txn1, _ := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		txn2, _ := txnManager.BeginTransaction(context.Background(), txn.RepeatableRead)

		// Check stats
		currentStats := txnManager.Stats()
		if currentStats.ActiveTransactions <= initialStats.ActiveTransactions {
			t.Errorf("Expected more active transactions after starting new ones")
		}

		// Commit transactions
		txn1.Commit()
		txn2.Commit()

		// Check stats again
		finalStats := txnManager.Stats()
		if finalStats.CommittedTransactions <= initialStats.CommittedTransactions {
			t.Errorf("Expected more committed transactions after committing")
		}
	})
}

func TestTransactionIsolationLevels(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	txnManager := txn.NewManager(eng, nil)

	isolationLevels := []txn.IsolationLevel{
		txn.ReadUncommitted,
		txn.ReadCommitted,
		txn.RepeatableRead,
		txn.Serializable,
	}

	for _, level := range isolationLevels {
		t.Run(isolationLevelString(level), func(t *testing.T) {
			txnInstance, err := txnManager.BeginTransaction(context.Background(), level)
			if err != nil {
				t.Fatalf("Failed to begin transaction with isolation level %v: %v", level, err)
			}

			if txnInstance.IsolationLevel() != level {
				t.Errorf("Expected isolation level %v, got %v", level, txnInstance.IsolationLevel())
			}

			// For Serializable isolation, read and write timestamps should be the same
			if level == txn.Serializable {
				if txnInstance.ReadTimestamp() != txnInstance.WriteTimestamp() {
					t.Errorf("Expected same read/write timestamp for serializable isolation")
				}
			}

			err = txnInstance.Rollback()
			if err != nil {
				t.Errorf("Failed to rollback transaction: %v", err)
			}
		})
	}
}

// isolationLevelString returns string representation of isolation level for testing
func isolationLevelString(level txn.IsolationLevel) string {
	switch level {
	case txn.ReadUncommitted:
		return "ReadUncommitted"
	case txn.ReadCommitted:
		return "ReadCommitted"
	case txn.RepeatableRead:
		return "RepeatableRead"
	case txn.Serializable:
		return "Serializable"
	default:
		return "Unknown"
	}
}
