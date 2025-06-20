package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

// TestTransactionContextPropagation verifies that transaction context is properly
// propagated through all operators
func TestTransactionContextPropagation(t *testing.T) {
	// Create test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	txnManager := txn.NewManager(eng, nil)

	// Create storage backend
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test schema
	if err := cat.CreateSchema("test"); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create a test table
	schema := &catalog.TableSchema{
		SchemaName: "test",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Begin a transaction
	mvccTxn, err := txnManager.BeginTransaction(nil, txn.ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer mvccTxn.Rollback()

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

	// Test 1: InsertOperator propagates transaction ID
	t.Run("InsertOperator", func(t *testing.T) {
		// Create a mock storage backend that tracks SetTransactionID calls
		mockStorage := &mockTxnStorageBackend{
			StorageBackend: storageBackend,
			txnIDCalls:     []uint64{},
		}

		insertOp := NewInsertOperator(table, mockStorage, nil)
		insertOp.values = [][]parser.Expression{
			{
				&parser.Literal{Value: types.Value{Data: int32(1)}},
				&parser.Literal{Value: types.Value{Data: "Alice"}},
			},
		}

		if err := insertOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open insert operator: %v", err)
		}

		// Verify transaction ID was set
		if len(mockStorage.txnIDCalls) != 1 {
			t.Errorf("Expected 1 SetTransactionID call, got %d", len(mockStorage.txnIDCalls))
		}
		if mockStorage.txnIDCalls[0] != uint64(mvccTxn.ID()) {
			t.Errorf("Expected transaction ID %d, got %d", mvccTxn.ID(), mockStorage.txnIDCalls[0])
		}
	})

	// Test 2: StorageScanOperator propagates transaction ID
	t.Run("StorageScanOperator", func(t *testing.T) {
		mockStorage := &mockTxnStorageBackend{
			StorageBackend: storageBackend,
			txnIDCalls:     []uint64{},
		}

		scanOp := NewStorageScanOperator(table, mockStorage)
		if err := scanOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open scan operator: %v", err)
		}

		// Verify transaction ID was set
		if len(mockStorage.txnIDCalls) != 1 {
			t.Errorf("Expected 1 SetTransactionID call, got %d", len(mockStorage.txnIDCalls))
		}
		if mockStorage.txnIDCalls[0] != uint64(mvccTxn.ID()) {
			t.Errorf("Expected transaction ID %d, got %d", mvccTxn.ID(), mockStorage.txnIDCalls[0])
		}
	})

	// Test 3: UpdateOperator propagates transaction ID
	t.Run("UpdateOperator", func(t *testing.T) {
		mockStorage := &mockTxnStorageBackend{
			StorageBackend: storageBackend,
			txnIDCalls:     []uint64{},
		}

		updateOp := NewUpdateOperator(table, mockStorage, nil, nil)
		if err := updateOp.Open(ctx); err != nil {
			// Update will fail because no rows exist, but that's ok
			// We're just testing that SetTransactionID was called
		}

		// Verify transaction ID was set
		if len(mockStorage.txnIDCalls) != 1 {
			t.Errorf("Expected 1 SetTransactionID call, got %d", len(mockStorage.txnIDCalls))
		}
		if mockStorage.txnIDCalls[0] != uint64(mvccTxn.ID()) {
			t.Errorf("Expected transaction ID %d, got %d", mvccTxn.ID(), mockStorage.txnIDCalls[0])
		}
	})

	// Test 4: DeleteOperator propagates transaction ID
	t.Run("DeleteOperator", func(t *testing.T) {
		mockStorage := &mockTxnStorageBackend{
			StorageBackend: storageBackend,
			txnIDCalls:     []uint64{},
		}

		deleteOp := NewDeleteOperator(table, mockStorage, nil)
		if err := deleteOp.Open(ctx); err != nil {
			// Delete will fail because no rows exist, but that's ok
			// We're just testing that SetTransactionID was called
		}

		// Verify transaction ID was set
		if len(mockStorage.txnIDCalls) != 1 {
			t.Errorf("Expected 1 SetTransactionID call, got %d", len(mockStorage.txnIDCalls))
		}
		if mockStorage.txnIDCalls[0] != uint64(mvccTxn.ID()) {
			t.Errorf("Expected transaction ID %d, got %d", mvccTxn.ID(), mockStorage.txnIDCalls[0])
		}
	})

	// Test 5: Non-transactional context doesn't set transaction ID
	t.Run("NonTransactionalContext", func(t *testing.T) {
		mockStorage := &mockTxnStorageBackend{
			StorageBackend: storageBackend,
			txnIDCalls:     []uint64{},
		}

		// Create context without transaction
		nonTxnCtx := &ExecContext{
			Catalog:    cat,
			Engine:     eng,
			TxnManager: txnManager,
			Txn:        nil, // No transaction
			Stats:      &ExecStats{},
		}

		scanOp := NewStorageScanOperator(table, mockStorage)
		if err := scanOp.Open(nonTxnCtx); err != nil {
			t.Fatalf("Failed to open scan operator: %v", err)
		}

		// Verify transaction ID was NOT set
		if len(mockStorage.txnIDCalls) != 0 {
			t.Errorf("Expected 0 SetTransactionID calls for non-transactional context, got %d", len(mockStorage.txnIDCalls))
		}
	})
}

// mockTxnStorageBackend wraps a StorageBackend and tracks SetTransactionID calls
type mockTxnStorageBackend struct {
	StorageBackend
	txnIDCalls []uint64
}

func (m *mockTxnStorageBackend) SetTransactionID(txnID uint64) {
	m.txnIDCalls = append(m.txnIDCalls, txnID)
	m.StorageBackend.SetTransactionID(txnID)
}

// TestExecContextFields verifies that ExecContext properly stores transaction metadata
func TestExecContextFields(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()
	txnManager := txn.NewManager(eng, nil)

	// Create a transaction
	mvccTxn, err := txnManager.BeginTransaction(nil, txn.RepeatableRead)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer mvccTxn.Rollback()

	// Create execution context
	ctx := &ExecContext{
		Catalog:        catalog.NewMemoryCatalog(),
		Engine:         eng,
		TxnManager:     txnManager,
		Txn:            mvccTxn,
		SnapshotTS:     int64(mvccTxn.ReadTimestamp()),
		IsolationLevel: txn.RepeatableRead,
		Stats:          &ExecStats{},
	}

	// Verify fields
	if ctx.Txn != mvccTxn {
		t.Error("Transaction not properly stored in ExecContext")
	}
	if ctx.SnapshotTS != int64(mvccTxn.ReadTimestamp()) {
		t.Errorf("SnapshotTS mismatch: expected %d, got %d", mvccTxn.ReadTimestamp(), ctx.SnapshotTS)
	}
	if ctx.IsolationLevel != txn.RepeatableRead {
		t.Errorf("IsolationLevel mismatch: expected RepeatableRead, got %v", ctx.IsolationLevel)
	}
}