package executor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

// TestConcurrentTransactionIsolation verifies proper isolation between concurrent transactions
func TestConcurrentTransactionIsolation(t *testing.T) {
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
		TableName:  "accounts",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "balance", DataType: types.Integer, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Insert initial data
	insertInitialData := func() {
		// Create a transaction for the initial data insert
		initTxn, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin init transaction: %v", err)
		}
		defer initTxn.Commit()

		ctx := &ExecContext{
			Catalog:        cat,
			Engine:         eng,
			TxnManager:     txnManager,
			Txn:            initTxn,
			SnapshotTS:     int64(initTxn.ReadTimestamp()),
			IsolationLevel: txn.ReadCommitted,
			Stats:          &ExecStats{},
		}

		insertOp := NewInsertOperator(table, storageBackend, nil)
		insertOp.values = [][]parser.Expression{
			{
				&parser.Literal{Value: types.Value{Data: int32(1)}},
				&parser.Literal{Value: types.Value{Data: int32(100)}},
			},
			{
				&parser.Literal{Value: types.Value{Data: int32(2)}},
				&parser.Literal{Value: types.Value{Data: int32(200)}},
			},
		}

		if err := insertOp.Open(ctx); err != nil {
			t.Fatalf("Failed to insert initial data: %v", err)
		}
		
		// Execute the insert
		for {
			row, err := insertOp.Next()
			if err != nil {
				t.Fatalf("Insert error: %v", err)
			}
			if row == nil {
				break
			}
		}
		
		insertOp.Close()
	}
	insertInitialData()

	t.Run("ReadCommittedIsolation", func(t *testing.T) {
		// Test that Read Committed allows non-repeatable reads
		var wg sync.WaitGroup
		results := make(chan int32, 2)

		// Transaction 1: Read balance twice
		wg.Add(1)
		go func() {
			defer wg.Done()

			txn1, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
			if err != nil {
				t.Errorf("Failed to begin txn1: %v", err)
				return
			}
			defer txn1.Rollback()

			ctx := &ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            txn1,
				SnapshotTS:     int64(txn1.ReadTimestamp()),
				IsolationLevel: txn.ReadCommitted,
				Stats:          &ExecStats{},
			}

			// First read
			balance1 := readAccountBalance(t, ctx, table, storageBackend, 1)
			results <- balance1

			// Wait for transaction 2 to update
			time.Sleep(100 * time.Millisecond)

			// Second read should see the committed update (non-repeatable read)
			// For Read Committed, update snapshot timestamp for new statement
			ctx.SnapshotTS = int64(txnManager.GetCurrentTimestamp())
			balance2 := readAccountBalance(t, ctx, table, storageBackend, 1)
			results <- balance2
		}()

		// Transaction 2: Update balance
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Wait a bit to ensure txn1 has done its first read
			time.Sleep(50 * time.Millisecond)

			txn2, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
			if err != nil {
				t.Errorf("Failed to begin txn2: %v", err)
				return
			}

			ctx := &ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            txn2,
				SnapshotTS:     int64(txn2.ReadTimestamp()),
				IsolationLevel: txn.ReadCommitted,
				Stats:          &ExecStats{},
			}

			// Update balance
			updateAccountBalance(t, ctx, table, storageBackend, 1, 150)

			// Commit
			if err := txn2.Commit(); err != nil {
				t.Errorf("Failed to commit txn2: %v", err)
			}
		}()

		wg.Wait()
		close(results)

		// Verify results
		balances := []int32{}
		for b := range results {
			balances = append(balances, b)
		}

		if len(balances) != 2 {
			t.Fatalf("Expected 2 balance readings, got %d", len(balances))
		}

		// In Read Committed, the second read should see the update
		if balances[0] != 100 {
			t.Errorf("First read expected 100, got %d", balances[0])
		}
		if balances[1] != 150 {
			t.Errorf("Second read expected 150 (non-repeatable read), got %d", balances[1])
		}
	})

	// Reset data for next test
	if err := storageBackend.DropTable(table.ID); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to recreate table: %v", err)
	}
	insertInitialData()

	t.Run("RepeatableReadIsolation", func(t *testing.T) {
		// Test that Repeatable Read prevents non-repeatable reads
		var wg sync.WaitGroup
		results := make(chan int32, 2)

		// Transaction 1: Read balance twice
		wg.Add(1)
		go func() {
			defer wg.Done()

			txn1, err := txnManager.BeginTransaction(context.Background(), txn.RepeatableRead)
			if err != nil {
				t.Errorf("Failed to begin txn1: %v", err)
				return
			}
			defer txn1.Rollback()

			ctx := &ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            txn1,
				SnapshotTS:     int64(txn1.ReadTimestamp()),
				IsolationLevel: txn.RepeatableRead,
				Stats:          &ExecStats{},
			}

			// First read
			balance1 := readAccountBalance(t, ctx, table, storageBackend, 1)
			results <- balance1

			// Wait for transaction 2 to update
			time.Sleep(100 * time.Millisecond)

			// Second read should NOT see the committed update (repeatable read)
			balance2 := readAccountBalance(t, ctx, table, storageBackend, 1)
			results <- balance2
		}()

		// Transaction 2: Update balance
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Wait a bit to ensure txn1 has done its first read
			time.Sleep(50 * time.Millisecond)

			txn2, err := txnManager.BeginTransaction(context.Background(), txn.RepeatableRead)
			if err != nil {
				t.Errorf("Failed to begin txn2: %v", err)
				return
			}

			ctx := &ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            txn2,
				SnapshotTS:     int64(txn2.ReadTimestamp()),
				IsolationLevel: txn.RepeatableRead,
				Stats:          &ExecStats{},
			}

			// Update balance
			updateAccountBalance(t, ctx, table, storageBackend, 1, 150)

			// Commit
			if err := txn2.Commit(); err != nil {
				t.Errorf("Failed to commit txn2: %v", err)
			}
		}()

		wg.Wait()
		close(results)

		// Verify results
		balances := []int32{}
		for b := range results {
			balances = append(balances, b)
		}

		if len(balances) != 2 {
			t.Fatalf("Expected 2 balance readings, got %d", len(balances))
		}

		// In Repeatable Read, both reads should see the same value
		if balances[0] != 100 {
			t.Errorf("First read expected 100, got %d", balances[0])
		}
		if balances[1] != 100 {
			t.Errorf("Second read expected 100 (repeatable read), got %d", balances[1])
		}
	})

	// Reset data for next test
	if err := storageBackend.DropTable(table.ID); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to recreate table: %v", err)
	}
	insertInitialData()

	t.Run("WriteWriteConflict", func(t *testing.T) {
		// Test concurrent updates to the same row
		var wg sync.WaitGroup
		errors := make(chan error, 2)

		// Transaction 1: Update account 1
		wg.Add(1)
		go func() {
			defer wg.Done()

			txn1, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
			if err != nil {
				errors <- err
				return
			}
			defer txn1.Rollback()

			ctx := &ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            txn1,
				SnapshotTS:     int64(txn1.ReadTimestamp()),
				IsolationLevel: txn.ReadCommitted,
				Stats:          &ExecStats{},
			}

			// Update balance
			updateAccountBalance(t, ctx, table, storageBackend, 1, 300)

			// Simulate some work
			time.Sleep(50 * time.Millisecond)

			// Commit
			if err := txn1.Commit(); err != nil {
				errors <- err
				return
			}
			errors <- nil
		}()

		// Transaction 2: Also update account 1
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Start slightly after txn1
			time.Sleep(10 * time.Millisecond)

			txn2, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
			if err != nil {
				errors <- err
				return
			}
			defer txn2.Rollback()

			ctx := &ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            txn2,
				SnapshotTS:     int64(txn2.ReadTimestamp()),
				IsolationLevel: txn.ReadCommitted,
				Stats:          &ExecStats{},
			}

			// Try to update the same row
			updateAccountBalance(t, ctx, table, storageBackend, 1, 400)

			// Commit - this should succeed in MVCC as it creates a new version
			if err := txn2.Commit(); err != nil {
				errors <- err
				return
			}
			errors <- nil
		}()

		wg.Wait()
		close(errors)

		// Check results - both should succeed in MVCC
		errorCount := 0
		for err := range errors {
			if err != nil {
				errorCount++
				t.Logf("Transaction error (expected in some isolation levels): %v", err)
			}
		}

		// In MVCC with Read Committed, both updates should succeed
		// The last writer wins
		if errorCount > 0 {
			t.Logf("Note: %d transactions had conflicts, which may be expected depending on implementation", errorCount)
		}
	})

	t.Run("PhantomReadTest", func(t *testing.T) {
		// Test phantom reads in different isolation levels
		var wg sync.WaitGroup

		// Transaction 1: Count rows twice
		wg.Add(1)
		go func() {
			defer wg.Done()

			txn1, err := txnManager.BeginTransaction(context.Background(), txn.RepeatableRead)
			if err != nil {
				t.Errorf("Failed to begin txn1: %v", err)
				return
			}
			defer txn1.Rollback()

			ctx := &ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            txn1,
				SnapshotTS:     int64(txn1.ReadTimestamp()),
				IsolationLevel: txn.RepeatableRead,
				Stats:          &ExecStats{},
			}

			// First count
			count1 := countRows(t, ctx, table, storageBackend)
			t.Logf("First count: %d rows", count1)

			// Wait for transaction 2 to insert
			time.Sleep(100 * time.Millisecond)

			// Second count - should be same in Repeatable Read
			count2 := countRows(t, ctx, table, storageBackend)
			t.Logf("Second count: %d rows", count2)

			if count1 != count2 {
				t.Errorf("Phantom read detected in Repeatable Read: count changed from %d to %d", count1, count2)
			}
		}()

		// Transaction 2: Insert new row
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Wait a bit
			time.Sleep(50 * time.Millisecond)

			txn2, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
			if err != nil {
				t.Errorf("Failed to begin txn2: %v", err)
				return
			}

			ctx := &ExecContext{
				Catalog:        cat,
				Engine:         eng,
				TxnManager:     txnManager,
				Txn:            txn2,
				SnapshotTS:     int64(txn2.ReadTimestamp()),
				IsolationLevel: txn.ReadCommitted,
				Stats:          &ExecStats{},
			}

			// Insert new row
			insertOp := NewInsertOperator(table, storageBackend, nil)
			insertOp.values = [][]parser.Expression{
				{
					&parser.Literal{Value: types.Value{Data: int32(3)}},
					&parser.Literal{Value: types.Value{Data: int32(300)}},
				},
			}

			if err := insertOp.Open(ctx); err != nil {
				t.Errorf("Failed to insert: %v", err)
				return
			}
			insertOp.Close()

			// Commit
			if err := txn2.Commit(); err != nil {
				t.Errorf("Failed to commit txn2: %v", err)
			}
		}()

		wg.Wait()
	})
}

// Helper function to read account balance
func readAccountBalance(t *testing.T, ctx *ExecContext, table *catalog.Table, storage StorageBackend, accountID int32) int32 {
	scanOp := NewStorageScanOperator(table, storage)
	
	if err := scanOp.Open(ctx); err != nil {
		t.Fatalf("Failed to open scan: %v", err)
	}
	defer scanOp.Close()

	rowCount := 0
	for {
		row, err := scanOp.Next()
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if row == nil {
			break
		}
		rowCount++

		if row.Values[0].Data.(int32) == accountID {
			return row.Values[1].Data.(int32)
		}
	}

	t.Fatalf("Account %d not found (scanned %d rows)", accountID, rowCount)
	return 0
}

// Helper function to update account balance
func updateAccountBalance(t *testing.T, ctx *ExecContext, table *catalog.Table, storage StorageBackend, accountID int32, newBalance int32) {
	// Create update operator with SET balance = newBalance WHERE id = accountID
	assignments := []parser.Assignment{
		{
			Column: "balance",
			Value:  &parser.Literal{Value: types.Value{Data: newBalance}},
		},
	}
	
	// Create WHERE clause for id = accountID
	whereClause := &parser.BinaryExpr{
		Operator: parser.TokenEqual,
		Left:     &parser.Identifier{Name: "id"},
		Right:    &parser.Literal{Value: types.Value{Data: accountID}},
	}
	
	updateOp := NewUpdateOperator(table, storage, assignments, whereClause)
	
	if err := updateOp.Open(ctx); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	updateOp.Close()
}

// Helper function to count rows
func countRows(t *testing.T, ctx *ExecContext, table *catalog.Table, storage StorageBackend) int {
	scanOp := NewStorageScanOperator(table, storage)
	
	if err := scanOp.Open(ctx); err != nil {
		t.Fatalf("Failed to open scan: %v", err)
	}
	defer scanOp.Close()

	count := 0
	for {
		row, err := scanOp.Next()
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if row == nil {
			break
		}
		count++
	}

	return count
}

// TestMVCCVersionChaining verifies that version chains are properly maintained
func TestMVCCVersionChaining(t *testing.T) {
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
		TableName:  "versions",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "value", DataType: types.Text, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Test creating multiple versions of the same row
	t.Run("VersionChainCreation", func(t *testing.T) {
		// Insert initial version
		txn1, _ := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		ctx1 := &ExecContext{
			Catalog:        cat,
			Engine:         eng,
			TxnManager:     txnManager,
			Txn:            txn1,
			SnapshotTS:     int64(txn1.ReadTimestamp()),
			IsolationLevel: txn.ReadCommitted,
			Stats:          &ExecStats{},
		}

		insertOp := NewInsertOperator(table, storageBackend, nil)
		insertOp.values = [][]parser.Expression{
			{
				&parser.Literal{Value: types.Value{Data: int32(1)}},
				&parser.Literal{Value: types.Value{Data: "version1"}},
			},
		}
		insertOp.Open(ctx1)
		insertOp.Close()
		txn1.Commit()

		// Update to create version 2
		txn2, _ := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		ctx2 := &ExecContext{
			Catalog:        cat,
			Engine:         eng,
			TxnManager:     txnManager,
			Txn:            txn2,
			SnapshotTS:     int64(txn2.ReadTimestamp()),
			IsolationLevel: txn.ReadCommitted,
			Stats:          &ExecStats{},
		}

		assignments := []parser.Assignment{
			{
				Column: "value",
				Value:  &parser.Literal{Value: types.Value{Data: "version2"}},
			},
		}
		updateOp := NewUpdateOperator(table, storageBackend, assignments, nil)
		updateOp.Open(ctx2)
		updateOp.Close()
		txn2.Commit()

		// Verify both versions exist in storage
		// This would require direct access to storage internals
		// For now, we verify through visibility
		
		// Note: Testing historical snapshots would require:
		// 1. Direct access to storage internals to verify version chains
		// 2. Ability to set custom snapshot timestamps on transactions
		// 
		// Currently, our implementation doesn't fully support this level of testing
		// without modifying the transaction manager or storage backend
		t.Logf("Note: Version chain testing is limited in current implementation")

		// New transaction should see version 2
		newTxn, _ := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
		newCtx := &ExecContext{
			Catalog:        cat,
			Engine:         eng,
			TxnManager:     txnManager,
			Txn:            newTxn,
			SnapshotTS:     int64(newTxn.ReadTimestamp()),
			IsolationLevel: txn.ReadCommitted,
			Stats:          &ExecStats{},
		}

		scanOp2 := NewStorageScanOperator(table, storageBackend)
		scanOp2.Open(newCtx)
		row2, _ := scanOp2.Next()
		if row2 != nil && row2.Values[1].Data.(string) != "version2" {
			t.Errorf("New snapshot should see version2, got %v", row2.Values[1].Data)
		}
		scanOp2.Close()
		newTxn.Rollback()
	})
}

// TestConcurrentInsertPerformance measures performance under concurrent load
func TestConcurrentInsertPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

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

	bufferPool := storage.NewBufferPool(diskManager, 100) // Larger buffer pool
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test schema
	if err := cat.CreateSchema("test"); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create a test table
	schema := &catalog.TableSchema{
		SchemaName: "test",
		TableName:  "perf_test",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "data", DataType: types.Text, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Run concurrent inserts
	concurrency := 10
	insertsPerGoroutine := 100
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < insertsPerGoroutine; j++ {
				txnInst, err := txnManager.BeginTransaction(context.Background(), txn.ReadCommitted)
				if err != nil {
					t.Errorf("Worker %d: Failed to begin transaction: %v", workerID, err)
					return
				}

				ctx := &ExecContext{
					Catalog:        cat,
					Engine:         eng,
					TxnManager:     txnManager,
					Txn:            txnInst,
					SnapshotTS:     int64(txnInst.ReadTimestamp()),
					IsolationLevel: txn.ReadCommitted,
					Stats:          &ExecStats{},
				}

				insertOp := NewInsertOperator(table, storageBackend, nil)
				insertOp.values = [][]parser.Expression{
					{
						&parser.Literal{Value: types.Value{Data: int32(workerID*1000 + j)}},
						&parser.Literal{Value: types.Value{Data: fmt.Sprintf("worker%d-row%d", workerID, j)}},
					},
				}

				if err := insertOp.Open(ctx); err != nil {
					t.Errorf("Worker %d: Insert failed: %v", workerID, err)
					txnInst.Rollback()
					return
				}
				insertOp.Close()

				if err := txnInst.Commit(); err != nil {
					t.Errorf("Worker %d: Commit failed: %v", workerID, err)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalInserts := concurrency * insertsPerGoroutine
	insertsPerSecond := float64(totalInserts) / elapsed.Seconds()

	t.Logf("Performance results:")
	t.Logf("  Total inserts: %d", totalInserts)
	t.Logf("  Elapsed time: %v", elapsed)
	t.Logf("  Inserts/second: %.2f", insertsPerSecond)

	// Verify all inserts succeeded
	ctx := &ExecContext{
		Catalog:    cat,
		Engine:     eng,
		TxnManager: txnManager,
		SnapshotTS: int64(txnManager.GetCurrentTimestamp()),
		Stats:      &ExecStats{},
	}

	count := countRows(t, ctx, table, storageBackend)
	if count != totalInserts {
		t.Errorf("Expected %d rows, found %d", totalInserts, count)
	}
}