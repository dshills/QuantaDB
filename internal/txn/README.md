# Transaction System

This package implements a comprehensive transaction system with Multi-Version Concurrency Control (MVCC) for QuantaDB.

## Features

### Transaction Manager (`Manager`)
- Manages transaction lifecycle (begin, commit, abort)
- Tracks active and committed transactions
- Implements garbage collection for old transaction metadata
- Supports configurable timeouts and thresholds

### MVCC Transactions (`MvccTransaction`)
- Multi-Version Concurrency Control for snapshot isolation
- Support for multiple isolation levels:
  - `ReadUncommitted`: Allows dirty reads
  - `ReadCommitted`: Prevents dirty reads
  - `RepeatableRead`: Prevents dirty and non-repeatable reads
  - `Serializable`: Full isolation with conflict detection

### Versioned Storage
- Each value is tagged with creation and deletion timestamps
- Multiple versions of the same key can exist simultaneously
- Visibility rules ensure transactions see consistent snapshots

## Key Components

### Transaction Types
```go
type MvccTransaction struct {
    ID() TransactionID
    IsolationLevel() IsolationLevel
    ReadTimestamp() Timestamp
    WriteTimestamp() Timestamp
    
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Commit() error
    Rollback() error
}
```

### MVCC Versioning
```go
type VersionedValue struct {
    Value           []byte
    CreatedByTxn    TransactionID
    CreatedAt       Timestamp
    DeletedByTxn    TransactionID
    DeletedAt       Timestamp
    NextVersion     *VersionedValue
}
```

## Usage Example

```go
// Create transaction manager
engine := engine.NewMemoryEngine()
manager := txn.NewManager(engine, nil)

// Begin transaction
ctx := context.Background()
tx, err := manager.BeginTransaction(ctx, txn.ReadCommitted)
if err != nil {
    return err
}

// Perform operations
err = tx.Put([]byte("key"), []byte("value"))
if err != nil {
    tx.Rollback()
    return err
}

value, err := tx.Get([]byte("key"))
if err != nil {
    tx.Rollback()
    return err
}

// Commit transaction
err = tx.Commit()
if err != nil {
    return err
}
```

## Integration with Executor

The transaction system integrates with the query executor through the `ExecContext`:

```go
type ExecContext struct {
    TxnManager *txn.Manager
    Txn        *txn.MvccTransaction
    // ... other fields
}
```

## Current Limitations

1. **Scan Operations**: The current scan operator doesn't fully support MVCC transactions. Scans use the underlying engine directly rather than going through the transaction layer.

2. **Recovery**: Transaction logging and recovery are not yet implemented.

3. **Deadlock Detection**: No deadlock detection mechanism is currently implemented.

## Future Enhancements

1. **Transactional Scans**: Extend the scan operator to respect transaction visibility rules.

2. **Write-Ahead Logging**: Implement persistent transaction logs for durability.

3. **Deadlock Detection**: Add deadlock detection and resolution.

4. **Lock Management**: Implement intention locks for better concurrency.

5. **Read-Only Optimization**: Optimize read-only transactions to avoid unnecessary overhead.

## Test Coverage

The transaction system has comprehensive test coverage (80.0%) including:
- Basic transaction operations
- MVCC visibility rules
- Isolation level enforcement
- Concurrent transaction scenarios
- Timeout and error handling
- Integration with the storage engine

Run tests with:
```bash
go test ./internal/txn/ -v
```