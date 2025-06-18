# Write-Ahead Logging (WAL) Implementation Plan

## Overview

Write-Ahead Logging (WAL) is a fundamental technique for ensuring durability and enabling crash recovery in database systems. The core principle is that all modifications must be logged before they are applied to the actual data pages.

## Design Goals

1. **Durability**: Ensure committed transactions survive crashes
2. **Atomicity**: Enable transaction rollback and recovery
3. **Performance**: Minimize I/O overhead with batching and buffering
4. **Simplicity**: Start with a straightforward implementation that can be enhanced later

## Architecture

### Components

1. **WAL Manager** (`internal/wal/manager.go`)
   - Central coordinator for all WAL operations
   - Manages log sequence numbers (LSNs)
   - Handles log buffer and flushing

2. **Log Records** (`internal/wal/record.go`)
   - Different record types for different operations
   - Each record has a unique LSN (Log Sequence Number)

3. **Log Buffer** (`internal/wal/buffer.go`)
   - In-memory buffer for log records
   - Batches writes for performance
   - Flushes on commit or when full

4. **Recovery Manager** (`internal/wal/recovery.go`)
   - Scans log on startup
   - Replays uncommitted changes (REDO)
   - Can support UNDO in the future

5. **Checkpoint Manager** (`internal/wal/checkpoint.go`)
   - Periodic checkpoints to limit recovery time
   - Flushes dirty pages and records checkpoint

## Log Record Format

```
+----------------+----------------+----------------+----------------+
|      LSN       |     Type       |     TxnID      |    Length      |
|   (8 bytes)    |   (2 bytes)    |   (8 bytes)    |   (4 bytes)    |
+----------------+----------------+----------------+----------------+
|                          Record Data                               |
|                        (variable length)                           |
+--------------------------------------------------------------------+
|                         Checksum (4 bytes)                         |
+--------------------------------------------------------------------+
```

### Record Types

1. **BEGIN_TXN** - Transaction start
2. **COMMIT_TXN** - Transaction commit
3. **ABORT_TXN** - Transaction abort
4. **INSERT** - Row insertion
5. **DELETE** - Row deletion  
6. **UPDATE** - Row update
7. **CREATE_TABLE** - Table creation
8. **CHECKPOINT** - Checkpoint record

### Record Data Formats

#### INSERT Record
```
TableID (8 bytes) | PageID (4 bytes) | SlotID (2 bytes) | RowData (variable)
```

#### DELETE Record
```
TableID (8 bytes) | PageID (4 bytes) | SlotID (2 bytes)
```

#### UPDATE Record
```
TableID (8 bytes) | PageID (4 bytes) | SlotID (2 bytes) | OldRowData | NewRowData
```

## Implementation Steps

### Phase 1: Basic Infrastructure
1. Define log record types and structures
2. Implement LSN generation and management
3. Create log buffer with basic append operations
4. Implement file-based log storage

### Phase 2: Integration with Storage
1. Modify storage operations to generate log records
2. Add LSN tracking to pages
3. Implement write ordering (log before data)
4. Add transaction ID tracking

### Phase 3: Recovery
1. Implement log scanning on startup
2. Add REDO functionality
3. Handle incomplete transactions
4. Test crash recovery scenarios

### Phase 4: Optimization
1. Add checkpointing
2. Implement log truncation
3. Optimize buffer flushing
4. Add group commit

## Integration Points

### Storage Backend
- Before any page modification, generate appropriate log record
- Track page LSN to know what's been applied
- Ensure log is flushed before page write

### Transaction Manager
- Generate BEGIN/COMMIT/ABORT records
- Ensure log flush on commit
- Track active transactions for recovery

### Buffer Pool
- Coordinate with WAL for write ordering
- Track dirty pages for checkpointing
- Implement force/no-force policies

## Testing Strategy

1. **Unit Tests**
   - Log record serialization/deserialization
   - Buffer management
   - LSN ordering

2. **Integration Tests**
   - Operations generate correct log records
   - Recovery replays log correctly
   - Checkpointing works

3. **Crash Tests**
   - Kill process at various points
   - Verify recovery brings correct state
   - Test with concurrent transactions

## Performance Considerations

1. **Batching**: Group multiple log records in single write
2. **Buffering**: Keep recent log in memory
3. **Sequential Writes**: Append-only log for optimal I/O
4. **Checkpoint Frequency**: Balance recovery time vs overhead

## Future Enhancements

1. **UNDO Logging**: For finer-grained recovery
2. **Log Compression**: Reduce storage overhead
3. **Parallel Recovery**: Speed up startup
4. **Replication**: Use WAL for streaming replication

## Success Criteria

1. All committed transactions survive crashes
2. No data corruption after recovery
3. Minimal performance impact (<10% overhead)
4. Clean, maintainable code with good tests