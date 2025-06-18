# Storage Package

This package implements the persistent storage layer for QuantaDB, providing page-based disk storage with an efficient buffer pool manager.

## Architecture

### Page Structure
- **Page Size**: 8KB (8192 bytes)
- **Page Header**: 24 bytes containing metadata
- **Page Types**: Data, Index, Free, Overflow
- **Slotted Page Layout**: Variable-length record storage with slot directory

### Components

#### 1. Page (`page.go`)
Base page structure with serialization/deserialization support:
- Fixed 8KB size for optimal I/O
- Header contains page ID, type, LSN, free space info
- Support for different page types

#### 2. Slotted Page (`slotted_page.go`)
Variable-length record storage within pages:
- Slot directory grows from beginning
- Records grow from end (backward)
- Supports add, get, update, delete operations
- Efficient space management

#### 3. Disk Manager (`disk_manager.go`)
Handles all disk I/O operations:
- Page allocation and deallocation
- Synchronous read/write operations
- File management and extension
- Metadata page (page 0) management

#### 4. Buffer Pool (`buffer_pool.go`)
In-memory page cache with LRU eviction:
- Configurable pool size
- Pin/unpin mechanism for page usage tracking
- Dirty page tracking and flushing
- Thread-safe concurrent access
- LRU eviction for unpinned pages

## Usage Example

```go
// Create disk manager
dm, err := NewDiskManager("database.db")
if err != nil {
    log.Fatal(err)
}
defer dm.Close()

// Create buffer pool with 100 page capacity
bp := NewBufferPool(dm, 100)

// Allocate a new page
page, err := bp.NewPage()
if err != nil {
    log.Fatal(err)
}

// Use as slotted page for records
sp := &SlottedPage{Page: page}

// Add a record
data := []byte("Hello, QuantaDB!")
slotNum, err := sp.AddRecord(data)
if err != nil {
    log.Fatal(err)
}

// Mark page as dirty and unpin
bp.UnpinPage(page.Header.PageID, true)

// Flush all dirty pages to disk
bp.FlushAllPages()
```

## Performance Characteristics

### Page Layout
- Slotted pages allow variable-length records
- No internal fragmentation for records
- Efficient space utilization
- Fast record access by slot number

### Buffer Pool
- LRU eviction minimizes disk I/O
- Pin counting prevents eviction of in-use pages
- Batch flushing for efficiency
- Thread-safe for concurrent access

### Disk I/O
- Sequential page allocation
- 8KB aligned I/O operations
- Synchronous writes with explicit flush control

## Future Enhancements

1. **Page Compression**: Transparent page compression
2. **Async I/O**: Asynchronous disk operations
3. **Prefetching**: Read-ahead for sequential scans
4. **Page Versioning**: Support for MVCC at page level
5. **Checksums**: Page integrity verification
6. **Defragmentation**: Online page compaction
7. **Large Objects**: Overflow page chains for large records

## Integration Points

This storage layer integrates with:
- **Transaction Manager**: For WAL and recovery
- **Index Manager**: For B+Tree page storage
- **Query Executor**: For table scans and data access
- **Catalog**: For metadata persistence