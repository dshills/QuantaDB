# Index Package

This package provides indexing functionality for QuantaDB, implementing B+Tree indexes for efficient data retrieval.

## Features

- **B+Tree Implementation**: High-performance B+Tree with configurable degree
- **Unique Constraints**: Support for unique and non-unique indexes
- **NULL Handling**: Configurable NULL value support in indexes
- **Range Scans**: Efficient range queries with ordered results
- **Concurrent Access**: Thread-safe operations with read-write locks
- **Composite Keys**: Support for multi-column indexes
- **Type Encoding**: Proper encoding for all SQL data types

## Architecture

### B+Tree Structure
- Internal nodes contain only keys for navigation
- All data is stored in leaf nodes
- Leaf nodes are linked for efficient range scans
- Configurable degree (default: 128) for optimal performance

### Components

1. **BTree**: Core B+Tree implementation
   - Insert, Delete, Search operations
   - Range scan support
   - Automatic balancing with splits and merges

2. **Index Interface**: Generic index interface
   - Supports different index types (B+Tree, Hash, etc.)
   - Pluggable architecture for future index types

3. **KeyEncoder**: Encodes SQL values for indexing
   - Preserves ordering for comparable types
   - Handles all SQL data types
   - Composite key support

4. **IndexManager**: Manages all indexes for tables
   - Creates and drops indexes
   - Updates indexes on data changes
   - Provides index statistics

## Usage

### Creating an Index

```go
// Create a B+Tree index
idx := NewBTreeIndex(unique, nullable)

// Insert a key-value pair
encoder := KeyEncoder{}
key, _ := encoder.EncodeValue(types.NewValue(int32(42)))
rowID := []byte("row-id-123")
err := idx.Insert(key, rowID)

// Search for a value
rowIDs, err := idx.Search(key)

// Range scan
entries, err := idx.Range(startKey, endKey)
```

### Index Manager

```go
// Create index manager
manager := NewManager(catalog)

// Create an index on a table
err := manager.CreateIndex("schema", "table", "idx_name", []string{"column"}, true)

// Get index for use
index, err := manager.GetIndex("schema", "table", "idx_name")
```

## Performance

Benchmarks on Apple M4 Pro:
- Insert: ~1,268 ns/op (824K ops/sec)
- Search: ~594 ns/op (2M ops/sec)
- Range: ~4,338 ns/op (280K ops/sec)

## Integration with Query Planner

The index package integrates with the query planner to:
- Select appropriate indexes for queries
- Extract index bounds from filter predicates
- Convert table scans to index scans when beneficial

## Future Enhancements

1. **Hash Indexes**: For equality-only queries
2. **Bitmap Indexes**: For low-cardinality columns
3. **Index-Only Scans**: Return data directly from index
4. **Partial Indexes**: Index subset of rows based on predicate
5. **Expression Indexes**: Index computed values
6. **Index Hints**: Allow query hints for index selection