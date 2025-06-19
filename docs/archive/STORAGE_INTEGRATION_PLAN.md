# Storage Integration Implementation Plan

## Overview
This plan outlines the steps to connect the SQL executor with the persistent storage engine, enabling QuantaDB to store and retrieve data from disk rather than just memory.

**Status**: ✅ Phase 1-2 Complete, 🔄 Phase 3-4 In Progress

## Current State Analysis

### What Exists:
1. **SQL Executor** (`internal/sql/executor/`) - Executes queries with operators
2. **Storage Engine** (`internal/storage/`) - Page-based disk storage with buffer pool
3. **Table Management** - Tables are created in catalog but not persisted
4. **DML Parsing** - INSERT/UPDATE/DELETE are parsed but not executed properly

### What's Missing:
1. Storage backend implementation for executor operators
2. Table data persistence on disk
3. Connection between catalog and storage engine
4. Proper row storage format integration

## Implementation Steps

### Phase 1: Storage Backend Integration ✅ COMPLETE

#### 1.1 Create Storage Backend Interface ✅
- Defined StorageBackend interface in `storage_backend.go`
- Implemented DiskStorageBackend using buffer pool
- Connected to page manager and buffer pool

#### 1.2 Update Table Creation ✅
- Tables persist to disk via storage backend
- CREATE TABLE allocates initial page
- Catalog tracks table metadata

#### 1.3 Implement Table Scan Operator ✅
- Created StorageScanOperator
- Reads rows from disk pages
- Handles row deserialization with slotted page format

### Phase 2: DML Operations 🔄 IN PROGRESS

#### 2.1 INSERT Implementation ✅
- Created InsertOperator that writes to storage
- Implemented row serialization
- Page management handles space allocation
- Basic implementation (no transaction integration yet)

#### 2.2 UPDATE Implementation ❌ TODO
- Create UpdateOperator with storage backend
- Implement in-place updates where possible
- Handle MVCC versioning for updates

#### 2.3 DELETE Implementation ❌ TODO
- Create DeleteOperator with storage backend
- Implement tombstone marking
- Handle vacuum/cleanup later

### Phase 3: Index Integration 🔄 NEXT PRIORITY

#### 3.1 Index Storage
- Persist B+Tree indexes to disk
- Update index on DML operations
- Implement index-backed scan operators

#### 3.2 Query Planner Updates
- Cost estimation for disk-based operations
- Choose between sequential scan vs index scan
- Update statistics based on actual data

### Phase 4: WAL Implementation ❌ TODO

#### 4.1 WAL Structure
- Design log record format
- Implement log buffer and flushing
- Create recovery manager

#### 4.2 Transaction Integration
- Log all modifications before applying
- Implement checkpoint mechanism
- Add crash recovery on startup

## Success Criteria

1. Can CREATE TABLE and data persists after restart
2. INSERT/UPDATE/DELETE modify persistent storage
3. SELECT queries read from disk
4. Basic crash recovery works
5. Performance benchmarks show reasonable disk I/O

## First Implementation Task

Start with creating the storage backend interface and updating table creation to persist data.