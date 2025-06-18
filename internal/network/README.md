# Network Layer

This package implements the PostgreSQL wire protocol v3, allowing standard PostgreSQL clients to connect to QuantaDB.

## Features

- PostgreSQL wire protocol v3 compatibility
- Simple query protocol support
- Transaction control (BEGIN/COMMIT/ROLLBACK)
- Multiple concurrent connections
- Configurable timeouts and connection limits

## Architecture

```
Client (psql, JDBC, etc.)
    ↓
TCP Server (:5432)
    ↓
Connection Handler
    ↓
Protocol Parser
    ↓
Query Executor
    ↓
Result Formatter
    ↓
Client
```

## Usage

### Starting the Server

```bash
./build/quantadb --host localhost --port 5432
```

### Connecting with psql

```bash
psql -h localhost -p 5432 -U user -d database
```

### Supported SQL Commands

Currently supported:
- SELECT queries
- BEGIN/COMMIT/ROLLBACK
- Basic expressions and filtering

## Protocol Implementation

### Message Types

**Frontend (Client → Server):**
- Query (Q) - Simple query execution
- Terminate (X) - Close connection
- Sync (S) - Synchronize after error

**Backend (Server → Client):**
- Authentication (R) - Auth request/response
- RowDescription (T) - Result set schema
- DataRow (D) - Result data
- CommandComplete (C) - Query completion
- ReadyForQuery (Z) - Ready for new query
- ErrorResponse (E) - Error details
- ParameterStatus (S) - Server parameters

### Connection Flow

1. Client connects via TCP
2. Client sends StartupMessage
3. Server responds with Authentication
4. Server sends ParameterStatus messages
5. Server sends BackendKeyData
6. Server sends ReadyForQuery
7. Client can now send queries

## Current Limitations

- No SSL/TLS support
- No prepared statements (extended query protocol)
- No COPY protocol
- No authentication (accepts all connections)
- Limited data type support

## Testing

Run the network tests:
```bash
go test ./internal/network/...
```

Test with psql:
```bash
# Start server
./build/quantadb

# In another terminal
echo "SELECT 1;" | psql -h localhost -p 5432
```

## Future Enhancements

1. **Extended Query Protocol**: Support for prepared statements
2. **Authentication**: Implement password and certificate auth
3. **SSL/TLS**: Encrypted connections
4. **COPY Protocol**: Bulk data import/export
5. **Connection Pooling**: Better resource management
6. **Protocol Extensions**: Custom QuantaDB features