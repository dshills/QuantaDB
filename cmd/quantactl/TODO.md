# quantactl TODO

This CLI tool needs implementation for:

1. **status** - Check database server status
2. **get** - Retrieve values by key
3. **put** - Store key-value pairs
4. **delete** - Delete keys
5. **query** - Execute SQL queries

## Implementation Notes

- Should use the PostgreSQL wire protocol to connect to QuantaDB
- Consider using the pgx library for PostgreSQL protocol support
- Add proper error handling and connection pooling
- Support configuration file and environment variables
- Add authentication support when implemented in server