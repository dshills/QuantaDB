# PostgreSQL Driver Testing for QuantaDB

This directory contains tests for verifying PostgreSQL driver compatibility with QuantaDB's Extended Query Protocol implementation.

## Test Coverage

### Core Features Tested
- ✅ Basic connection establishment
- ✅ Simple queries without parameters
- ✅ Prepared statements with parameters ($1, $2 style)
- ✅ Parameter type inference (integer, string, boolean)
- ✅ Transaction handling (BEGIN, COMMIT, ROLLBACK)
- ✅ Multiple parameter queries
- ✅ Error handling for invalid queries

### Driver-Specific Features
- **Go pq**: Connection pooling, prepared statement caching
- **Go pgx**: Batch operations, context cancellation
- **Python psycopg2**: Server-side cursors, named cursors
- **Node.js pg**: Connection pooling, async/await
- **JDBC**: PreparedStatement, ResultSet metadata

## Running Tests

### Prerequisites
1. Start QuantaDB server:
   ```bash
   cd /Users/dshills/Development/projects/QuantaDB
   make build
   ./build/quantadb --port 5432 --data ./test-data
   ```

2. Create test database and tables:
   ```sql
   CREATE TABLE users (
       id INTEGER NOT NULL,
       name TEXT NOT NULL,
       email TEXT,
       age INTEGER,
       active BOOLEAN
   );
   
   CREATE INDEX users_id_idx ON users(id);
   
   INSERT INTO users VALUES 
       (1, 'Alice', 'alice@example.com', 30, true),
       (2, 'Bob', 'bob@example.com', 25, true),
       (3, 'Charlie', 'charlie@example.com', 35, false);
   ```

### Go Drivers
```bash
cd test/drivers/go
go mod init quantadb-driver-test
go get github.com/lib/pq
go get github.com/jackc/pgx/v5
go run test_pq.go
go run test_pgx.go
```

### Python Driver
```bash
cd test/drivers/python
pip install psycopg2-binary
python test_psycopg2.py
```

### Node.js Driver
```bash
cd test/drivers/nodejs
npm init -y
npm install pg
node test_pg.js
```

### JDBC Driver
```bash
cd test/drivers/java
# Download PostgreSQL JDBC driver
curl -o postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
javac -cp postgresql.jar TestJDBC.java
java -cp .:postgresql.jar TestJDBC
```

## Expected Results

All tests should pass without errors, demonstrating:
1. Successful connection to QuantaDB
2. Parameter substitution working correctly
3. Proper result formatting
4. Transaction semantics maintained
5. Error responses in PostgreSQL format

## Troubleshooting

### Common Issues
1. **Connection refused**: Ensure QuantaDB server is running on port 5432
2. **Parameter errors**: Check that Extended Query Protocol is properly implemented
3. **Type mismatches**: Verify parameter type inference is working
4. **SSL errors**: QuantaDB supports SSL negotiation but doesn't require certificates

### Debug Mode
Start QuantaDB with debug logging:
```bash
./build/quantadb --port 5432 --log-level debug
```

This will show detailed information about:
- Connection establishment
- Extended Query Protocol messages
- Parameter binding and substitution
- Query planning and execution