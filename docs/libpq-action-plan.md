# lib/pq Compatibility - Action Plan

## Immediate Actions (30 minutes)

### 1. Add Protocol Logging
Add detailed logging to see exactly where lib/pq disconnects:

```go
// Add to connection.go at the start of handleStartup()
c.logger.Debug("=== PROTOCOL TRACE START ===")
defer c.logger.Debug("=== PROTOCOL TRACE END ===")

// Log each message sent
c.logger.Debug("Sending message", "type", msgType, "length", len(data))
```

### 2. Fix Parameter Order and Values
PostgreSQL clients expect specific parameter order and values:

```go
// Fixed parameter list matching PostgreSQL 15
parameters := []struct{name, value string}{
    {"application_name", c.params["application_name"]},
    {"client_encoding", "UTF8"},
    {"DateStyle", "ISO, MDY"},
    {"integer_datetimes", "on"},
    {"IntervalStyle", "postgres"},
    {"is_superuser", "off"},
    {"server_encoding", "UTF8"},
    {"server_version", "15.0"},
    {"session_authorization", c.params["user"]},
    {"standard_conforming_strings", "on"},
    {"TimeZone", "UTC"},
}
```

### 3. Fix Message Batching
lib/pq might expect all startup messages in one network packet:

```go
// Use a bytes.Buffer to batch messages
var batch bytes.Buffer
batchWriter := bufio.NewWriter(&batch)

// Write all startup messages to batch
// ... write auth, params, backend key ...

// Send entire batch at once
c.conn.Write(batch.Bytes())
```

## Debug Tools (1 hour)

### 1. Protocol Sniffer
```go
// tools/protocol_sniffer.go
// Intercepts and logs all protocol messages
type ProtocolSniffer struct {
    client net.Conn
    server net.Conn
}

func (ps *ProtocolSniffer) Run() {
    // Log all messages in both directions
    go ps.clientToServer()
    go ps.serverToClient()
}
```

### 2. lib/pq Test Harness
```go
// tools/libpq_test.go
// Minimal lib/pq connection test with debug output
func testLibPQ() {
    os.Setenv("PGLOGLEVEL", "2")
    
    db, err := sql.Open("postgres", 
        "host=localhost port=5432 user=postgres sslmode=disable")
    
    // Add custom logger
    log.SetOutput(&ProtocolLogger{})
    
    err = db.Ping()
    // Analyze output
}
```

## Most Likely Fixes (2 hours)

### Fix 1: Authentication Flow
lib/pq might expect specific authentication flow:

```go
// After sending AuthOK, immediately send parameters
// No delay, no flush between messages
auth := &protocol.Authentication{Type: protocol.AuthOK}
protocol.WriteMessage(c.writer, auth.ToMessage())
// DO NOT FLUSH HERE

// Send all parameters
for _, param := range parameters {
    ps := &protocol.ParameterStatus{...}
    protocol.WriteMessage(c.writer, ps.ToMessage())
}

// Only flush after ALL startup messages
c.writer.Flush()
```

### Fix 2: Backend Key Data
Ensure valid process ID and secret key:

```go
// Process ID must be > 0 and < max int32
c.id = uint32(os.Getpid()) % (1 << 31)

// Secret key must be non-zero
binary.Read(rand.Reader, binary.BigEndian, &c.secretKey)
if c.secretKey == 0 {
    c.secretKey = 0x12345678
}
```

### Fix 3: Handle Parse Message
lib/pq might immediately send a Parse message after ReadyForQuery:

```go
// In handleMessage(), add case for Parse before startup complete
case protocol.MsgParse:
    if c.state != StateReady {
        return fmt.Errorf("unexpected parse message in state %v", c.state)
    }
    return c.handleParse(ctx, msg)
```

## Testing Progression

1. **Level 1**: Basic connection
   ```bash
   go run test/debug_connection.go
   ```

2. **Level 2**: Simple query
   ```sql
   SELECT 1
   ```

3. **Level 3**: Parameterized query
   ```sql
   SELECT $1::text
   ```

4. **Level 4**: Full TPC-H benchmark
   ```bash
   cd test/tpch && go run cmd/benchmark/main.go
   ```

## Fallback Strategy

If lib/pq proves incompatible:

### Option 1: Use pgx Driver
```go
import "github.com/jackc/pgx/v5"
// pgx is more forgiving and has better error messages
```

### Option 2: Simple PostgreSQL Driver
```go
// Create minimal driver that only supports:
// - Simple query protocol
// - No prepared statements
// - No transactions
// Just enough for benchmarking
```

### Option 3: Direct Benchmark Mode
```go
// Skip network layer entirely
// Benchmark executor directly
// Still valuable for performance testing
```

## Success Metrics

1. ✅ lib/pq connects without error
2. ✅ `db.Ping()` succeeds
3. ✅ `SELECT 1` returns result
4. ✅ TPC-H queries execute
5. ✅ psql client works
6. ✅ No performance regression

## Timeline

- **Hour 1**: Add logging, test basic fixes
- **Hour 2**: Implement message batching
- **Hour 3**: Debug with protocol sniffer
- **Hour 4**: Test all clients
- **Hour 5**: Run full benchmarks

## Key Insight

The most common issue with lib/pq compatibility is **message boundaries**. PostgreSQL sends startup messages in specific chunks, and lib/pq has implicit expectations about when data arrives. Focus on:

1. When to flush the writer
2. Which messages to batch together
3. Exact parameter values and order

Start with the simplest fix (parameter order) and work up to more complex solutions (message batching).