# lib/pq Debugging Steps

## Step 1: Create Protocol Comparison Tool

First, create a tool to compare PostgreSQL and QuantaDB protocol messages:

```go
// test/protocol_compare.go
package main

import (
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "net"
    "time"
)

func captureProtocol(addr string, label string) {
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        fmt.Printf("%s: Failed to connect: %v\n", label, err)
        return
    }
    defer conn.Close()
    
    // Log all bytes
    logConn := &LoggingConn{Conn: conn, Label: label}
    
    // Send SSL request
    sslReq := make([]byte, 8)
    binary.BigEndian.PutUint32(sslReq[0:4], 8)
    binary.BigEndian.PutUint32(sslReq[4:8], 80877103)
    logConn.Write(sslReq)
    
    // Read response
    buf := make([]byte, 1)
    logConn.Read(buf)
    
    // Send startup
    startup := buildStartupMessage()
    logConn.Write(startup)
    
    // Read until ReadyForQuery
    readUntilReady(logConn)
}
```

## Step 2: Common lib/pq Issues

### Issue 1: Buffering Problem
lib/pq uses buffered I/O and expects messages to be properly flushed.

**Fix**:
```go
// In handleStartup() after sending all messages:
if err := c.writer.Flush(); err != nil {
    return fmt.Errorf("failed to flush startup messages: %w", err)
}
```

### Issue 2: Parameter Format
lib/pq validates certain parameters strictly.

**Critical Parameters**:
- `server_version`: Must be parseable (e.g., "15.0")
- `integer_datetimes`: Must be exactly "on" or "off"
- `standard_conforming_strings`: Must be exactly "on" or "off"

### Issue 3: Message Ordering
PostgreSQL sends messages in specific order:
1. Authentication OK
2. Parameter Status (in specific order)
3. Backend Key Data
4. ReadyForQuery

## Step 3: Quick Fixes to Try

### Fix 1: Batch Startup Messages
```go
func (c *Connection) handleStartup() error {
    // ... existing SSL handling ...
    
    // Buffer all startup response messages
    messages := []protocol.Message{}
    
    // Authentication OK
    auth := &protocol.Authentication{Type: protocol.AuthOK}
    messages = append(messages, *auth.ToMessage())
    
    // Parameter status - IN SPECIFIC ORDER
    paramOrder := []string{
        "application_name",
        "client_encoding", 
        "DateStyle",
        "integer_datetimes",
        "IntervalStyle",
        "is_superuser",
        "server_encoding",
        "server_version",
        "session_authorization",
        "standard_conforming_strings",
        "TimeZone",
    }
    
    params := map[string]string{
        "application_name":            "",
        "client_encoding":            "UTF8",
        "DateStyle":                  "ISO, MDY",
        "integer_datetimes":          "on",
        "IntervalStyle":              "postgres",
        "is_superuser":               "on",
        "server_encoding":            "UTF8",
        "server_version":             "15.0",
        "session_authorization":      c.params["user"],
        "standard_conforming_strings": "on",
        "TimeZone":                   "UTC",
    }
    
    for _, name := range paramOrder {
        if value, ok := params[name]; ok {
            ps := &protocol.ParameterStatus{Name: name, Value: value}
            messages = append(messages, *ps.ToMessage())
        }
    }
    
    // Backend key data
    keyData := &protocol.BackendKeyData{
        ProcessID: c.id,
        SecretKey: c.secretKey,
    }
    messages = append(messages, *keyData.ToMessage())
    
    // Ready for query
    ready := &protocol.ReadyForQuery{Status: protocol.TxnStatusIdle}
    messages = append(messages, *ready.ToMessage())
    
    // Write all messages then flush once
    for _, msg := range messages {
        if err := protocol.WriteMessage(c.writer, &msg); err != nil {
            return err
        }
    }
    
    return c.writer.Flush()
}
```

### Fix 2: Handle Startup Parameters
lib/pq sends specific startup parameters that must be acknowledged:

```go
// In handleStartup, after parsing params:
if appName, ok := c.params["application_name"]; ok && appName != "" {
    // lib/pq sends application_name, acknowledge it
    parameters["application_name"] = appName
}
```

### Fix 3: Process ID Fix
Ensure process ID is valid:

```go
// In NewConnection:
c := &Connection{
    id:     uint32(connID), // Ensure > 0
    // ...
}
```

## Step 4: Testing Strategy

1. **Basic lib/pq test**:
```go
db, _ := sql.Open("postgres", "host=localhost port=5432 sslmode=disable")
err := db.Ping()
// Should succeed
```

2. **With debug logging**:
```go
os.Setenv("PGLOGLEVEL", "2") // Enable lib/pq debug
```

3. **Compare with PostgreSQL**:
```bash
# Start PostgreSQL on 5433
# Start QuantaDB on 5432
# Run protocol_compare tool
```

## Step 5: Advanced Debugging

If simple fixes don't work:

1. **Packet-level debugging**:
```bash
sudo tcpdump -i any -X -s 0 'port 5432'
```

2. **strace the client**:
```bash
strace -e trace=network go run test_client.go
```

3. **Add panic recovery**:
```go
defer func() {
    if r := recover(); r != nil {
        c.logger.Error("Panic in connection", "error", r)
    }
}()
```

## Common lib/pq Quirks

1. **Expects immediate response** - No delays between auth and ready
2. **Validates server_version** - Must be valid PostgreSQL version
3. **Checks parameter_status order** - Some clients expect specific order
4. **Requires backend key data** - ProcessID must be > 0
5. **Strict on SSL response** - Must be exactly one byte 'N'

## Emergency Workaround

If all else fails, create a minimal PostgreSQL protocol proxy:

```go
// proxy.go - Forward to real PostgreSQL and log differences
func proxy() {
    // Listen on 5432
    // Forward to PostgreSQL on 5433
    // Log all message differences
}
```