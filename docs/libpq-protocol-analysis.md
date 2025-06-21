# PostgreSQL Wire Protocol Analysis - lib/pq Compatibility Issues

## Key Findings from Protocol Review

After reviewing the official PostgreSQL wire protocol documentation, I've identified several issues in QuantaDB's implementation that are causing lib/pq compatibility problems.

### 1. Required Parameter Status Messages

PostgreSQL documentation states there is a **hard-wired set of parameters** that MUST be sent:

**Required Parameters (in order):**
- `server_version`
- `server_encoding`
- `client_encoding`
- `application_name`
- `default_transaction_read_only`
- `in_hot_standby`
- `is_superuser`
- `session_authorization`
- `DateStyle`
- `IntervalStyle`
- `TimeZone`
- `integer_datetimes`
- `standard_conforming_strings`

**QuantaDB is missing:**
- `application_name` (must be sent even if empty)
- `default_transaction_read_only`
- `in_hot_standby`
- `IntervalStyle`
- `is_superuser`
- `session_authorization`

### 2. Parameter Values Format

- `server_version`: Should be a simple version like "15.0", not "15.0 (QuantaDB 0.1.0)"
- `application_name`: Must be sent from startup parameters or empty string
- `is_superuser`: Must be "on" or "off"
- `default_transaction_read_only`: Must be "on" or "off"
- `in_hot_standby`: Must be "on" or "off"

### 3. Message Ordering

The protocol specifies this exact order after authentication:
1. AuthenticationOk
2. ParameterStatus messages (multiple)
3. BackendKeyData
4. ReadyForQuery

### 4. SSL Negotiation Issue

The current implementation reads the startup message incorrectly after SSL negotiation. When SSL is requested:
1. Client sends 8-byte SSL request
2. Server responds with 'N'
3. Client sends **new** startup message
4. Server must read this as a fresh message, not continue from previous buffer

## Specific Code Fixes Required

### Fix 1: Correct Parameter List
```go
// In handleStartup(), send ALL required parameters
parameters := []struct {
    name  string
    value string
}{
    {"application_name", c.params["application_name"]}, // Can be empty
    {"client_encoding", "UTF8"},
    {"DateStyle", "ISO, MDY"},
    {"default_transaction_read_only", "off"},
    {"in_hot_standby", "off"},
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

### Fix 2: Correct SSL Handling
```go
// After sending 'N' for SSL, read complete new startup message
if length == 8 && version == protocol.SSLRequestCode {
    // Send 'N'
    c.conn.Write([]byte{'N'})
    
    // Now read a COMPLETE new startup message
    // DO NOT use the existing buffer
    return c.handleStartup() // Recursive call to read fresh
}
```

### Fix 3: Process ID Must Be Valid
```go
// Ensure process ID > 0
if c.id == 0 {
    c.id = 1000 + uint32(connID)
}
```

### Fix 4: Single Flush Pattern
```go
// Write all messages without flushing
protocol.WriteMessage(c.writer, authOk)
for _, param := range parameters {
    protocol.WriteMessage(c.writer, paramStatus)
}
protocol.WriteMessage(c.writer, backendKeyData)
protocol.WriteMessage(c.writer, readyForQuery)

// Single flush at the end
return c.writer.Flush()
```

## Why lib/pq Fails

lib/pq is very strict about the startup sequence. It expects:
1. Exact parameter names and values
2. All required parameters to be present
3. Proper SSL negotiation flow
4. Valid process IDs
5. Correct message boundaries

The "bad connection" error occurs because lib/pq detects protocol violations during the startup phase and immediately closes the connection.

## Testing the Fix

After implementing these fixes, test with:
```go
db, err := sql.Open("postgres", "host=localhost port=5432 sslmode=disable")
err = db.Ping() // Should succeed
```

## References

- PostgreSQL Protocol Flow: https://www.postgresql.org/docs/current/protocol-flow.html
- Protocol Message Formats: https://www.postgresql.org/docs/current/protocol-message-formats.html
- Required Parameters: Section 53.2.6 of PostgreSQL documentation