# lib/pq Quick Fixes - Try These First

## Fix #1: Parameter Order and Values (Most Likely Issue)

**File**: `internal/network/connection.go`

**Change** the parameter map in `handleStartup()` from:
```go
parameters := map[string]string{
    "server_version":              "15.0 (QuantaDB 0.1.0)",
    "server_encoding":             "UTF8",
    "client_encoding":             "UTF8",
    "DateStyle":                   "ISO, MDY",
    "integer_datetimes":           "on",
    "TimeZone":                    "UTC",
    "standard_conforming_strings": "on",
}
```

**To**:
```go
// EXACT PostgreSQL 15 parameters in EXACT order
paramList := []struct {
    name  string
    value string
}{
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

// Send in order
for _, param := range paramList {
    if param.value == "" {
        continue // Skip empty values
    }
    ps := &protocol.ParameterStatus{
        Name:  param.name,
        Value: param.value,
    }
    if err := protocol.WriteMessage(c.writer, ps.ToMessage()); err != nil {
        return err
    }
}
```

## Fix #2: Remove Intermediate Flushes

**Problem**: Multiple flushes might confuse lib/pq

**Change** in `handleStartup()`:
```go
// REMOVE all intermediate c.writer.Flush() calls
// Only flush ONCE at the very end

func (c *Connection) handleStartup() error {
    // ... SSL handling ...
    
    // Send auth
    auth := &protocol.Authentication{Type: protocol.AuthOK}
    protocol.WriteMessage(c.writer, auth.ToMessage())
    // NO FLUSH HERE
    
    // Send parameters
    for _, param := range paramList {
        // ...
        protocol.WriteMessage(c.writer, ps.ToMessage())
        // NO FLUSH HERE
    }
    
    // Send backend key
    keyData := &protocol.BackendKeyData{...}
    protocol.WriteMessage(c.writer, keyData.ToMessage())
    // NO FLUSH HERE
    
    // Send ready
    ready := &protocol.ReadyForQuery{Status: protocol.TxnStatusIdle}
    protocol.WriteMessage(c.writer, ready.ToMessage())
    
    // SINGLE FLUSH AT END
    return c.writer.Flush()
}
```

## Fix #3: Valid Process ID

**Problem**: Process ID of 0 or negative might be rejected

**Change** in `NewConnection()`:
```go
// Instead of:
id: connID,

// Use:
id: uint32(connID + 1000), // Ensure > 0 and looks like real PID
```

## Fix #4: Remove "(QuantaDB 0.1.0)" from server_version

**Problem**: lib/pq might parse server_version strictly

**Change**:
```go
// From:
"server_version": "15.0 (QuantaDB 0.1.0)",

// To:
"server_version": "15.0",
```

## Fix #5: Add Missing Parameters

**Problem**: lib/pq might expect additional parameters

**Add these parameters**:
```go
{"IntervalStyle", "postgres"},
{"is_superuser", "off"},  // or "on" based on user
{"session_authorization", c.params["user"]},
```

## Test After Each Fix

After each fix, test with:
```bash
go run test/tpch/debug_connection.go
```

## Complete Quick Fix

Here's the complete `handleStartup()` with all fixes:

```go
func (c *Connection) handleStartup() error {
    // ... existing SSL handling code ...
    
    // Send authentication OK
    auth := &protocol.Authentication{Type: protocol.AuthOK}
    if err := protocol.WriteMessage(c.writer, auth.ToMessage()); err != nil {
        return err
    }
    
    // Send parameters in PostgreSQL order
    paramList := []struct {
        name  string
        value string
    }{
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
    
    for _, param := range paramList {
        if param.value == "" && param.name != "application_name" {
            continue
        }
        ps := &protocol.ParameterStatus{
            Name:  param.name,
            Value: param.value,
        }
        if err := protocol.WriteMessage(c.writer, ps.ToMessage()); err != nil {
            return err
        }
    }
    
    // Generate valid secret key
    if err := binary.Read(rand.Reader, binary.BigEndian, &c.secretKey); err != nil {
        return fmt.Errorf("failed to generate secret key: %w", err)
    }
    
    // Send backend key data with valid process ID
    keyData := &protocol.BackendKeyData{
        ProcessID: c.id + 1000, // Ensure it looks like real PID
        SecretKey: c.secretKey,
    }
    if err := protocol.WriteMessage(c.writer, keyData.ToMessage()); err != nil {
        return err
    }
    
    // Send ready for query
    ready := &protocol.ReadyForQuery{Status: protocol.TxnStatusIdle}
    if err := protocol.WriteMessage(c.writer, ready.ToMessage()); err != nil {
        return err
    }
    
    // CRITICAL: Single flush at the end
    return c.writer.Flush()
}
```

## If These Don't Work

1. Check server.log for any new error messages
2. Run with `PGLOGLEVEL=2` for lib/pq debug output
3. Use tcpdump to compare with real PostgreSQL
4. Try the protocol sniffer tool from the debug plan

Most likely the issue is one of:
- Parameter order/values
- Message buffering/flushing
- Missing required parameters

Start with Fix #1 (parameter order) as it's the most common issue.