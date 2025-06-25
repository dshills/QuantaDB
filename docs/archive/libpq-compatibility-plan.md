# lib/pq Compatibility Resolution Plan

## Problem Statement

The Go PostgreSQL driver (lib/pq) fails to connect to QuantaDB with "driver: bad connection" error, while our custom protocol test succeeds. This prevents running TPC-H benchmarks and blocks PostgreSQL client compatibility.

## Root Cause Analysis

### What Works
- TCP connection establishment
- SSL negotiation (server responds 'N' correctly)
- Startup message parsing
- Authentication flow
- Custom client can execute queries successfully

### What Fails
- lib/pq immediately disconnects after connection
- No specific error message from lib/pq
- Server shows successful connection then immediate disconnect

## Investigation Plan

### 1. Deep Protocol Analysis (Priority: High)

**Objective**: Understand exactly what lib/pq expects during connection

**Actions**:
- Use tcpdump/Wireshark to capture lib/pq → PostgreSQL traffic
- Compare byte-by-byte with QuantaDB responses
- Focus on:
  - Message boundaries and buffering
  - Parameter status values
  - Timing between messages
  - Any undocumented protocol expectations

**Tools**:
```bash
# Capture PostgreSQL traffic
tcpdump -i lo0 -w postgres.pcap 'port 5432'

# Capture QuantaDB traffic  
tcpdump -i lo0 -w quantadb.pcap 'port 5432'
```

### 2. lib/pq Source Code Review (Priority: High)

**Objective**: Identify specific validation or expectations in lib/pq

**Key Areas**:
- `conn.go`: Connection establishment logic
- `conn_go18.go`: SSL negotiation
- `buf.go`: Message reading/writing
- Error handling and connection state

**Specific Functions**:
- `DialOpen()`: Initial connection
- `startup()`: Startup message handling
- `recv()`: Message receiving logic
- Parameter validation logic

### 3. Enhanced Logging (Priority: Medium)

**Objective**: Add detailed protocol logging to identify exact failure point

**Implementation**:
```go
// Add to connection.go
type ProtocolLogger struct {
    conn net.Conn
    logger *log.Logger
}

func (p *ProtocolLogger) Write(b []byte) (int, error) {
    p.logger.Printf("WRITE: %d bytes: %x", len(b), b)
    return p.conn.Write(b)
}

func (p *ProtocolLogger) Read(b []byte) (int, error) {
    n, err := p.conn.Read(b)
    if n > 0 {
        p.logger.Printf("READ: %d bytes: %x", n, b[:n])
    }
    return n, err
}
```

### 4. Protocol Fixes (Priority: High)

**Potential Issues to Fix**:

#### A. Message Buffering
- lib/pq might expect all startup response messages in one TCP packet
- Solution: Buffer all startup messages before flushing

#### B. Parameter Values
- Some parameters might have specific format requirements
- Check: `server_version`, `standard_conforming_strings`, `integer_datetimes`

#### C. Backend Key Data
- Ensure process ID is > 0
- Verify secret key generation

#### D. Character Encoding
- lib/pq might validate encoding parameters
- Ensure UTF8 handling is correct

### 5. Implementation Strategy

#### Phase 1: Diagnosis (2-3 hours)
1. Set up PostgreSQL 15 for comparison
2. Capture and analyze protocol traces
3. Add verbose logging to QuantaDB
4. Identify exact divergence point

#### Phase 2: Fix Implementation (3-4 hours)
1. Implement message batching for startup
2. Fix any parameter format issues
3. Ensure proper message ordering
4. Handle any missing protocol features

#### Phase 3: Testing (1-2 hours)
1. Test with lib/pq debug client
2. Test with psql command-line client
3. Test with other PostgreSQL clients (pgAdmin, DBeaver)
4. Run full TPC-H benchmark suite

## Specific Hypotheses to Test

### 1. Startup Message Batching
**Hypothesis**: lib/pq expects all startup responses in specific chunks
**Test**: Buffer AuthOK + all ParameterStatus + BackendKeyData + ReadyForQuery

### 2. Parameter Validation
**Hypothesis**: lib/pq validates specific parameter formats
**Test**: Match exact PostgreSQL 15 parameter values

### 3. Timing Sensitivity
**Hypothesis**: lib/pq has tight timeout expectations
**Test**: Remove all delays between messages

### 4. Extended Protocol
**Hypothesis**: lib/pq immediately sends Parse/Bind messages
**Test**: Ensure we handle extended protocol correctly

## Success Criteria

1. `sql.Open()` and `db.Ping()` succeed with lib/pq
2. Basic queries work: `SELECT 1`, `SELECT version()`
3. TPC-H benchmark suite runs successfully
4. psql command-line client works
5. No regression in custom protocol test

## Fallback Options

If lib/pq compatibility proves too complex:

1. **Custom Driver**: Create a simplified PostgreSQL driver for benchmarking
2. **Direct Mode**: Bypass network layer for benchmarks
3. **Alternative Drivers**: Test with pgx or other Go PostgreSQL drivers
4. **Protocol Subset**: Implement only the subset needed for benchmarking

## Next Steps

1. Start with network capture comparison
2. Add comprehensive protocol logging
3. Test message batching hypothesis
4. Iterate based on findings