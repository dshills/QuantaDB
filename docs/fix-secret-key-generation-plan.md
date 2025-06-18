# Fix Secret Key Generation Security Issue

## Problem Summary

The current implementation uses a predictable formula (`c.id * 12345`) to generate the secret key in BackendKeyData. This is a critical security vulnerability as it makes session hijacking possible if an attacker can guess or discover the connection ID.

## Technical Details

### Current Implementation
- **Location**: `internal/network/connection.go:234`
- **Issue**: `SecretKey: c.id * 12345` - Predictable formula based on connection ID
- **Context**: BackendKeyData is sent during PostgreSQL connection startup

### PostgreSQL Protocol Requirements
- BackendKeyData contains ProcessID and SecretKey
- Used for cancellation requests (client can cancel queries using these values)
- Must be unpredictable to prevent unauthorized cancellations
- 32-bit integer value

## Implementation Plan

### Step 1: Add Secure Random Generation
Replace the predictable formula with cryptographically secure random generation:

```go
import (
    "crypto/rand"
    "encoding/binary"
)

// Generate secure random secret key
var secretKey uint32
err := binary.Read(rand.Reader, binary.BigEndian, &secretKey)
if err != nil {
    return fmt.Errorf("failed to generate secret key: %w", err)
}
```

### Step 2: Store Secret Key in Connection
The secret key needs to be stored for later validation during cancellation requests:

```go
type Connection struct {
    // ... existing fields ...
    secretKey uint32  // Add this field
}
```

### Step 3: Update BackendKeyData Creation
Modify the authentication completion to use the secure key:

```go
// Generate secure secret key during connection setup
var secretKey uint32
err := binary.Read(rand.Reader, binary.BigEndian, &secretKey)
if err != nil {
    return fmt.Errorf("failed to generate secret key: %w", err)
}
c.secretKey = secretKey

// Send backend key data
keyData := &protocol.BackendKeyData{
    ProcessID: c.id,
    SecretKey: c.secretKey,
}
```

### Step 4: Implement Cancellation Request Validation
Add proper validation for cancellation requests (if not already implemented):

```go
func (s *Server) handleCancellationRequest(processID uint32, secretKey uint32) error {
    conn := s.findConnection(processID)
    if conn == nil {
        return nil // Silently ignore per PostgreSQL behavior
    }
    
    if conn.secretKey != secretKey {
        return nil // Invalid secret key, ignore
    }
    
    // Proceed with cancellation
    return conn.Cancel()
}
```

### Step 5: Add Tests
Create tests to verify:
1. Secret keys are random and unpredictable
2. Different connections get different secret keys
3. Cancellation requests validate the secret key correctly

```go
func TestSecretKeyGeneration(t *testing.T) {
    // Create multiple connections
    keys := make(map[uint32]bool)
    for i := 0; i < 100; i++ {
        conn := &Connection{id: uint32(i)}
        // Generate secret key (extract the logic into a method)
        var secretKey uint32
        err := binary.Read(rand.Reader, binary.BigEndian, &secretKey)
        require.NoError(t, err)
        
        // Check for duplicates
        require.False(t, keys[secretKey], "Duplicate secret key generated")
        keys[secretKey] = true
        
        // Verify it's not predictable
        require.NotEqual(t, conn.id * 12345, secretKey)
    }
}
```

## Implementation Steps

1. **Locate and review** all uses of BackendKeyData
2. **Add secretKey field** to Connection struct
3. **Implement secure generation** using crypto/rand
4. **Update BackendKeyData creation** to use secure key
5. **Add cancellation validation** if missing
6. **Write comprehensive tests**
7. **Update any documentation** about the protocol implementation

## Testing Strategy

### Unit Tests
- Test secret key randomness
- Test uniqueness across connections
- Test cancellation request validation

### Integration Tests
- Test actual PostgreSQL client connections
- Test cancellation requests with valid/invalid keys
- Test connection stability after changes

### Security Validation
- Verify no predictable patterns in generated keys
- Ensure proper entropy in random generation
- Check for any timing attacks

## Rollback Plan
- Keep original code commented for reference
- Add feature flag if needed for gradual rollout
- Monitor for any connection issues after deployment

## Success Criteria
1. Secret keys are cryptographically secure
2. No predictable patterns in key generation
3. Cancellation requests properly validate keys
4. All existing tests pass
5. New security tests pass
6. No performance degradation

## Estimated Time
- Implementation: 1-2 hours
- Testing: 1 hour
- Code review and refinement: 30 minutes
- Total: 2-3 hours