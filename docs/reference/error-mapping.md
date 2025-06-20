# PostgreSQL Error Code Mapping

## Overview

QuantaDB implements PostgreSQL-compatible error codes (SQLSTATE) to ensure client compatibility. All errors sent to clients include proper SQLSTATE codes along with detailed error information.

## Implementation

### Error Package Structure

The error system is implemented in `internal/errors/`:
- `codes.go` - PostgreSQL SQLSTATE code constants
- `errors.go` - Core error types and builders
- `categories.go` - Category-specific error constructors

### Error Type

```go
type Error struct {
    Code       string // SQLSTATE code
    Message    string // Primary error message
    Detail     string // Optional detailed error message
    Hint       string // Optional hint message
    Position   int    // Character position in query
    Schema     string // Schema name if applicable
    Table      string // Table name if applicable
    Column     string // Column name if applicable
    // ... additional fields
}
```

## Common Error Mappings

### Syntax Errors (Class 42)
- `42601` - Syntax error
- `42703` - Undefined column
- `42P01` - Undefined table
- `42P07` - Duplicate table
- `42702` - Ambiguous column
- `42803` - Grouping error

### Data Errors (Class 22)
- `22012` - Division by zero
- `22003` - Numeric value out of range
- `22001` - String data right truncation
- `22P02` - Invalid text representation
- `22007` - Invalid datetime format

### Integrity Constraints (Class 23)
- `23502` - Not null violation
- `23505` - Unique violation
- `23503` - Foreign key violation
- `23514` - Check violation

### Transaction Errors (Class 25)
- `25001` - Active SQL transaction
- `25P01` - No active SQL transaction
- `25P02` - In failed SQL transaction
- `25006` - Read only SQL transaction

### Connection Errors (Class 08)
- `08000` - Connection exception
- `08006` - Connection failure
- `08P01` - Protocol violation

### Resource Errors (Class 53)
- `53000` - Insufficient resources
- `53100` - Disk full
- `53200` - Out of memory
- `53300` - Too many connections

## Usage Examples

### Creating Errors

```go
// Syntax error with position
err := errors.ParseError("unexpected token", line, column)

// Table not found
err := errors.TableNotFoundError("users")

// Column not found with table context
err := errors.ColumnNotFoundError("email", "users")

// Constraint violation with details
err := errors.UniqueViolationError("john@example.com", "users_email_key").
    WithTable("public", "users").
    WithColumn("email").
    WithDetail("Key (email)=(john@example.com) already exists.")
```

### Converting Errors

The network layer automatically converts all errors to PostgreSQL format:

```go
func (c *Connection) sendError(err error) error {
    // Convert to QuantaDB error if it isn't already
    qErr := errors.GetError(err)
    
    errResp := &protocol.ErrorResponse{
        Code:           qErr.Code,
        Message:        qErr.Message,
        Detail:         qErr.Detail,
        Hint:           qErr.Hint,
        // ... all other fields
    }
    // Send to client
}
```

### Checking Error Types

```go
if errors.IsError(err, errors.UniqueViolation) {
    // Handle unique constraint violation
}
```

## Integration Points

### Parser
- Returns `ParseError` with line/column information
- Automatically converted to `42601` syntax errors

### Planner
- Table/column not found → `42P01`, `42703`
- Ambiguous references → `42702`
- Invalid queries → `42000`

### Executor
- Division by zero → `22012`
- Type mismatches → `42804`
- Constraint violations → `23xxx`

### Storage
- I/O errors → `58030`
- Corruption → `XX001`
- Space errors → `53100`

### Transaction Manager
- Transaction state errors → `25xxx`
- Deadlocks → `40P01`
- Serialization failures → `40001`

## Client Compatibility

The error system ensures compatibility with all PostgreSQL drivers:
- Go: `pq`, `pgx`
- Python: `psycopg2`
- Java: JDBC PostgreSQL driver
- Node.js: `pg`
- .NET: Npgsql

Clients receive properly formatted error responses with all standard fields populated, allowing them to handle errors using their native PostgreSQL error handling mechanisms.