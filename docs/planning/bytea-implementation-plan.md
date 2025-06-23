# BYTEA Implementation Plan

## Overview

Implement BYTEA (byte array) data type in QuantaDB to support binary data storage. BYTEA is PostgreSQL's type for storing binary strings - sequences of octets (bytes).

## Background

BYTEA allows storage of binary data such as:
- Images, documents, compressed data
- Cryptographic keys and hashes  
- Serialized objects
- Any arbitrary binary content

PostgreSQL BYTEA format:
- Input: Hex format `\x48656c6c6f` or escape format `\\000\\001\\002`
- Output: Hex format by default
- NULL bytes and non-printable characters are supported

## Implementation Approach

### 1. Type System
- Add BYTEA to DataType enumeration
- Define byte array representation ([]byte in Go)
- Implement DataType interface methods

### 2. Parser Support
- Add TokenBytea to token types
- Update lexer to recognize BYTEA keyword
- Handle BYTEA literal parsing (hex format initially)

### 3. Value Handling
- Create ByteaValue type or use generic handling
- Implement comparison operations
- Handle NULL BYTEA values

### 4. Storage
- Serialize as length-prefixed byte array
- Handle variable-length storage efficiently
- Ensure proper alignment and padding

### 5. Operations
- Equality comparison
- Length function (octet_length)
- Concatenation operator (||)
- Type casting to/from text

## Technical Design

### Type Definition
```go
// In types package
const Bytea DataType = ...

// Value representation
type ByteaValue struct {
    Data []byte
    Null bool
}
```

### Parser Token
```go
TokenBytea TokenType = ...
```

### Storage Format
```
[4 bytes: length] [n bytes: data]
```

### Literal Syntax
```sql
-- Hex format (PostgreSQL standard)
INSERT INTO t VALUES ('\x48656c6c6f');

-- Escape format (legacy)
INSERT INTO t VALUES (E'\\000\\001\\002');
```

## Implementation Steps

1. **Add Type Definition**
   - Update types/types.go with Bytea constant
   - Implement DataType interface methods
   - Add serialization support

2. **Parser Integration**
   - Add BYTEA token to parser
   - Update type parsing in CREATE TABLE
   - Handle BYTEA literals in expressions

3. **Value System**
   - Implement NewByteaValue constructor
   - Add IsNull, AsBytes methods
   - Handle type conversions

4. **Storage Layer**
   - Update row serialization
   - Handle variable-length storage
   - Test with large binary data

5. **Executor Support**
   - Update expression evaluation
   - Handle BYTEA in comparisons
   - Support concatenation

6. **Testing**
   - Unit tests for type system
   - Integration tests for CRUD
   - Boundary tests (empty, large)

## Query Examples

### Basic Operations
```sql
-- Create table with BYTEA column
CREATE TABLE files (
    id INTEGER,
    content BYTEA
);

-- Insert binary data
INSERT INTO files VALUES (1, '\x48656c6c6f');

-- Query binary data
SELECT * FROM files WHERE content = '\x48656c6c6f';

-- Get length
SELECT octet_length(content) FROM files;
```

### Type Casting
```sql
-- Cast text to bytea
SELECT 'Hello'::bytea;

-- Cast bytea to text (if valid UTF-8)
SELECT content::text FROM files;
```

## Success Criteria

1. CREATE TABLE with BYTEA columns works
2. INSERT/SELECT of binary data works
3. Comparison operations work correctly
4. Storage handles NULL and empty values
5. Large binary objects (up to reasonable limit)
6. PostgreSQL-compatible hex format

## Risks and Mitigation

1. **Memory Usage**
   - Risk: Large BYTEA values exhaust memory
   - Mitigation: Set reasonable size limits

2. **Performance**
   - Risk: Slow serialization of large values
   - Mitigation: Efficient byte copying

3. **Compatibility**
   - Risk: Format differences from PostgreSQL
   - Mitigation: Follow PostgreSQL standards

## Future Enhancements

1. Escape format input support
2. Binary protocol support
3. Large object (LOB) support
4. Compression options
5. Streaming API for large values

## Testing Strategy

1. **Unit Tests**
   - Type system tests
   - Serialization round-trip
   - Comparison operations

2. **Integration Tests**
   - Table creation with BYTEA
   - CRUD operations
   - Mixed type queries

3. **Edge Cases**
   - NULL values
   - Empty byte arrays
   - Maximum size values
   - Special bytes (0x00, 0xFF)

4. **Compatibility Tests**
   - PostgreSQL format compatibility
   - Driver compatibility tests