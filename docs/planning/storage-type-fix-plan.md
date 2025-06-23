# Storage Type Mismatch Fix Plan

## Issue Summary
The storage layer expects INTEGER values to be `int32`, but the INSERT operator converts them to `int64`, causing serialization failures.

## Root Cause Analysis

### 1. Type System Expectation
- `internal/sql/types/integer.go`: INTEGER type implementation expects `int32`
- `internal/sql/executor/row.go`: Serialization/deserialization uses `int32` for INTEGER

### 2. INSERT Operation Bug
- `internal/sql/executor/insert.go` (lines 280-303): Converts all INTEGER values to `int64`
- This mismatch causes "expected int32, got int64" errors during serialization

### 3. Impact
- All INSERT operations with INTEGER columns fail
- Foreign key tests fail
- Transaction tests fail
- Date arithmetic tests fail

## Implementation Steps

### Step 1: Fix INSERT Type Conversion
**File**: `internal/sql/executor/insert.go`
**Function**: `convertValueToColumnType` (lines 280-303)

Change the INTEGER case to:
1. Keep `int32` values as-is
2. Convert `int64` to `int32` with bounds checking
3. Convert other numeric types to `int32` with proper validation

### Step 2: Add Bounds Checking
For `int64` to `int32` conversion:
- Check if value is within INT32 range: [-2,147,483,648 to 2,147,483,647]
- Return error if out of bounds

### Step 3: Handle Edge Cases
- NULL values: Return as-is
- String to int32 conversion with validation
- Float to int32 conversion with truncation warning

### Step 4: Test Updates
After fixing the conversion:
1. Run affected tests to verify fix
2. May need to update test data that assumes int64 for INTEGER columns

## Testing Plan

### 1. Unit Tests
- Test convertValueToColumnType with various inputs
- Test boundary values for INTEGER
- Test type conversion errors

### 2. Integration Tests
Run the failing tests:
```bash
go test ./internal/sql -run TestDateArithmeticIntegration
go test ./internal/sql/executor -run TestConcurrentTransactionIsolation
go test ./internal/sql/executor -run TestForeignKeyConstraints
```

### 3. Regression Tests
Run full test suite to ensure no new issues:
```bash
make test
```

## Success Criteria
1. All "expected int32, got int64" errors are resolved
2. INSERT operations work correctly with INTEGER columns
3. Foreign key and transaction tests pass
4. No regression in other tests

## Risk Assessment
- **Low Risk**: Change is localized to type conversion logic
- **Main Risk**: Ensuring all numeric type conversions are handled correctly
- **Mitigation**: Comprehensive testing of all numeric type conversions