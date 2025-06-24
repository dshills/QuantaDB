# Cross Product and Arithmetic Operators Fix

## Issues Fixed

### 1. Cross Product Bug with Multiple Table Aliases

**Problem**: When using multiple aliases for the same table (e.g., `FROM nation n1, nation n2`), the cross product was producing incorrect duplicate results:
- Expected: (5,5), (5,6), (6,5), (6,6)
- Actual: (5,5), (5,5), (6,6), (6,6)

**Root Cause**: The `StorageScanOperator` was not preserving table alias information from the logical plan, causing issues with column resolution in cross products.

**Fix**: Modified `StorageScanOperator` to:
1. Store table alias in the operator
2. Created `NewStorageScanOperatorWithAlias` function
3. Updated schema generation to populate both `TableName` and `TableAlias` fields
4. Modified `BasicExecutor.buildScanOperator` to use the new function with alias

**Files Changed**:
- `internal/sql/executor/storage_scan.go`
- `internal/sql/executor/executor.go`

### 2. Arithmetic Operators Returning Boolean Values

**Problem**: All arithmetic operators (+, -, *, /) were incorrectly returning boolean values instead of numeric results:
- `SELECT 1 + 1` returned `false` instead of `2`
- `SELECT 10 / 5` returned `false` instead of `2`

**Root Cause**: The `convertExpression` function in the planner was not setting the correct result type for binary expressions based on the operator.

**Fix**: Modified `convertExpression` to determine result type based on operator:
- Arithmetic operators (+, -, *, /, %) return numeric types
- Division always returns Float type for SQL compatibility
- Logical operators (AND, OR) return Boolean
- String concatenation (||) returns Text
- Comparison operators continue to return Boolean

**Files Changed**:
- `internal/sql/planner/planner.go` (lines 688-722 for binary expressions, 735-756 for unary)

## Impact

These fixes enabled:
- Q7 (Volume Shipping) - Uses `nation n1, nation n2` aliases
- Q8 (National Market Share) - Uses multiple aliases and CASE with division

TPC-H coverage increased from 59% (13/22) to 68% (15/22).

## Testing

Created comprehensive test programs to verify:
1. Cross product correctness with simple tables
2. Arithmetic operations returning correct types and values
3. CASE expressions with arithmetic
4. Q7 and Q8 queries executing successfully

All tests pass, confirming the fixes work correctly without breaking existing functionality.