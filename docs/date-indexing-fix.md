# Date/Time Indexing Support Fix

## Problem
When creating indexes on DATE or TIMESTAMP columns, the system threw an error: "unsupported type for indexing: time.Time". This prevented loading TPC-H data which requires indexes on date columns like `o_orderdate` and `l_shipdate`.

## Root Cause
The `EncodeValue` function in `/internal/index/index.go` didn't have a case to handle `time.Time` values. The B+Tree index encoder only supported basic types (int32, int64, float32, float64, bool, string) but not time values.

## Solution
Added support for `time.Time` in the index encoder by:
1. Adding a new case in the `EncodeValue` switch statement for `time.Time`
2. Encoding time.Time values as Unix nanoseconds (8 bytes, big-endian) for consistent ordering
3. This works for both DATE and TIMESTAMP types since they both use time.Time internally

## Implementation Details
```go
case time.Time:
    // DATE/TIMESTAMP - For indexing purposes, we encode all time.Time values
    // as Unix nanoseconds (8 bytes) to ensure consistent ordering
    // This works for both DATE and TIMESTAMP types
    nano := v.UnixNano()
    var buf [8]byte
    binary.BigEndian.PutUint64(buf[:], uint64(nano))
    return buf[:], nil
```

## Impact
- Enables indexing on DATE and TIMESTAMP columns
- Allows TPC-H benchmark data to be loaded (requires date column indexes)
- Maintains proper ordering for date/time values in B+Tree indexes
- No changes needed to query processing or storage layer

## Testing
Verified with:
```sql
INSERT INTO orders VALUES (1, 1, 'O', 73422.45, DATE '1993-04-09', '3-MEDIUM', 'Clerk#000000866', 0, 'test comment');
```
Successfully inserts data into a table with an index on the date column.