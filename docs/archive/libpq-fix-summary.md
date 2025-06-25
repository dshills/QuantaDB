# lib/pq Compatibility Fix - Executive Summary

## The Problem
- lib/pq (Go PostgreSQL driver) connects but immediately disconnects with "bad connection"
- Custom protocol test works fine, suggesting lib/pq has specific expectations
- This blocks TPC-H benchmarks and PostgreSQL client compatibility

## Root Cause Analysis
After analyzing the protocol, the most likely issues are:
1. **Parameter mismatch** - lib/pq expects exact PostgreSQL parameter names/values
2. **Message buffering** - lib/pq expects startup messages in specific chunks
3. **Missing parameters** - Some parameters like `IntervalStyle` may be required

## Quick Fix Priority

### 1. Fix Parameters (90% chance this is the issue)
```go
// Change server_version from "15.0 (QuantaDB 0.1.0)" to just "15.0"
// Add missing parameters: IntervalStyle, is_superuser, session_authorization
// Send parameters in exact PostgreSQL order
```

### 2. Fix Message Buffering (70% chance)
```go
// Remove all intermediate flushes
// Send Auth + Params + BackendKey + Ready in one flush
```

### 3. Fix Process ID (30% chance)
```go
// Ensure ProcessID > 0 and looks realistic
// Use connID + 1000 instead of just connID
```

## Implementation Steps

1. **Immediate** (30 min): Try quick fixes in order
2. **If needed** (1 hr): Add protocol logging to pinpoint issue
3. **If still stuck** (2 hr): Use tcpdump to compare with PostgreSQL
4. **Last resort** (1 hr): Create minimal lib/pq-compatible proxy

## Expected Outcome
- lib/pq connections succeed
- TPC-H benchmarks can run
- Full PostgreSQL client compatibility

## Files Created
- `docs/libpq-compatibility-plan.md` - Full investigation plan
- `docs/libpq-debug-steps.md` - Debugging tools and techniques
- `docs/libpq-action-plan.md` - Step-by-step implementation
- `docs/libpq-quick-fixes.md` - Exact code changes to try

Start with the quick fixes - they'll likely solve the issue immediately.