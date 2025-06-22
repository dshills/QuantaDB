# Immediate Action Plan - GROUP BY Crash Fix

## Priority 1: Debug and Fix GROUP BY Server Crash

### Current Status
- GROUP BY queries cause server crash with SIGSEGV
- Simple COUNT(*) works, but COUNT(*) with GROUP BY crashes
- This is blocking all aggregate query testing

### Debugging Strategy

#### Step 1: Reproduce the Crash Locally
```sql
-- Known failing query
SELECT c_mktsegment, COUNT(*) FROM customer GROUP BY c_mktsegment;
```

#### Step 2: Add Debug Logging
Add comprehensive logging to GROUP BY execution path:
- GroupByOperator initialization
- Row processing
- Result building
- Schema handling

#### Step 3: Identify Root Cause
Likely culprits:
1. Null pointer in groups map
2. Schema not properly propagated
3. Result object creation failure
4. Memory management issue

### Implementation Steps

#### 1. Add Debug Infrastructure
- Enable detailed logging in GroupByOperator
- Add nil checks throughout execution path
- Add memory allocation tracking

#### 2. Fix GROUP BY Implementation
- Fix result object creation
- Ensure proper schema propagation
- Add comprehensive error handling

#### 3. Test and Validate
- Test with various GROUP BY patterns
- Verify memory safety
- Run regression tests

### Expected Timeline
- **Day 1**: Reproduce crash, add logging
- **Day 2-3**: Identify and fix root cause
- **Day 4**: Test and validate fix

## Next Steps After GROUP BY Fix

### Priority 2: JOIN Column Resolution
- Fix column resolver for qualified names
- Test with customer/orders JOIN

### Priority 3: Aggregate Expressions
- Support SUM(a)/SUM(b) in projections
- Enable TPC-H Q8 execution

## Success Criteria
- No server crashes on GROUP BY queries
- Correct results for aggregate queries
- All existing functionality still works