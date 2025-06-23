# Sort-Merge Join Implementation Design

**Created**: December 21, 2024  
**Status**: Design Phase  
**Target**: Complete in 2-3 days

## Overview

Sort-merge join is a critical join algorithm that excels when:
- Input data is already sorted (e.g., from an index scan)
- Join involves inequality predicates (<, >, <=, >=)
- Memory is limited (can spill to disk)
- Need stable performance characteristics

## Architecture

### Components

```
┌─────────────────┐     ┌─────────────────┐
│  Left Relation  │     │ Right Relation  │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│   Sort Phase    │     │   Sort Phase    │
│  (if needed)    │     │  (if needed)    │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
      ┌──┴───────────────────────┴──┐
      │       Merge Phase           │
      │  - Compare current tuples   │
      │  - Advance pointers         │
      │  - Handle duplicates        │
      └─────────────┬───────────────┘
                    │
                    ▼
              ┌───────────┐
              │  Output   │
              └───────────┘
```

## Detailed Design

### 1. Sort Phase

#### 1.1 Sort Detection
```go
type SortInfo struct {
    IsSorted     bool
    SortKeys     []SortKey
    NullOrdering NullOrdering // NULLS FIRST or NULLS LAST
}

func detectSortOrder(operator Operator) *SortInfo {
    switch op := operator.(type) {
    case *IndexScanOperator:
        // Index scans provide sorted data
        return &SortInfo{
            IsSorted: true,
            SortKeys: op.Index.Columns,
        }
    case *SortOperator:
        // Explicitly sorted
        return op.GetSortInfo()
    default:
        // Not sorted
        return &SortInfo{IsSorted: false}
    }
}
```

#### 1.2 External Sort Implementation
```go
type ExternalSort struct {
    memoryLimit   int64
    tempDir       string
    spillFiles    []string
    compareFn     CompareFunc
    bufferPool    *BufferPool
}

func (es *ExternalSort) Sort(input RowIterator) (SortedIterator, error) {
    // Phase 1: Create sorted runs
    runs := es.createSortedRuns(input)
    
    // Phase 2: Merge runs
    if len(runs) == 1 {
        return runs[0], nil
    }
    
    return es.mergeRuns(runs)
}

func (es *ExternalSort) createSortedRuns(input RowIterator) []SortedIterator {
    var runs []SortedIterator
    
    for {
        // Load rows up to memory limit
        batch := es.loadBatch(input)
        if len(batch) == 0 {
            break
        }
        
        // Sort in memory
        sort.Slice(batch, func(i, j int) bool {
            return es.compareFn(batch[i], batch[j]) < 0
        })
        
        // Write to disk or keep in memory
        if es.shouldSpill() {
            run := es.spillToDisk(batch)
            runs = append(runs, run)
        } else {
            run := NewMemoryIterator(batch)
            runs = append(runs, run)
        }
    }
    
    return runs
}
```

### 2. Merge Phase

#### 2.1 Basic Merge Algorithm
```go
type MergeJoinOperator struct {
    leftChild     Operator
    rightChild    Operator
    joinType      JoinType
    joinCondition Expression
    leftSortKeys  []SortKey
    rightSortKeys []SortKey
    
    // State for merge
    leftIter      SortedIterator
    rightIter     SortedIterator
    leftRow       *Row
    rightRow      *Row
    leftGroup     []*Row  // For handling duplicates
    rightGroup    []*Row
}

func (m *MergeJoinOperator) Next() (*Row, error) {
    for {
        // Initialize if needed
        if m.leftRow == nil {
            m.leftRow, _ = m.leftIter.Next()
        }
        if m.rightRow == nil {
            m.rightRow, _ = m.rightIter.Next()
        }
        
        // Check termination
        if m.leftRow == nil || m.rightRow == nil {
            return m.handleOuterJoin()
        }
        
        // Compare join keys
        cmp := m.compareKeys(m.leftRow, m.rightRow)
        
        switch {
        case cmp < 0:
            // Left < Right: advance left
            if m.joinType == LeftOuter || m.joinType == FullOuter {
                return m.outputUnmatched(m.leftRow, nil)
            }
            m.leftRow, _ = m.leftIter.Next()
            
        case cmp > 0:
            // Left > Right: advance right
            if m.joinType == RightOuter || m.joinType == FullOuter {
                return m.outputUnmatched(nil, m.rightRow)
            }
            m.rightRow, _ = m.rightIter.Next()
            
        default:
            // Equal: found match
            return m.handleMatch()
        }
    }
}
```

#### 2.2 Duplicate Handling
```go
func (m *MergeJoinOperator) handleMatch() (*Row, error) {
    // Collect all rows with same key from both sides
    m.leftGroup = m.collectGroup(m.leftIter, m.leftRow)
    m.rightGroup = m.collectGroup(m.rightIter, m.rightRow)
    
    // Output cartesian product of groups
    return m.outputGroups()
}

func (m *MergeJoinOperator) collectGroup(iter SortedIterator, firstRow *Row) []*Row {
    group := []*Row{firstRow}
    currentKey := m.extractKey(firstRow)
    
    for {
        nextRow, err := iter.Peek() // Look ahead without consuming
        if err != nil || m.compareKey(currentKey, m.extractKey(nextRow)) != 0 {
            break
        }
        
        row, _ := iter.Next()
        group = append(group, row)
    }
    
    return group
}
```

### 3. Join Types Support

#### 3.1 Inner Join
```go
func (m *MergeJoinOperator) innerJoin() (*Row, error) {
    // Only output matching rows
    if m.groupIndex < len(m.leftGroup) * len(m.rightGroup) {
        leftIdx := m.groupIndex / len(m.rightGroup)
        rightIdx := m.groupIndex % len(m.rightGroup)
        
        result := m.combineRows(m.leftGroup[leftIdx], m.rightGroup[rightIdx])
        m.groupIndex++
        
        if m.evaluateJoinCondition(result) {
            return result, nil
        }
    }
    
    // Reset for next groups
    m.leftRow = nil
    m.rightRow = nil
    m.groupIndex = 0
    
    return m.Next()
}
```

#### 3.2 Outer Joins
```go
func (m *MergeJoinOperator) leftOuterJoin() (*Row, error) {
    // Include unmatched left rows with NULL right side
    if m.leftRow != nil && (m.rightRow == nil || m.compareKeys(m.leftRow, m.rightRow) < 0) {
        result := m.combineRows(m.leftRow, m.nullRightRow())
        m.leftRow, _ = m.leftIter.Next()
        return result, nil
    }
    
    return m.innerJoin()
}
```

### 4. Optimization Strategies

#### 4.1 Early Termination
```go
func (m *MergeJoinOperator) canTerminateEarly() bool {
    // For inner join with inequality, can stop when one side exhausted
    if m.joinType == InnerJoin {
        if m.hasInequality() && (m.leftRow == nil || m.rightRow == nil) {
            return true
        }
    }
    return false
}
```

#### 4.2 Index Awareness
```go
func (m *MergeJoinOperator) optimizeWithIndexes() {
    // If one side has index, use it as inner relation
    leftIndexed := m.hasIndex(m.leftChild)
    rightIndexed := m.hasIndex(m.rightChild)
    
    if leftIndexed && !rightIndexed {
        // Swap to make indexed relation inner
        m.swapChildren()
    }
}
```

### 5. Cost Model

```go
type MergeJoinCost struct {
    sortCostLeft   float64
    sortCostRight  float64
    mergeCost      float64
    totalCost      float64
}

func estimateMergeJoinCost(left, right PlanNode, stats *Statistics) MergeJoinCost {
    leftRows := stats.EstimateRows(left)
    rightRows := stats.EstimateRows(right)
    
    cost := MergeJoinCost{}
    
    // Sort cost if not already sorted
    if !isSorted(left) {
        cost.sortCostLeft = leftRows * math.Log2(leftRows) * CPU_TUPLE_COST
    }
    if !isSorted(right) {
        cost.sortCostRight = rightRows * math.Log2(rightRows) * CPU_TUPLE_COST
    }
    
    // Merge cost
    cost.mergeCost = (leftRows + rightRows) * CPU_OPERATOR_COST
    
    // Add I/O cost for external sort
    if leftRows > SORT_MEM_LIMIT {
        passes := math.Log2(leftRows / SORT_MEM_LIMIT)
        cost.sortCostLeft += leftRows * passes * RANDOM_IO_COST
    }
    
    cost.totalCost = cost.sortCostLeft + cost.sortCostRight + cost.mergeCost
    
    return cost
}
```

## Implementation Plan

### Day 1: Core Algorithm
- [ ] Basic merge logic for equi-joins
- [ ] In-memory sort implementation
- [ ] Simple duplicate handling
- [ ] Inner join support

### Day 2: Advanced Features
- [ ] External sort with spilling
- [ ] All join types (left, right, full outer)
- [ ] Inequality join conditions
- [ ] NULL handling

### Day 3: Optimization & Testing
- [ ] Cost model integration
- [ ] Index-aware optimization
- [ ] Performance benchmarks
- [ ] Edge case testing

## Testing Strategy

### Unit Tests
```go
func TestMergeJoinBasic(t *testing.T) {
    // Test simple equi-join
    left := []Row{{1, "A"}, {2, "B"}, {3, "C"}}
    right := []Row{{1, "X"}, {2, "Y"}, {3, "Z"}}
    
    result := executeMergeJoin(left, right, INNER_JOIN)
    expected := []Row{{1, "A", "X"}, {2, "B", "Y"}, {3, "C", "Z"}}
    
    assert.Equal(t, expected, result)
}

func TestMergeJoinDuplicates(t *testing.T) {
    // Test duplicate handling
    left := []Row{{1, "A"}, {1, "B"}, {2, "C"}}
    right := []Row{{1, "X"}, {1, "Y"}, {2, "Z"}}
    
    result := executeMergeJoin(left, right, INNER_JOIN)
    expected := []Row{
        {1, "A", "X"}, {1, "A", "Y"},
        {1, "B", "X"}, {1, "B", "Y"},
        {2, "C", "Z"},
    }
    
    assert.Equal(t, expected, result)
}
```

### Performance Tests
```go
func BenchmarkMergeJoinVsHashJoin(b *testing.B) {
    sizes := []int{1000, 10000, 100000}
    
    for _, size := range sizes {
        b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
            left := generateSortedData(size)
            right := generateSortedData(size)
            
            b.Run("merge", func(b *testing.B) {
                for i := 0; i < b.N; i++ {
                    executeMergeJoin(left, right, INNER_JOIN)
                }
            })
            
            b.Run("hash", func(b *testing.B) {
                for i := 0; i < b.N; i++ {
                    executeHashJoin(left, right, INNER_JOIN)
                }
            })
        })
    }
}
```

## Success Criteria

1. **Correctness**
   - Passes all PostgreSQL compatibility tests
   - Handles all join types correctly
   - Proper NULL semantics

2. **Performance**
   - Within 20% of hash join for equi-joins
   - Better than hash join for pre-sorted data
   - Efficient memory usage with spilling

3. **Integration**
   - Planner chooses sort-merge when appropriate
   - Cost model accurately predicts performance
   - Works with existing operators

---

*This design provides a complete blueprint for implementing sort-merge join in QuantaDB. Follow the implementation plan day by day for systematic progress.*