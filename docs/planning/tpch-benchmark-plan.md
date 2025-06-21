# TPC-H Benchmark Implementation Plan

## Overview

TPC-H is the industry standard decision support benchmark consisting of 22 complex SQL queries. We'll implement queries 3, 5, 8, and 10 as they test key optimizer capabilities:

- **Query 3**: Customer Market Segment - Tests joins, aggregation, and sorting
- **Query 5**: Local Supplier Volume - Tests multi-way joins and aggregation
- **Query 8**: National Market Share - Tests complex joins and date arithmetic
- **Query 10**: Returned Item Reporting - Tests outer joins and aggregation

## Implementation Steps

### 1. Schema Creation
Create TPC-H tables:
- NATION (25 rows)
- REGION (5 rows)
- PART (SF * 200,000 rows)
- SUPPLIER (SF * 10,000 rows)
- PARTSUPP (SF * 800,000 rows)
- CUSTOMER (SF * 150,000 rows)
- ORDERS (SF * 1,500,000 rows)
- LINEITEM (SF * 6,000,000 rows)

SF = Scale Factor (we'll use 0.01 for testing, 0.1 for benchmarks)

### 2. Data Generation
- Create data generator for TPC-H spec compliance
- Generate test data at SF=0.01 (1% scale)
- Generate benchmark data at SF=0.1 (10% scale)

### 3. Query Implementation

#### Query 3: Customer Market Segment
```sql
SELECT l_orderkey, 
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate, 
       o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < date '1995-03-15'
  AND l_shipdate > date '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue desc, o_orderdate
LIMIT 10;
```

#### Query 5: Local Supplier Volume
```sql
SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= date '1994-01-01'
  AND o_orderdate < date '1995-01-01'
GROUP BY n_name
ORDER BY revenue desc;
```

#### Query 8: National Market Share
```sql
SELECT o_year,
       sum(case when nation = 'BRAZIL' then volume else 0 end) / 
       sum(volume) as mkt_share
FROM (
  SELECT extract(year from o_orderdate) as o_year,
         l_extendedprice * (1 - l_discount) as volume,
         n2.n_name as nation
  FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
  WHERE p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate between date '1995-01-01' and date '1996-12-31'
    AND p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
GROUP BY o_year
ORDER BY o_year;
```

#### Query 10: Returned Item Reporting
```sql
SELECT c_custkey, c_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= date '1993-10-01'
  AND o_orderdate < date '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue desc
LIMIT 20;
```

### 4. Benchmark Framework
- Create benchmark runner
- Measure query execution time
- Track optimizer statistics
- Generate performance reports
- Compare with and without optimizations

### 5. Success Metrics
- All queries execute correctly
- Performance baseline established
- Optimizer improvements measurable
- Results reproducible

## Timeline
- Schema & Data Generation: 2 hours
- Query Implementation: 3 hours  
- Benchmark Framework: 2 hours
- Testing & Validation: 1 hour

Total: ~8 hours