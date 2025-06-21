-- Simplified TPC-H queries that work with QuantaDB's current SQL support

-- Simplified Query 3: Customer Market Segment
-- Original uses date literals which QuantaDB doesn't support yet
SELECT l_orderkey, 
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate, 
       o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC
LIMIT 10;

-- Simplified Query 5: Local Supplier Volume
SELECT n_name, 
       sum(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= '1994-01-01'
  AND o_orderdate < '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC;

-- Simplified Query 10: Returned Item Reporting
SELECT c_custkey, 
       c_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal, 
       n_name, 
       c_address, 
       c_phone, 
       c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= '1993-10-01'
  AND o_orderdate < '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;

-- Test query to verify joins work
SELECT COUNT(*) 
FROM customer, orders 
WHERE c_custkey = o_custkey;

-- Test aggregation
SELECT c_mktsegment, COUNT(*) as cnt, AVG(c_acctbal) as avg_balance
FROM customer
GROUP BY c_mktsegment
ORDER BY cnt DESC;