-- Simple benchmark queries to test QuantaDB performance
-- These are basic queries that should work with current implementation

-- Test 1: Table scan
SELECT COUNT(*) FROM customer;

-- Test 2: Filter with scan
SELECT COUNT(*) FROM customer WHERE c_mktsegment = 'BUILDING';

-- Test 3: Simple join
SELECT COUNT(*) 
FROM customer c, orders o 
WHERE c.c_custkey = o.o_custkey;

-- Test 4: Join with filter
SELECT COUNT(*)
FROM customer c, orders o
WHERE c.c_custkey = o.o_custkey
  AND c.c_mktsegment = 'BUILDING';

-- Test 5: Aggregation
SELECT c_mktsegment, COUNT(*) as cnt
FROM customer
GROUP BY c_mktsegment;

-- Test 6: Three-way join
SELECT COUNT(*)
FROM customer c, orders o, lineitem l
WHERE c.c_custkey = o.o_custkey
  AND o.o_orderkey = l.l_orderkey;

-- Test 7: Join with aggregation
SELECT c.c_mktsegment, COUNT(*) as order_count, SUM(o.o_totalprice) as total_revenue
FROM customer c, orders o
WHERE c.c_custkey = o.o_custkey
GROUP BY c.c_mktsegment
ORDER BY total_revenue DESC;