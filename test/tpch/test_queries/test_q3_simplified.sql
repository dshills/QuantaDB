-- Test simplified Q3 components
-- First, check if we have data
SELECT COUNT(*) FROM customer WHERE c_mktsegment = 'BUILDING';
SELECT COUNT(*) FROM orders WHERE o_orderdate < DATE '1995-03-15';
SELECT COUNT(*) FROM lineitem WHERE l_shipdate > DATE '1995-03-15';

-- Test a simple 3-way join without aggregation
SELECT 
    l.l_orderkey,
    l.l_extendedprice,
    l.l_discount,
    o.o_orderdate,
    o.o_shippriority
FROM
    customer c,
    orders o,
    lineitem l
WHERE
    c.c_mktsegment = 'BUILDING'
    AND c.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND o.o_orderdate < DATE '1995-03-15'
    AND l.l_shipdate > DATE '1995-03-15'
LIMIT 5;