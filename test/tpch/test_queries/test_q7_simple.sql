-- Test Q7 without table aliases
-- First check if we can do the basic join
SELECT 
    COUNT(*) as total_volume_records
FROM
    supplier,
    lineitem,
    orders,
    customer
WHERE
    s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
LIMIT 10;