-- Test EXISTS
SELECT COUNT(*) 
FROM orders o
WHERE EXISTS (
    SELECT 1 
    FROM lineitem l 
    WHERE l.l_orderkey = o.o_orderkey
)
LIMIT 10;
