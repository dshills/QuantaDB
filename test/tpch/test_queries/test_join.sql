SELECT o.o_orderkey, c.c_name 
FROM orders o, customer c 
WHERE o.o_custkey = c.c_custkey 
LIMIT 5;