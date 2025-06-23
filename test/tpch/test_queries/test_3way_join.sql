SELECT l.l_orderkey, o.o_orderdate, c.c_name
FROM lineitem l, orders o, customer c
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
LIMIT 5;