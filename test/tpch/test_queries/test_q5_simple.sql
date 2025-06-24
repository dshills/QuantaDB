-- Test Q5 with simpler join first
SELECT 
    COUNT(*) as matching_suppliers
FROM
    customer c,
    supplier s,
    nation n
WHERE
    c.c_nationkey = s.s_nationkey
    AND s.s_nationkey = n.n_nationkey
LIMIT 10;