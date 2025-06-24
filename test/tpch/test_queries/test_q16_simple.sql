-- Test NOT IN with subquery
SELECT COUNT(*) 
FROM partsupp
WHERE ps_suppkey NOT IN (
    SELECT s_suppkey 
    FROM supplier 
    WHERE s_comment LIKE '%Customer%'
);