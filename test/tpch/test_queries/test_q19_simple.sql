-- Test complex OR conditions with simple count
SELECT
    COUNT(*) AS match_count
FROM
    lineitem,
    part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND l_quantity >= 1 AND l_quantity <= 11
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND l_quantity >= 10 AND l_quantity <= 20
    );