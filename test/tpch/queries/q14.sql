-- TPC-H Query 14: Promotion Effect
-- This query monitors the market response to a promotion such as TV advertisements or
-- a special campaign. The query determines how much average revenue increase would have
-- resulted from eliminating certain company-wide discounts in a given percentage range
-- in a given year. Asking this type of "what if" query can be used to look for ways to
-- increase revenues.
SELECT
    100.00 * SUM(CASE
        WHEN p_type LIKE 'PROMO%'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0
    END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1 MONTH';