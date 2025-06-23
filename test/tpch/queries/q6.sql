-- TPC-H Query 6: Forecasting Revenue Change
-- This query quantifies the amount of revenue increase that would have resulted
-- from eliminating certain company-wide discounts in a given percentage range
-- in a given year. Asking this type of "what if" query can be used to look for
-- ways to increase revenues.
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 YEAR'
    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 24;