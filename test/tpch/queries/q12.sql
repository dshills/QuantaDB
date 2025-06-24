-- TPC-H Query 12: Shipping Modes and Order Priority
-- This query determines whether selecting less expensive modes of shipping is negatively
-- affecting the critical-priority orders by causing more parts to be received by customers
-- after the committed date.
SELECT
    l_shipmode,
    SUM(CASE
        WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH'
        THEN 1
        ELSE 0
    END) AS high_line_count,
    SUM(CASE
        WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH'
        THEN 1
        ELSE 0
    END) AS low_line_count
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1 YEAR'
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;