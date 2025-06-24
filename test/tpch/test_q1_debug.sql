-- TPC-H Query 1 (Debug Version)
-- This version includes type checking and intermediate calculations for debugging

-- First, let's check the data types of the columns we'll be using
SELECT 
    'Column Type Check' AS debug_step,
    pg_typeof(l_returnflag) AS returnflag_type,
    pg_typeof(l_linestatus) AS linestatus_type,
    pg_typeof(l_quantity) AS quantity_type,
    pg_typeof(l_extendedprice) AS extendedprice_type,
    pg_typeof(l_discount) AS discount_type,
    pg_typeof(l_tax) AS tax_type,
    pg_typeof(l_shipdate) AS shipdate_type
FROM lineitem
LIMIT 1;

-- Check date arithmetic
SELECT 
    'Date Arithmetic Check' AS debug_step,
    DATE '1998-12-01' AS base_date,
    INTERVAL '90' DAY AS interval_value,
    DATE '1998-12-01' - INTERVAL '90' DAY AS calculated_date,
    pg_typeof(DATE '1998-12-01' - INTERVAL '90' DAY) AS result_type
;

-- Sample a few rows to see intermediate calculations
SELECT 
    'Sample Calculations' AS debug_step,
    l_returnflag,
    l_linestatus,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_shipdate,
    l_extendedprice * (1 - l_discount) AS disc_price,
    l_extendedprice * (1 - l_discount) * (1 + l_tax) AS charge,
    CASE 
        WHEN l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY THEN 'INCLUDED'
        ELSE 'EXCLUDED'
    END AS filter_status
FROM lineitem
LIMIT 10;

-- Check aggregation functions with sample data
SELECT 
    'Aggregation Test' AS debug_step,
    l_returnflag,
    l_linestatus,
    COUNT(*) AS row_count,
    MIN(l_quantity) AS min_qty,
    MAX(l_quantity) AS max_qty,
    SUM(l_quantity) AS sum_qty,
    AVG(l_quantity) AS avg_qty,
    MIN(l_shipdate) AS min_shipdate,
    MAX(l_shipdate) AS max_shipdate
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- Full query with all calculations (same as simple version but with comments)
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;