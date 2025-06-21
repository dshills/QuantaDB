-- Phase 4: Query Transformation Test Queries
-- These queries test the transformations and optimizations in Phase 4

-- ============================================
-- 1. PROJECTION PUSHDOWN TESTS
-- ============================================

-- Test 1.1: Simple projection through join
-- Only t1.id and t2.name should be read from storage
SELECT t1.id, t2.name 
FROM large_table t1 
JOIN large_table2 t2 ON t1.ref_id = t2.id
WHERE t2.status = 'active';

-- Test 1.2: Projection with expressions
-- Should compute expressions after projection
SELECT 
    UPPER(c.name) as customer_name,
    o.total * 1.1 as total_with_tax
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE o.order_date > '2024-01-01';

-- Test 1.3: Projection elimination with index-only scan
-- Should use covering index and never access table
SELECT id, email 
FROM users 
WHERE email LIKE 'test%'
ORDER BY email
LIMIT 10;

-- Test 1.4: Complex projection through multiple joins
-- Should eliminate unused columns at each join level
SELECT 
    c.name,
    p.product_name,
    SUM(oi.quantity) as total_qty
FROM customers c
JOIN orders o ON c.id = o.customer_id  
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
WHERE c.region = 'US'
GROUP BY c.name, p.product_name;

-- ============================================
-- 2. SUBQUERY TESTS
-- ============================================

-- Test 2.1: Scalar subquery in SELECT
SELECT 
    name,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count
FROM customers c;

-- Test 2.2: Scalar subquery in WHERE
SELECT * FROM products p
WHERE price > (SELECT AVG(price) FROM products);

-- Test 2.3: EXISTS subquery (uncorrelated)
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.total > 1000);

-- Test 2.4: EXISTS subquery (correlated)
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.id 
    AND o.total > 1000
);

-- Test 2.5: NOT EXISTS
SELECT * FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.id
);

-- Test 2.6: IN with subquery
SELECT * FROM customers
WHERE id IN (
    SELECT DISTINCT customer_id 
    FROM orders 
    WHERE order_date > '2024-01-01'
);

-- Test 2.7: NOT IN with NULLs
-- Must handle NULL semantics correctly
SELECT * FROM products
WHERE category_id NOT IN (
    SELECT id FROM categories WHERE active = false
);

-- Test 2.8: Correlated subquery with aggregate
SELECT 
    c.*,
    (SELECT MAX(order_date) 
     FROM orders o 
     WHERE o.customer_id = c.id) as last_order_date
FROM customers c;

-- Test 2.9: Multiple correlated subqueries
SELECT 
    p.name,
    p.price,
    (SELECT AVG(price) FROM products p2 
     WHERE p2.category_id = p.category_id) as avg_category_price,
    (SELECT COUNT(*) FROM order_items oi 
     WHERE oi.product_id = p.id) as times_ordered
FROM products p
WHERE p.price > (
    SELECT AVG(price) * 0.8 
    FROM products p3 
    WHERE p3.category_id = p.category_id
);

-- Test 2.10: Nested subqueries
SELECT * FROM customers c
WHERE id IN (
    SELECT customer_id FROM orders
    WHERE id IN (
        SELECT order_id FROM order_items
        WHERE product_id IN (
            SELECT id FROM products WHERE category = 'Electronics'
        )
    )
);

-- ============================================
-- 3. SUBQUERY DECORRELATION TESTS
-- ============================================

-- Test 3.1: Simple EXISTS to semi-join
-- Should transform to: customers SEMI JOIN orders ON ...
SELECT c.* FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.id 
    AND o.status = 'completed'
);

-- Test 3.2: NOT EXISTS to anti-join
-- Should transform to: customers ANTI JOIN orders ON ...
SELECT c.* FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.id 
    AND o.order_date > '2024-01-01'
);

-- Test 3.3: IN to semi-join with additional filters
-- Should push down the WHERE clause
SELECT * FROM products p
WHERE p.id IN (
    SELECT product_id 
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.id
    WHERE o.order_date > '2024-01-01'
    AND oi.quantity > 10
);

-- Test 3.4: Correlated aggregate subquery
-- Should decorrelate using window functions or lateral join
SELECT 
    e.name,
    e.salary,
    e.department_id
FROM employees e
WHERE e.salary > (
    SELECT AVG(e2.salary) * 1.2
    FROM employees e2
    WHERE e2.department_id = e.department_id
);

-- ============================================
-- 4. CTE TESTS
-- ============================================

-- Test 4.1: Simple CTE
WITH high_value_orders AS (
    SELECT * FROM orders WHERE total > 1000
)
SELECT * FROM high_value_orders;

-- Test 4.2: Multiple CTEs
WITH 
regional_sales AS (
    SELECT 
        region,
        SUM(total) as total_sales,
        COUNT(*) as order_count
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    GROUP BY region
),
top_regions AS (
    SELECT * FROM regional_sales 
    WHERE total_sales > 100000
)
SELECT * FROM top_regions ORDER BY total_sales DESC;

-- Test 4.3: CTE with predicate pushdown
-- The WHERE clause should be pushed into the CTE
WITH order_summary AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total) as total_spent
    FROM orders
    GROUP BY customer_id
)
SELECT * FROM order_summary
WHERE order_count > 5;

-- Test 4.4: CTE referenced multiple times
-- Should materialize once and reuse
WITH expensive_products AS (
    SELECT * FROM products WHERE price > 100
)
SELECT 
    (SELECT COUNT(*) FROM expensive_products) as total_count,
    (SELECT AVG(price) FROM expensive_products) as avg_price,
    (SELECT MAX(price) FROM expensive_products) as max_price;

-- Test 4.5: Nested CTEs
WITH 
base_data AS (
    SELECT * FROM orders WHERE order_date > '2024-01-01'
),
summary AS (
    SELECT 
        customer_id,
        COUNT(*) as recent_orders,
        SUM(total) as recent_total
    FROM base_data
    GROUP BY customer_id
)
SELECT c.*, s.recent_orders, s.recent_total
FROM customers c
JOIN summary s ON c.id = s.customer_id
WHERE s.recent_orders > 3;

-- Test 4.6: Recursive CTE (basic)
WITH RECURSIVE subordinates AS (
    -- Anchor: direct reports to CEO
    SELECT id, name, manager_id, 1 as level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive: reports of reports
    SELECT e.id, e.name, e.manager_id, s.level + 1
    FROM employees e
    JOIN subordinates s ON e.manager_id = s.id
)
SELECT * FROM subordinates ORDER BY level, name;

-- ============================================
-- 5. COMPLEX TRANSFORMATION TESTS
-- ============================================

-- Test 5.1: Combine all optimizations
WITH high_value_customers AS (
    SELECT customer_id
    FROM orders
    WHERE total > 1000
    GROUP BY customer_id
    HAVING COUNT(*) > 3
)
SELECT 
    c.name,
    c.email,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as total_orders
FROM customers c
WHERE c.id IN (SELECT * FROM high_value_customers)
AND EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.id 
    AND o.order_date > '2024-06-01'
);

-- Test 5.2: TPC-H Query 3 variant
SELECT 
    l.orderkey,
    SUM(l.extendedprice * (1 - l.discount)) as revenue,
    o.orderdate,
    o.shippriority
FROM customer c
JOIN orders o ON c.custkey = o.custkey
JOIN lineitem l ON l.orderkey = o.orderkey
WHERE 
    c.mktsegment = 'BUILDING'
    AND o.orderdate < '1995-03-15'
    AND l.shipdate > '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate
LIMIT 10;

-- ============================================
-- 6. PERFORMANCE VALIDATION QUERIES
-- ============================================

-- Before optimization: N+1 query pattern
-- After: Single query with semi-join
SELECT c.* FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.id
    AND o.total > (
        SELECT AVG(o2.total) 
        FROM orders o2 
        WHERE o2.customer_id = c.id
    )
);

-- Projection pushdown validation
-- Should only read necessary columns from large tables
EXPLAIN SELECT 
    t1.col1,
    t2.col99
FROM 
    hundred_column_table t1
    JOIN hundred_column_table t2 ON t1.id = t2.ref_id
WHERE t1.col1 = 'test';

-- ============================================
-- 7. NULL SEMANTICS TESTS
-- ============================================

-- Test NOT IN with NULL values
CREATE TABLE test_null (id INT, val INT);
INSERT INTO test_null VALUES (1, 10), (2, 20), (3, NULL);

-- Should return empty set due to NULL
SELECT * FROM test_null WHERE val NOT IN (SELECT val FROM test_null);

-- Should handle NULL correctly in EXISTS
SELECT * FROM test_null t1
WHERE EXISTS (
    SELECT 1 FROM test_null t2 
    WHERE t2.val = t1.val
);

-- ============================================
-- 8. ERROR CASES
-- ============================================

-- Should error: scalar subquery returns multiple rows
SELECT 
    name,
    (SELECT id FROM orders) as order_id
FROM customers;

-- Should error: subquery has too many columns
SELECT * FROM customers
WHERE id = (SELECT id, name FROM customers WHERE id = 1);

-- Should error: correlated column not in scope
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = non_existent_alias.id
);