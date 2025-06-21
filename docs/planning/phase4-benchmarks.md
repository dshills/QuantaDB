# Phase 4 Performance Benchmarks

## Overview

This document defines performance benchmarks to validate the effectiveness of Phase 4 query transformations. Each benchmark compares query execution before and after optimization.

## Benchmark Setup

### Test Data
```sql
-- Create test schema
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    region TEXT,
    created_at TIMESTAMP
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    total DECIMAL(10,2),
    status TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT,
    category_id INTEGER,
    price DECIMAL(10,2),
    description TEXT
);

-- Create indexes
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_products_category ON products(category_id);

-- Insert test data
-- 100K customers, 1M orders, 5M order items, 10K products
```

## Benchmarks

### 1. Projection Pushdown Benchmarks

#### Benchmark 1.1: Wide Table Join
```sql
-- Query: Select 2 columns from 100-column tables
SELECT t1.col1, t2.col99
FROM wide_table_1 t1
JOIN wide_table_2 t2 ON t1.id = t2.ref_id
WHERE t1.status = 'active';
```

**Expected Results**:
- Before: Read all 200 columns, high memory usage
- After: Read only 4 columns (col1, id, col99, ref_id)
- **Target**: 50x reduction in bytes read

#### Benchmark 1.2: Multi-Join Projection
```sql
-- Query: 5-way join selecting final columns
SELECT c.name, p.name, SUM(oi.quantity)
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
JOIN categories cat ON p.category_id = cat.id
WHERE cat.name = 'Electronics'
GROUP BY c.name, p.name;
```

**Expected Results**:
- Before: Full row width at each join
- After: Progressive column elimination
- **Target**: 70% memory reduction

### 2. Subquery Transformation Benchmarks

#### Benchmark 2.1: EXISTS to Semi-Join
```sql
-- Find customers with high-value orders
SELECT c.* FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.id 
    AND o.total > 1000
);
```

**Expected Results**:
- Before: N+1 queries (1 per customer)
- After: Single hash semi-join
- **Target**: 100x speedup for 100K customers

#### Benchmark 2.2: NOT IN to Anti-Join
```sql
-- Find customers without recent orders
SELECT c.* FROM customers c
WHERE c.id NOT IN (
    SELECT customer_id FROM orders 
    WHERE order_date > CURRENT_DATE - INTERVAL '30 days'
);
```

**Expected Results**:
- Before: Build full list, check each customer
- After: Hash anti-join with early termination
- **Target**: 50x speedup

#### Benchmark 2.3: Correlated Aggregate
```sql
-- Find above-average products per category
SELECT p.* FROM products p
WHERE p.price > (
    SELECT AVG(p2.price) * 1.2
    FROM products p2
    WHERE p2.category_id = p.category_id
);
```

**Expected Results**:
- Before: Subquery per product (10K executions)
- After: Window function or lateral join
- **Target**: 100x speedup

### 3. CTE Benchmarks

#### Benchmark 3.1: CTE Materialization
```sql
WITH expensive_calculation AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total) as total_spent,
        AVG(total) as avg_order
    FROM orders
    GROUP BY customer_id
    HAVING COUNT(*) > 10
)
SELECT 
    c.*,
    ec.order_count,
    ec.total_spent,
    ec.avg_order
FROM customers c
JOIN expensive_calculation ec ON c.id = ec.customer_id
WHERE ec.total_spent > 10000;
```

**Expected Results**:
- Materialize CTE once
- Reuse for join
- **Target**: Same as inline view

#### Benchmark 3.2: CTE Inlining
```sql
WITH simple_filter AS (
    SELECT * FROM orders WHERE status = 'completed'
)
SELECT customer_id, COUNT(*)
FROM simple_filter
GROUP BY customer_id;
```

**Expected Results**:
- Should inline CTE
- Single table scan
- **Target**: No overhead vs direct query

### 4. Complex Query Benchmarks

#### Benchmark 4.1: TPC-H Query 3
```sql
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
```

**Target**: Within 20% of PostgreSQL performance

#### Benchmark 4.2: Complex Nested Query
```sql
-- Customers who ordered above-average products from their favorite categories
WITH customer_categories AS (
    SELECT 
        o.customer_id,
        p.category_id,
        COUNT(*) as purchase_count
    FROM orders o
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    GROUP BY o.customer_id, p.category_id
),
favorite_categories AS (
    SELECT DISTINCT ON (customer_id)
        customer_id,
        category_id
    FROM customer_categories
    ORDER BY customer_id, purchase_count DESC
)
SELECT DISTINCT c.*
FROM customers c
JOIN favorite_categories fc ON c.id = fc.customer_id
WHERE EXISTS (
    SELECT 1 
    FROM orders o
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    WHERE o.customer_id = c.id
    AND p.category_id = fc.category_id
    AND p.price > (
        SELECT AVG(price) * 1.5 
        FROM products 
        WHERE category_id = fc.category_id
    )
);
```

**Expected Optimizations**:
1. CTE materialization for customer_categories
2. EXISTS to semi-join transformation
3. Correlated subquery decorrelation
4. Projection pushdown through all operations

**Target**: < 5 seconds on 1M orders

## Benchmark Execution

### Test Harness
```go
type Benchmark struct {
    Name     string
    Query    string
    Setup    []string
    Teardown []string
}

func RunBenchmark(b *Benchmark) BenchmarkResult {
    // 1. Run setup
    // 2. Warm up (3 runs)
    // 3. Measure 10 runs
    // 4. Calculate stats (mean, p50, p95, p99)
    // 5. Run teardown
    // 6. Return results
}
```

### Metrics to Collect
1. **Execution Time**
   - Total time
   - Planning time
   - Execution time

2. **Resource Usage**
   - Rows read from storage
   - Bytes read from storage
   - Memory peak usage
   - Temp disk usage

3. **Operator Statistics**
   - Rows processed per operator
   - Actual vs estimated rows
   - Cache hit rates

### Success Criteria

| Optimization | Target Improvement | Acceptable Range |
|--------------|-------------------|------------------|
| Projection Pushdown | 50% I/O reduction | 30-70% |
| EXISTS to Semi-Join | 100x speedup | 50-200x |
| NOT IN to Anti-Join | 50x speedup | 20-100x |
| Correlated Subquery | 100x speedup | 50-200x |
| CTE Optimization | No overhead | ±10% |
| TPC-H Queries | Within 20% of PostgreSQL | 0.8x - 1.2x |

## Regression Prevention

### Continuous Benchmarking
1. Run benchmarks on every PR
2. Flag regressions > 10%
3. Track trends over time

### Benchmark Suite
```bash
# Run all benchmarks
make benchmark-phase4

# Run specific category
make benchmark-projection
make benchmark-subquery
make benchmark-cte

# Generate report
make benchmark-report
```

## Notes

- Start with synthetic benchmarks for isolated testing
- Add real-world queries as they're identified
- Compare against PostgreSQL 15 as baseline
- Consider cloud vs local storage impact
- Account for cold vs warm cache behavior

---
*Created: December 21, 2024*  
*Part of Phase 4 Query Transformation Enhancements*