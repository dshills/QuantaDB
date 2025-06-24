-- Create indexes for TPC-H query optimization
-- These indexes target the most common join columns and filter conditions

-- Customer indexes
CREATE INDEX idx_customer_mktsegment ON customer(c_mktsegment);
CREATE INDEX idx_customer_custkey ON customer(c_custkey);

-- Orders indexes  
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderkey ON orders(o_orderkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);

-- Lineitem indexes
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_lineitem_commitdate ON lineitem(l_commitdate);
CREATE INDEX idx_lineitem_receiptdate ON lineitem(l_receiptdate);

-- Part indexes
CREATE INDEX idx_part_partkey ON part(p_partkey);
CREATE INDEX idx_part_type ON part(p_type);
CREATE INDEX idx_part_brand ON part(p_brand);

-- Supplier indexes (if table exists)
-- CREATE INDEX idx_supplier_suppkey ON supplier(s_suppkey);
-- CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);

-- PartSupp indexes (if table exists)
-- CREATE INDEX idx_partsupp_partkey ON partsupp(ps_partkey);
-- CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);