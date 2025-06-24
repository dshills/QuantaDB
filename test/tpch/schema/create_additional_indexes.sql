-- Additional indexes for newly added tables and queries

-- Supplier indexes
CREATE INDEX idx_supplier_suppkey ON supplier(s_suppkey);
CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);

-- Nation indexes
CREATE INDEX idx_nation_nationkey ON nation(n_nationkey);
CREATE INDEX idx_nation_regionkey ON nation(n_regionkey);
CREATE INDEX idx_nation_name ON nation(n_name);

-- Region indexes
CREATE INDEX idx_region_regionkey ON region(r_regionkey);
CREATE INDEX idx_region_name ON region(r_name);

-- PartSupp indexes
CREATE INDEX idx_partsupp_partkey ON partsupp(ps_partkey);
CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);

-- Additional Part indexes
CREATE INDEX idx_part_name ON part(p_name);
CREATE INDEX idx_part_size ON part(p_size);

-- Customer indexes
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);