-- TPC-H Schema for QuantaDB
-- Scale Factor will be specified during data generation

-- Small dimension tables
CREATE TABLE nation (
    n_nationkey INTEGER NOT NULL,
    n_name TEXT NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment TEXT
);

CREATE TABLE region (
    r_regionkey INTEGER NOT NULL,
    r_name TEXT NOT NULL,
    r_comment TEXT
);

-- Medium-sized tables
CREATE TABLE supplier (
    s_suppkey INTEGER NOT NULL,
    s_name TEXT NOT NULL,
    s_address TEXT NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone TEXT NOT NULL,
    s_acctbal DECIMAL(15,2) NOT NULL,
    s_comment TEXT
);

CREATE TABLE part (
    p_partkey INTEGER NOT NULL,
    p_name TEXT NOT NULL,
    p_mfgr TEXT NOT NULL,
    p_brand TEXT NOT NULL,
    p_type TEXT NOT NULL,
    p_size INTEGER NOT NULL,
    p_container TEXT NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment TEXT
);

CREATE TABLE customer (
    c_custkey INTEGER NOT NULL,
    c_name TEXT NOT NULL,
    c_address TEXT NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone TEXT NOT NULL,
    c_acctbal DECIMAL(15,2) NOT NULL,
    c_mktsegment TEXT NOT NULL,
    c_comment TEXT
);

-- Large fact tables
CREATE TABLE partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment TEXT
);

CREATE TABLE orders (
    o_orderkey INTEGER NOT NULL,
    o_custkey INTEGER NOT NULL,
    o_orderstatus TEXT NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority TEXT NOT NULL,
    o_clerk TEXT NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment TEXT
);

CREATE TABLE lineitem (
    l_orderkey INTEGER NOT NULL,
    l_partkey INTEGER NOT NULL,
    l_suppkey INTEGER NOT NULL,
    l_linenumber INTEGER NOT NULL,
    l_quantity DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount DECIMAL(15,2) NOT NULL,
    l_tax DECIMAL(15,2) NOT NULL,
    l_returnflag TEXT NOT NULL,
    l_linestatus TEXT NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct TEXT NOT NULL,
    l_shipmode TEXT NOT NULL,
    l_comment TEXT
);

-- Create indexes for common join patterns
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_partsupp_partkey ON partsupp(ps_partkey);
CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);