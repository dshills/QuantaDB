-- Create all TPC-H tables from scratch
-- Drop existing tables first
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;

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
    s_acctbal FLOAT NOT NULL,
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
    p_retailprice FLOAT NOT NULL,
    p_comment TEXT
);

CREATE TABLE customer (
    c_custkey INTEGER NOT NULL,
    c_name TEXT NOT NULL,
    c_address TEXT NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone TEXT NOT NULL,
    c_acctbal FLOAT NOT NULL,
    c_mktsegment TEXT NOT NULL,
    c_comment TEXT
);

-- Large fact tables
CREATE TABLE partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost FLOAT NOT NULL,
    ps_comment TEXT
);

CREATE TABLE orders (
    o_orderkey INTEGER NOT NULL,
    o_custkey INTEGER NOT NULL,
    o_orderstatus TEXT NOT NULL,
    o_totalprice FLOAT NOT NULL,
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
    l_quantity FLOAT NOT NULL,
    l_extendedprice FLOAT NOT NULL,
    l_discount FLOAT NOT NULL,
    l_tax FLOAT NOT NULL,
    l_returnflag TEXT NOT NULL,
    l_linestatus TEXT NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct TEXT NOT NULL,
    l_shipmode TEXT NOT NULL,
    l_comment TEXT
);