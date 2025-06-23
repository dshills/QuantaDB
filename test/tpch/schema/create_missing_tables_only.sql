-- Create only missing tables
CREATE TABLE nation (
    n_nationkey INTEGER NOT NULL,
    n_name TEXT NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment TEXT
);

CREATE TABLE supplier (
    s_suppkey INTEGER NOT NULL,
    s_name TEXT NOT NULL,
    s_address TEXT NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone TEXT NOT NULL,
    s_acctbal FLOAT NOT NULL,
    s_comment TEXT
);

CREATE TABLE partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost FLOAT NOT NULL,
    ps_comment TEXT
);