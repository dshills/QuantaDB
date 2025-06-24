CREATE TABLE partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DECIMAL(10,2) NOT NULL,
    ps_comment TEXT,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);