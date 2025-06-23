-- Test date insertion
INSERT INTO orders VALUES (
    1,                    -- o_orderkey
    1,                    -- o_custkey
    'O',                  -- o_orderstatus
    73422.45,            -- o_totalprice
    DATE '1993-04-09',   -- o_orderdate
    '3-MEDIUM',          -- o_orderpriority
    'Clerk#000000866',   -- o_clerk
    0,                   -- o_shippriority
    'test comment'       -- o_comment
);