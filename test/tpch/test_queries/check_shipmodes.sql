SELECT DISTINCT l_shipmode FROM lineitem LIMIT 10;
SELECT COUNT(*) FROM lineitem WHERE l_shipmode IN ('MAIL', 'SHIP');