SELECT COUNT(*) AS part_count FROM part;
SELECT COUNT(*) AS lineitem_count FROM lineitem;
SELECT COUNT(*) AS promo_parts FROM part WHERE p_type LIKE 'PROMO%';