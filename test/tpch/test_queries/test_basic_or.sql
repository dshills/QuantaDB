-- Test basic OR condition
SELECT COUNT(*) FROM part WHERE p_brand = 'Brand#12' OR p_brand = 'Brand#23';
SELECT p_brand FROM part WHERE p_brand = 'Brand#12' OR p_brand = 'Brand#23' LIMIT 5;