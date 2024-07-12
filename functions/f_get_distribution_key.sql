CREATE OR REPLACE FUNCTION std6_116.f_get_distribution_key(p_table_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
DECLARE
  v_location 			text := 'std6_116.f_get_distribution_key';
  v_table_name 			text;
  v_dist_key 			text;
  v_table_oid 			int4;
 
BEGIN
     	v_table_name = std6_116.f_unify_name(p_table_name);
	
     	PERFORM std6_116.f_write_log(
     	p_log_type := 'SERVICE', 
     	p_log_message := 'START get distribution for table '||v_table_name, 
     	p_location    := v_location);
     
	SELECT c.oid
	INTO v_table_oid
	FROM pg_class AS c INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
	WHERE n.nspname||'.'||c.relname = v_table_name
	LIMIT 1; 
	
	IF v_table_oid = 0 OR v_table_oid IS NULL THEN
		v_dist_key = 'DISTRIBUTED RANDOMLY';
	ELSE
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	END IF;

	PERFORM std6_116.f_write_log(
     	p_log_type := 'SERVICE', 
     	p_log_message := 'END get distribution for table '||v_table_name || ' dist key: ' || v_dist_key,
     	p_location    := v_location);
	
	RETURN v_dist_key;
END;



$$
EXECUTE ON ANY;