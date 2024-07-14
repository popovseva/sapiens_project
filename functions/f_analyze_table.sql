CREATE OR REPLACE FUNCTION std6_116.f_analyze_table(p_table_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
DECLARE
  v_location       text := 'std6_116.f_analyze_table';
  v_table_name     text;
  v_sql            text;
BEGIN
	v_table_name := std6_116.f_unify_name(p_name := p_table_name);
	PERFORM std6_116.f_write_log(
  	p_log_type := 'SERVICE', 
    	p_log_message := 'START analyze table '||v_table_name, 
    	p_location    := v_location);

  	v_sql := 'ANALYZE '||v_table_name;
 	EXECUTE v_sql;
 
  perform std6_116.f_write_log(
     p_log_type := 'SERVICE', 
     p_log_message := 'END analyze table '||v_table_name, 
     p_location    := v_location); 
END



$$
EXECUTE ON ANY;