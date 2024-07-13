CREATE OR REPLACE FUNCTION std6_116.f_create_tmp_table(p_table_name text, p_prefix_name text DEFAULT NULL::text, p_suffix_name text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	

DECLARE
    v_location 			    text := 'std6_116.f_create_tmp_table';
    v_table_name    		text;
    v_tmp_t_name    		text;
    v_storage_param 		text;
    v_sql           		text;
    v_suffix_name   		text;
    v_prefix_name   		text;
    v_schema_name   		text;
    v_dist_key      		text;
    v_full_table_name 		text;
    
BEGIN
    v_suffix_name = coalesce(p_suffix_name,'');	-- _2021-07-01
    v_prefix_name = coalesce(p_prefix_name,''); -- prt_
   
    v_table_name  = std6_116.f_unify_name(p_name := p_table_name);
    v_full_table_name  = std6_116.f_unify_name(p_name := v_table_name);
   
    v_schema_name = left(v_full_table_name,position('.' in v_full_table_name)-1);
    v_table_name =  right(v_full_table_name,length(v_full_table_name) - POSITION('.' in v_full_table_name));

    v_tmp_t_name = v_schema_name||'.'||v_prefix_name||v_table_name||v_suffix_name;
   
    v_tmp_t_name    = std6_116.f_unify_name(p_name := v_tmp_t_name);
    v_storage_param = std6_116.f_get_table_attributes(p_table_name);
    v_dist_key = std6_116.f_get_distribution_key(p_table_name);
   
     PERFORM std6_116.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START Creating temp table '||v_tmp_t_name||' for table '||v_full_table_name,
     p_location    := v_location);
   
     v_sql := 
	     'DROP TABLE IF EXISTS ' || v_tmp_t_name || ';'
	      || 'CREATE TABLE ' || v_tmp_t_name || ' (like ' || v_full_table_name || ') ' || v_storage_param||' '||v_dist_key||';';
  
    EXECUTE v_sql;
   
     PERFORM std6_116.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END Creating temp table '||v_tmp_t_name||' for table '||v_full_table_name,
     p_location    := v_location);
   
   RETURN v_tmp_t_name;
END;




$$
EXECUTE ON ANY;