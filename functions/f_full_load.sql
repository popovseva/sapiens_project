CREATE OR REPLACE FUNCTION std6_116.f_full_load(p_table_to_name text, p_file_name text, p_ip text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$



DECLARE
	v_location   	text := 'std6_116.f_full_load';
	v_ext_table  	text;
	v_schema_name 	text;
	v_table_to_name text;
	v_file		    text;
	v_ip			text;
	v_sql 		    text;
	v_gpfdist    	text;
	v_cnt 	 	    int8;
BEGIN
	v_schema_name := left(p_table_to_name,position('.' IN p_table_to_name)-1);
	v_table_to_name := right(p_table_to_name,length(p_table_to_name) - POSITION('.' IN p_table_to_name));
	v_ext_table = std6_116.f_unify_name(p_name := p_table_to_name)||'_ext';
	v_ip = p_ip;

	--logs
	PERFORM std6_116.f_write_log(p_log_type := 'INFO', p_log_message := 'START std6_116.f_full_load', p_location := v_location);
	v_gpfdist = 'GPFDIST://'|| p_ip ||'/'||p_file_name||'.CSV';
	RAISE NOTICE 'EXTERNAL TABLE NAME IS: %, TABLE LOCATION IS: %.', v_ext_table, v_gpfdist;

	--truncating target	table
	v_sql = 'TRUNCATE TABLE '||p_table_to_name||'';
	EXECUTE v_sql;

	-- creating external table
	v_ext_table = std6_116.f_create_ext_table3(p_source := v_table_to_name, p_ext_protocol := 'GPFDIST', p_ip := v_ip, p_schema_name := v_schema_name);

	EXECUTE 'INSERT INTO '||p_table_to_name||' SELECT * FROM '||v_ext_table||''; 
	GET DIAGNOSTICS v_cnt = ROW_COUNT;
	PERFORM std6_116.f_analyze_table(p_table_name := p_table_to_name);
	PERFORM std6_116.f_write_log(p_log_type := 'INFO', p_log_message := 'END std6_116.f_full_load', p_location :=  v_location);
	RAISE NOTICE '% rows inserted from % into %', v_cnt, p_file_name, p_table_to_name; 						
	RETURN v_cnt;
END;



$$
EXECUTE ON ANY;