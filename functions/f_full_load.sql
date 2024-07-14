CREATE OR REPLACE FUNCTION std6_116.f_full_load(p_table text, p_file_name text, p_ip text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

	

DECLARE
	v_location   	text := 'std6_116.f_full_load';
	v_ext_table  	text;
	v_file		    text;
	v_sql 		    text;
	v_gpfdist    	text;
	v_cnt 	 	    int8;
BEGIN
	
	PERFORM std6_116.f_write_log(p_log_type := 'INFO', p_log_message := 'START std6_116.f_full_load', p_location := v_location);

	v_ext_table = std6_116.f_unify_name(p_name := p_table)||'_ext';
	v_gpfdist = 'GPFDIST://'|| p_ip ||'/'||p_file_name||'.CSV';
	RAISE NOTICE 'EXTERNAL TABLE NAME IS: %, TABLE LOCATION IS: %.', v_ext_table, v_gpfdist;
	
	v_sql = 'TRUNCATE TABLE '||p_table||'; 
		DROP EXTERNAL TABLE IF EXISTS '||v_ext_table||';
		CREATE EXTERNAL TABLE '||v_ext_table|| '(LIKE '||p_table||') 
		LOCATION('''||v_gpfdist||'''
		) ON ALL
		FORMAT ''CSV'' (DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' HEADER)
		ENCODING ''UTF8'''; 
	
	EXECUTE v_sql;

	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table||''; 
	GET DIAGNOSTICS v_cnt = ROW_COUNT;
	PERFORM std6_116.f_analyze_table(p_table_name := p_table);
	PERFORM std6_116.f_write_log(p_log_type := 'INFO', p_log_message := 'END std6_116.f_full_load', p_location :=  v_location);
	RAISE NOTICE '% rows inserted from % into %', v_cnt, p_file_name, p_table; 						
	RETURN v_cnt;
END;



$$
EXECUTE ON ANY;