CREATE OR REPLACE FUNCTION std6_116.f_create_ext_table(p_source text, p_ext_protocol text, p_ip text, p_user text DEFAULT NULL::text, p_pass text DEFAULT NULL::text, p_schema_name text DEFAULT 'std6_116'::text)
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
-- p_source - CSV file or PostgreSQL table
DECLARE 
	v_location 			text := 'std6_116.f_create_ext_table';
	v_ext_table 		text;
	v_schema_name 		text;
	v_sql 				text;
	v_conn_params 		text;
	v_ext_protocol 		text;
	v_table 			text;
BEGIN
	v_ext_table = p_source || '_ext';
	v_table := p_source;
	v_schema_name := p_schema_name;
	v_ext_protocol := UPPER(p_ext_protocol);

  PERFORM std6_116.f_write_log(
  	p_log_type    := 'INFO',  
  	p_log_message := 'Start creating external table '||v_ext_table,
  	p_location    := v_location);

	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '|| v_schema_name || '.' || v_ext_table;

	IF v_ext_protocol = 'PXF' THEN 
		v_conn_params := 
			' LOCATION (''pxf://gp.'||p_source ||
			'?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver' || 
			'&DB_URL=jdbc:postgresql://'|| p_ip ||'/postgres' || 
			'&USER='|| p_user ||'&PASS='|| p_pass ||''') ON ALL ' ||
			' FORMAT ''CUSTOM'' (FORMATTER = ''pxfwritable_import'')' ||
			' ENCODING ''UTF8'' ;'; 	
	ELSIF v_ext_protocol = 'GPFDIST' THEN 
		v_conn_params := 
		' LOCATION (''gpfdist://'|| p_ip ||'/' || p_source || '.CSV'') ON ALL' ||
	    	' FORMAT ''CSV'' ( DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' HEADER )' ||
	    	' ENCODING ''UTF8'' ' ||
	    	' SEGMENT REJECT LIMIT 10 ROWS;';
	ELSE 
	  PERFORM std6_116.f_write_log(
	     p_log_type    := 'ERROR', 
	     p_log_message := 'External protocol is not supported',
	     p_location    := v_location);
		RAISE NOTICE 'External protocol is not supported';
	END IF;	

	PERFORM std6_116.f_write_log(
		p_log_type    := 'DEBUG', 
		p_log_message := 'v_conn_params:  ' || v_conn_params,
		p_location    := v_location);

	-- creating external table
	IF p_source = 'traffic' THEN
		v_sql = 
			'CREATE EXTERNAL TABLE ' || v_schema_name || '.' || v_ext_table ||
			' (plant bpchar(4), date bpchar(10), time bpchar(6), frame_id bpchar(10), quantity int4)' || v_conn_params;
		EXECUTE v_sql;
	ELSE
		v_sql = 
			'CREATE EXTERNAL TABLE ' || v_schema_name || '.'  || v_ext_table ||
			' (LIKE ' || v_schema_name || '.'  || v_table || ')' || v_conn_params;
		EXECUTE v_sql;
	END IF;


  PERFORM std6_116.f_write_log(
  	p_log_type    := 'INFO',  
  	p_log_message := 'END creating external table '||v_ext_table,
  	p_location    := v_location);

	RETURN v_ext_table;

END;



$$
EXECUTE ON ANY;