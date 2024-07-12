CREATE OR REPLACE FUNCTION std6_116.f_write_log(p_log_type text, p_log_message text, p_location text)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
	
DECLARE

	v_log_type TEXT;
	v_log_message TEXT;
	v_sql TEXT;
	v_location TEXT;
	v_res TEXT;
	
BEGIN

	-- Checking log type
	v_log_type = UPPER(p_log_type);
	v_location = LOWER(p_location);
	IF v_log_type NOT IN ('ERROR', 'INFO', 'WARN', 'DEBUG','SERVICE') THEN
		RAISE EXCEPTION 'Illegal log type! Use one of: INFO, WARN, ERROR, DEBUG, SERVICE';
	END IF;
	
	RAISE NOTICE '%: %: <%> Location[%]', CLOCK_TIMESTAMP(), v_log_type, p_log_message, v_location;
	
	-- Processing the message passed to the function
	v_log_message := REPLACE(p_log_message, '''', '''''');
	
	-- Adding logs to the table
	v_sql = 'INSERT INTO std6_116.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user)
			 VALUES ( '|| NEXTVAL('std6_116.log_id_seq')|| ' ,
					  ''' || v_log_type || ''',
					  ' || COALESCE('''' || v_log_message || '''', '''empty''')|| ',
					  ' || COALESCE('''' || v_location || '''', 'null')|| ',
					  ' || CASE WHEN v_log_type = 'ERROR' THEN TRUE ELSE FALSE END || ',
						   CURRENT_TIMESTAMP, CURRENT_USER);';		
	v_res := dblink('adb_server', v_sql); -- remotely executing a query with inserting records into the log table using DBLINK
END;




$$
EXECUTE ON ANY;