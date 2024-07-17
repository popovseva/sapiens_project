-- DROP FUNCTION std6_116.f_load_delta_partitions2(text, text, text, timestamp, timestamp, text, text, text, text);

CREATE OR REPLACE FUNCTION std6_116.f_load_delta_partitions(p_table_to_name text, p_source text, p_partition_key text, p_start_date timestamp, p_end_date timestamp, ext_protocol text, p_ip text, p_user text DEFAULT NULL::text, p_pass text DEFAULT NULL::text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
		
DECLARE
	v_location	 			text := 'std6_116.f_load_delta_partitions';
  	v_table_to_name         text;
	v_temp_table			text;
	v_ext_table 			text;
	v_table_oid 			int4;
	v_dist_key				text;
	v_params				text;
	v_cnt					int8;
	v_load_interval 		interval;
	v_start_date 			date;
	v_end_date 				date;
	v_iter_date				date;
	v_where					text;
	v_cnt_prt				int8;
	v_partition_start_sql 	text;
	v_partition_start 		date;
	v_schema_name 			text;
BEGIN 

	v_schema_name := left(p_table_to_name,position('.' IN p_table_to_name)-1); -- target table schema name
	v_table_to_name := right(p_table_to_name,length(p_table_to_name) - POSITION('.' IN p_table_to_name));
	v_temp_table := v_table_to_name||'_tmp'; 

	-- checking if start date is lesser than start date of the first partition
  SELECT  partitionrangestart
  INTO v_partition_start_sql
  FROM (
		SELECT p.*, rank() OVER (ORDER BY partitionrank DESC) rnk
		FROM pg_partitions p
		WHERE p.partitionrank IS NOT NULL AND p.schemaname||'.'||p.tablename = lower(v_schema_name || '.' || v_table_to_name)
  ) q
  ORDER BY rnk DESC LIMIT 1;
 
 	EXECUTE 'SELECT '||v_partition_start_sql INTO v_partition_start;

 	IF p_start_date < v_partition_start THEN
  	RAISE EXCEPTION 'Start date cannot be lesser than start date of first partition';
 	END IF;

	-- logs
  PERFORM std6_116.f_write_log(
  	p_log_type     := 'INFO', 
	p_log_message  := 'START std6_116.f_load_delta_partitions', 
	p_location 	   :=  v_location);
  
	-- creating external table
	v_ext_table = std6_116.f_create_ext_table(p_source, ext_protocol, p_ip, p_user, p_pass);
	
	-- cutting new partitions from default if they don't exist
 	PERFORM std6_116.f_create_date_partitions(
  	p_table_name      := v_schema_name || '.' || v_table_to_name, 
  	p_partition_value := p_end_date::timestamp
  );
 
 	-- getting table oid and distribution key
	 SELECT std6_116.f_get_distribution_key(v_schema_name || '.' || v_table_to_name) INTO v_dist_key;

	-- parameters of the target table 
	SELECT coalesce('with ('||array_to_string(reloptions, ', ')||')','')
	FROM pg_class INTO v_params
	WHERE oid = v_table_to_name::regclass;
	

	v_load_interval = '1 month'::interval;
	v_start_date = date_trunc('month', p_start_date);
	v_end_date = date_trunc('month', p_end_date);
	v_iter_date = v_start_date;
	v_cnt = 0;
	
	WHILE v_iter_date <= v_end_date LOOP
		
	-- creating delta table for inserting the interval; 
	  v_temp_table = std6_116.f_create_tmp_table( 
	  p_table_name  := p_table_to_name, 
      p_prefix_name := 'prt_', 
      p_suffix_name := '_tmp_'||to_char(v_iter_date,'YYYYMM')
    );
   

	 IF p_source = 'traffic' THEN
	 
	 	v_where = 'to_date ( ' || p_partition_key ||', ''DD.MM.YYYY'' ) >= '''||v_iter_date||'''::date and to_date ( ' || p_partition_key ||', ''DD.MM.YYYY'' ) < '''||v_iter_date + v_load_interval||'''::date';

	 	EXECUTE 'INSERT INTO '||v_temp_table||'
				  SELECT plant, to_date(date, ''DD.MM.YYYY'') date, time, frame_id, quantity 
				  FROM '||v_ext_table||' 
				  WHERE '||v_where;
				 
		
	 ELSE
	 
	 	v_where = p_partition_key ||'::date >= '''||v_iter_date||'''::date and '||p_partition_key||'::date < '''||v_iter_date + v_load_interval||'''::date';
      	
	 	PERFORM std6_116.f_insert_table(p_table_to := v_temp_table, p_table_from := v_ext_table, p_where := v_where);
     
     END IF;
     
      GET DIAGNOSTICS v_cnt_prt = ROW_COUNT;
	  RAISE NOTICE 'Iteration number: % Inserted rows: %',v_cnt, v_cnt_prt;

    PERFORM std6_116.f_switch_partition(
    	p_table_name        := v_table_to_name,
    	p_partition_value   := v_iter_date,
    	p_switch_table_name := v_temp_table
    );
 
		v_cnt := v_cnt + v_cnt_prt;
		v_iter_date = v_iter_date + v_load_interval;
	
		EXECUTE 'DROP TABLE IF EXISTS '||v_temp_table;
	END LOOP;

  PERFORM std6_116.f_analyze_table(p_table_name := v_table_to_name);

  
  PERFORM std6_116.f_write_log(
  p_log_type    := 'INFO', 
  p_log_message  := 'END std6_116.f_load_delta_partitions', 
  p_location 	   :=  v_location);

 
  RETURN v_cnt;
END;




$$
EXECUTE ON ANY;