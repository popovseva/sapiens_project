CREATE OR REPLACE FUNCTION std6_116.f_create_date_partitions(p_table_name text, p_partition_value timestamp)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	

DECLARE
	v_location             text := 'std6_116.f_create_date_partitions';
	v_cnt_partitions       int;
	v_table                text;
	v_error                text;
	v_partition            text;
	v_partition_end_sql    text;
	v_partition_end        timestamp;
	v_partition_delta_sql  text;
	v_partition_delta      interval;
	v_ts_format            text := 'YYYY-MM-DD HH24:MI:SS';
	v_interval             interval;
BEGIN

  -- unifying table name
  v_table = std6_116.f_unify_name(p_table_name);

  PERFORM std6_116.f_write_log(
     p_log_type    := 'INFO', 
     p_log_message := 'START Creating partitions for table '||v_table, 
     p_location    := v_location);

    
  -- checking the presence of partitions in the target table
  SELECT count(*)
  INTO v_cnt_partitions
  FROM pg_partitions p
  WHERE p.schemaname||'.'||p.tablename = lower(v_table);
  
 RAISE NOTICE 'Partitions count: %', v_cnt_partitions;
 
 -- if target table is partitioned:
  If v_cnt_partitions > 1 THEN
    LOOP
      -- getting the parameters of the last partition - the last date of this partition and the partitioning interval
      SELECT  partitionrangeend,  partitionrangeend||'::timestamp-'||partitionrangestart||'::timestamp'
      INTO v_partition_end_sql, v_partition_delta_sql
          FROM (
              SELECT p.*, rank() OVER (ORDER BY partitionrank DESC) rnk
              FROM pg_partitions p
              WHERE p.partitionrank IS NOT NULL
              AND   p.schemaname||'.'||p.tablename = lower(v_table)
              ) q
          WHERE rnk = 1;

      -- date of the last partition
      EXECUTE 'SELECT '||v_partition_end_sql INTO v_partition_end;
      -- partitioning interval
      EXECUTE 'SELECT '||v_partition_delta_sql INTO v_partition_delta;

      -- writing logs 
      PERFORM std6_116.f_write_log(
         p_log_type    := 'INFO', 
         p_log_message := 'v_partition_end:{'||v_partition_end||'}', 
         p_location    := v_location);
      PERFORM std6_116.f_write_log(
         p_log_type    := 'INFO', 
         p_log_message := 'v_partition_delta:{'||v_partition_delta||'}', 
         p_location    := v_location);

      -- exiting the function if date of the last partition > date in parameter
      EXIT WHEN v_partition_end > p_partition_value;

      -- defining the partitioning interval
      IF v_partition_delta between '28 days'::interval and '31 days'::interval THEN
        v_interval := '1 month'::interval;
        v_partition = 'm_'||to_char(v_partition_end,'mm_yyyy');
      ELSIF v_partition_delta < '28 days'::interval THEN
        v_interval := '1 day'::interval;
        v_partition = 'd_'||to_char(v_partition_end,'dd_mm_yyyy');
      ELSIF v_partition_delta > '32 days'::interval THEN
        v_interval := '1 year'::interval;
        v_partition = 'y_'||to_char(v_partition_end,'yyyy');
      ELSE
        v_error := 'Unable to define partition interval ';
        RAISE EXCEPTION '% for table % partition %',v_error, v_table,v_partition_end_sql;
      END IF;

      -- adding the partition
      EXECUTE 'ALTER TABLE '||v_table||' 
	       SPLIT DEFAULT PARTITION START ('||v_partition_end_sql||') 
	       END ('''||to_char(v_partition_end+v_interval, v_ts_format)||'''::timestamp)
      	       INTO (PARTITION '||v_partition||', default partition)';
    END LOOP;
  ELSE
      PERFORM std6_116.f_write_log(
         p_log_type    := 'WARN', 
         p_log_message := 'Table ' || v_table || ' is not partitioned ', 
         p_location    :=  v_location);
  END IF;

  PERFORM std6_116.f_write_log(
     p_log_type    := 'INFO', 
     p_log_message := 'END Creating partitions for table '||v_table, 
     p_location    :=  v_location);

END;



$$
EXECUTE ON ANY;