CREATE OR REPLACE FUNCTION std6_116.f_insert_table(p_table_to text, p_table_from text, p_where text DEFAULT NULL::text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
DECLARE
    v_where text;
    v_cnt int8;
   
BEGIN
    PERFORM std6_116.f_write_log('INFO','START f_insert_table',p_table_to);
    v_where = coalesce(p_where,' 1 = 1 ');
    
   EXECUTE 'INSERT INTO '||p_table_to||' SELECT * FROM '||p_table_from || ' WHERE '||v_where;

   GET DIAGNOSTICS v_cnt = ROW_COUNT;
   RAISE NOTICE '% rows inserted from % into %',v_cnt, p_table_from,p_table_to;
   PERFORM std6_116.f_write_log('INFO','END f_insert_table',p_table_to);
   RETURN v_cnt;
END;



$$
EXECUTE ON ANY;