CREATE OR REPLACE FUNCTION std6_116.f_switch_partition(p_table_name text, p_partition_value timestamp, p_switch_table_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
		
DECLARE
	v_location 		text := 'std6_116.f_switch_partition';
	v_table_to_name 	text;
  	v_switch_table_name 	text;
BEGIN
	v_table_to_name = f_unify_name(p_table_name);
	v_switch_table_name = f_unify_name(p_switch_table_name);

  PERFORM std6_116.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START switch partitions for date '|| p_partition_value ||' in table '||v_table_to_name, 
     p_location    := v_location); 

 EXECUTE 'ALTER TABLE '||v_table_to_name||' EXCHANGE PARTITION FOR (DATE '''||p_partition_value||''') WITH TABLE '||v_switch_table_name||' WITH VALIDATION';
	
  PERFORM std6_116.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'END switch partitions for date '|| p_partition_value ||' in table '||v_table_to_name, 
     p_location    := v_location);      

END;



$$
EXECUTE ON ANY;