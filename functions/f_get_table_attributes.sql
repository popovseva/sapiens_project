CREATE OR REPLACE FUNCTION std6_116.f_get_table_attributes(p_table_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	

DECLARE
  v_location text := 'std6_116.f_get_table_attributes';
  v_params   text;
  
BEGIN
	SELECT coalesce('with (' || array_to_string(reloptions, ', ') || ')','')
	FROM pg_class  
	INTO v_params
	WHERE oid = std6_116.f_unify_name(p_name := p_table_name)::regclass;
	RETURN v_params;
END;



$$
EXECUTE ON ANY;