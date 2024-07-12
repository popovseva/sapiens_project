CREATE OR REPLACE FUNCTION std6_116.f_unify_name(p_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
BEGIN  
	RETURN lower(trim(translate(p_name, ';/''','')));
END;


$$
EXECUTE ON ANY;