CREATE OR REPLACE FUNCTION std6_116.f_load_mart_proj(p_start_date text, p_end_date text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
	
DECLARE
  	v_location 			text := 'std6_116.f_load_mart_proj';
	v_cnt 				int8;
	v_sql 				text;
	v_table_name 		text;
	v_name_start		text;
	v_name_end			text;
	v_view_name 		text;
	v_table_oid 		text;
	v_ext_table 		text;
	v_temp_table 		text;
	v_params 			text;
	v_start_date 		date;
	v_end_date 			timestamp;
	v_where 			text;
	v_load_interval 	interval;

BEGIN

    -- logs
	PERFORM std6_116.f_write_log(
			p_log_type    := 'INFO', 
			p_log_message := 'Start f_load_mart_proj', 
			p_location    := v_location);
		
	v_load_interval = '1 month'::interval;
	v_name_start = REPLACE(p_start_date, '-', '');
	v_name_end = REPLACE(p_end_date, '-', '');
	v_table_name = 'std6_116.mart_'||v_name_start||'_'||v_name_end;
		

	RAISE NOTICE 'MART TABLE IS: %', v_table_name;
	EXECUTE 'DROP TABLE IF EXISTS '||v_table_name||' CASCADE';

	v_start_date = TO_DATE(p_start_date, 'YYYY-MM-DD');
	v_end_date = TO_DATE(p_end_date, 'YYYY-MM-DD');
	v_where  = 'BETWEEN '''||v_start_date||''' AND '''||v_end_date||'''';

-- querry for creating mart 
	v_sql = 'CREATE TABLE '||v_table_name||' 
			 WITH(appendonly = TRUE,
				  orientation = COLUMN,
				  compresstype = zstd,
				  compresslevel = 1)
			AS (
			WITH daily_bills_mart as (
				SELECT plant, calday AS date,  
			    	SUM(rpa_sat) as daily_turnover,
			       	SUM(qty) AS daily_qty,
			       	COUNT(DISTINCT billnum)  AS daily_bills,
			       	ROUND(SUM(qty) / COUNT(DISTINCT billnum), 2) AS daily_avg_sold_qty,
			       	ROUND(SUM(rpa_sat) / COUNT(DISTINCT billnum), 1) as daily_mean_bill
			  	FROM (SELECT bi.billnum, bi.billitem, bi.material, bh.plant, bi.calday, bi.qty, bi.rpa_sat, bi.netval, bi.tax
			       	  FROM std6_116.bills_head bh JOIN std6_116.bills_item bi USING(billnum)
			       	) 
			       t1 GROUP BY plant, date),
			daily_coupons_mart AS (
			    SELECT plant, date, SUM(single_coupon_discount) AS daily_coupon_discounts,
			           COUNT(distinct coupon_num) AS daily_items_on_discount 
			    FROM (
			        SELECT c.plant, c.date, c.billnum, c.coupon_num, c.promo_id, p.promo_type, c.material, p.discount, 
			               CASE 
			                   WHEN p.promo_type = 1 THEN p.discount
			                   WHEN p.promo_type = 2 THEN (p.discount::DECIMAL / 100) * (bi.rpa_sat::DECIMAL / bi.qty)
			               END AS single_coupon_discount,
			               ROW_NUMBER() OVER(PARTITION BY c.coupon_num ORDER BY c.coupon_num) AS rn
			        FROM std6_116.coupons c 
			        JOIN std6_116.bills_item bi USING(billnum, material) 
			        JOIN std6_116.promos p USING(promo_id, material)
			    ) t2 
			    WHERE rn = 1
			    group by plant, date 
			),
			daily_traffic_mart AS (
			SELECT plant, date, SUM(quantity) as daily_traffic 
			FROM std6_116.traffic
			GROUP BY plant, date)
			SELECT dtm.plant, st.txt, dtm.date,
			TO_CHAR(dtm.date, ''YYYY-MM'') AS month,
			COALESCE(daily_turnover, 0) AS daily_turnover,
			COALESCE(daily_coupon_discounts, 0) AS daily_coupon_discounts,
			COALESCE(daily_turnover, 0) - COALESCE(daily_coupon_discounts, 0) as daily_turnover_with_discounts,
			COALESCE(daily_qty, 0) as daily_qty,
			COALESCE(daily_bills, 0) as daily_bills,
			COALESCE(daily_traffic,0) as daily_traffic,
			COALESCE(daily_items_on_discount, 0) as daily_items_on_discount,
			COALESCE(ROUND(daily_items_on_discount::decimal / daily_qty, 3), 0) AS daily_share_discount_items,
			COALESCE(ROUND(daily_qty::decimal / daily_bills, 2), 0) as daily_avg_sold_qty,
			COALESCE(ROUND(daily_bills::decimal / daily_traffic, 4), 0) as daily_conversion_coeff, 
			COALESCE(ROUND(daily_turnover::decimal / daily_bills, 1), 0) as daily_avg_bill,
			COALESCE(ROUND(daily_turnover::decimal / daily_traffic, 1), 0) as daily_avg_turnover_per_client
			FROM daily_bills_mart dbm left join daily_coupons_mart dcm using (plant, date) right join daily_traffic_mart dtm using(plant, date) left join stores st using(plant)
			WHERE DATE  '||v_where||')
			DISTRIBUTED RANDOMLY';

		EXECUTE v_sql;
		GET DIAGNOSTICS v_cnt = ROW_COUNT;
		RAISE NOTICE '% rows inserted rows in table %', v_table_name, v_cnt;
	
 
	
  PERFORM std6_116.f_analyze_table(p_table_name := v_table_name);
   
  PERFORM std6_116.f_write_log(
		  p_log_type    := 'INFO', 
		  p_log_message := 'End f_load_mart_proj', 
		  p_location    := v_location);
	RETURN v_cnt;
END;




$$
EXECUTE ON ANY;