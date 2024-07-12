CREATE SCHEMA std6_116; -- creating schema in Greenplum


-- creating dimension tables
CREATE TABLE std6_116.stores (
	plant bpchar(4),
	txt text
)
DISTRIBUTED REPLICATED;



CREATE TABLE std6_116.promos (
	promo_id varchar,
	promo_name varchar,
	promo_type int4,
	material int8,
	discount int4
)
DISTRIBUTED REPLICATED;



CREATE TABLE std6_116.promo_types (
	promo_type int4,
	txt text
)
DISTRIBUTED REPLICATED;



-- creating fact tables
CREATE TABLE std6_116.traffic (
	plant bpchar(4),
	date date,
	time bpchar(6),
	frame_id bpchar(10),
	quantity int4
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY
PARTITION BY range(date)
(
	start (date '2020-11-01') inclusive 
	end (date '2022-01-01') exclusive
	every (interval '1 month'),
	default partition def
);



CREATE TABLE std6_116.bills_head (
	billnum int8,
	plant bpchar(4),
	calday date
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED by (billnum)
PARTITION BY RANGE(calday)
(
	START (date '2020-11-01') INCLUSIVE 
	END (date '2021-03-01') EXCLUSIVE
	EVERY (interval '1 month'),
	DEFAULT PARTITION def
);



CREATE TABLE std6_116.bills_item (
	billnum int8,
	billitem int8,
	material int8,
	qty int8,
	netval numeric(17,2),
	tax numeric(17,2),
	rpa_sat numeric(17,2),
	calday date
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED by (billnum)
PARTITION BY RANGE(calday)
(
	START (date '2020-11-01') INCLUSIVE 
	END (date '2021-03-01') EXCLUSIVE
	EVERY (interval '1 month'),
	DEFAULT PARTITION def
);



CREATE TABLE std6_116.coupons (
	plant varchar
	date date,
	coupon_num varchar,
	promo_id varchar,
	material int8,
	billnum int8
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED by (billnum)
PARTITION BY RANGE(date)
(
	START (date '2020-11-01') INCLUSIVE 
	END (date '2021-03-01') EXCLUSIVE
	EVERY (interval '1 month'),
	DEFAULT PARTITION def
);



-- creating log table
CREATE TABLE std6_116.logs (
	log_id int8 PRIMARY KEY,
	log_timestamp timestamp DEFAULT now() NOT NULL,
	log_type text NOT NULL,
	log_msg text NOT NULL,
	log_location text NULL,
	is_error bool NULL,
	log_user text DEFAULT "current_user"() NULL
)
DISTRIBUTED BY (log_id);