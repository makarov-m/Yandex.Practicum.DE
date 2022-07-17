-- rollback script
DELETE FROM mart.f_sales;
DELETE FROM mart.d_item;
DELETE FROM mart.d_city;
DELETE FROM mart.d_customer;
DELETE FROM staging.user_order_log ;

DROP table if exists staging.customer_research;
DROP table if exists staging.user_order_log;
DROP table if exists staging.user_orders_log;
DROP table if exists staging.user_activity_log;
DROP table if exists staging.price_log;
DROP table if exists mart.f_customer_retention;


-- rallback column
ALTER TABLE mart.f_sales DROP COLUMN status;

-- create table back
CREATE TABLE staging.user_order_log (
	id serial4 NOT NULL,
	date_time timestamp NOT NULL,
	city_id int4 NOT NULL,
	city_name varchar(100) NULL,
	customer_id int4 NOT NULL,
	first_name varchar(100) NULL,
	last_name varchar(100) NULL,
	item_id int4 NOT NULL,
	item_name varchar(100) NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	CONSTRAINT user_order_log_pkey PRIMARY KEY (id)
);
CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);
CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);