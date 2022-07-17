drop table if exists mart.f_customer_retention;
-- create mart for customer_retention
CREATE TABLE if not exists mart.f_customer_retention (
	id serial not null,
	new_customers_count int NULL,
	returning_customers_count int NULL,
	refunded_customer_count int NULL,
	period_name varchar (20) DEFAULT 'weekly'::character varying,
	period_id int null,
	item_id int NULL,
	new_customers_revenue numeric(10, 2) null,
	returning_customers_revenue numeric(10, 2) null,
	customers_refunded int null,
	CONSTRAINT f_customer_retention_pkey PRIMARY KEY (id)
);