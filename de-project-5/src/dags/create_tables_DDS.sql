create schema if not exists dds;

create table if not exists dds.restaurants(
	id serial PRIMARY KEY,
	restaurant_id varchar(100) UNIQUE not null,
	restaurant_name varchar(100) not null
);

create table if not exists dds.couriers(
	id serial PRIMARY KEY,
	courier_id varchar(100) UNIQUE not null,
	courier_name varchar(100) not null
);

create table if not exists dds.orders(
	id serial PRIMARY KEY,
	order_id varchar(100) unique not null,
	order_ts timestamp not null,
	restaurant_id int default (1),
	sum numeric(14,2) not null default 0 check (sum >= 0)
);


create table if not exists dds.deliveries(
	id serial PRIMARY KEY,
	delivery_id varchar(100) unique not null,
	courier_id int,
	order_id int,
	address varchar (255) not null,
	delivery_ts timestamp,
	rate int4 not null check (rate >= 1 and rate <= 5),
	tip_sum numeric(14,2) not null default 0 check (tip_sum >= 0)
);

create table if not exists dds.timestamps
(
	id serial primary key,
	ts timestamp unique not null,
	year smallint not null CONSTRAINT dm_timestamps_year_check 
		CHECK ((year >= 2022) and (year < 2500)),
	month smallint not null CONSTRAINT dm_timestamps_month_check 
		CHECK ((month >= 1) and (month <= 12)),
	day smallint not null CONSTRAINT dm_timestamps_day_check 
		CHECK ((day >= 1) and (day <= 31)),
	time time not null,
	date date not null
);

CREATE TABLE if not exists dds.settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	workflow_key varchar,                          -- time_key
	workflow_settings text,                        -- table_name
	CONSTRAINT settings_pkey PRIMARY KEY (id)
);

ALTER TABLE dds.deliveries  DROP CONSTRAINT IF EXISTS deliveries_courier_id_fkey;
ALTER TABLE dds.deliveries  DROP CONSTRAINT IF EXISTS deliveries_timestamps_id_fkey;
ALTER TABLE dds.deliveries  DROP CONSTRAINT IF EXISTS deliveries_orders_id_fkey;
ALTER TABLE dds.orders  DROP CONSTRAINT IF EXISTS orders_timestamps_id_fkey;
ALTER TABLE dds.orders  DROP CONSTRAINT IF EXISTS orders_restaurants_id_fkey;

ALTER TABLE dds.deliveries 
ADD CONSTRAINT deliveries_courier_id_fkey 
FOREIGN KEY (courier_id) 
REFERENCES dds.couriers(id);

ALTER TABLE dds.deliveries  
ADD CONSTRAINT deliveries_timestamps_id_fkey
FOREIGN KEY (delivery_ts) 
REFERENCES dds.timestamps (ts);

ALTER TABLE dds.deliveries  
ADD CONSTRAINT deliveries_orders_id_fkey 
FOREIGN KEY (order_id) 
REFERENCES dds.orders (id);

ALTER TABLE dds.orders  
ADD CONSTRAINT orders_timestamps_id_fkey
FOREIGN KEY (order_ts) 
REFERENCES dds.timestamps (ts);

ALTER TABLE dds.orders  
ADD CONSTRAINT orders_restaurants_id_fkey 
FOREIGN KEY (restaurant_id) 
REFERENCES dds.restaurants (id);

--TRUNCATE dds.restaurants CASCADE;
--TRUNCATE dds.couriers CASCADE;
--TRUNCATE dds.timestamps CASCADE;
--TRUNCATE dds.orders CASCADE;
--TRUNCATE dds.deliveries CASCADE;
--TRUNCATE dds.settings CASCADE;