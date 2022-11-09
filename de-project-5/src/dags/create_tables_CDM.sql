create schema if not exists cdm;

create table if not exists cdm.dm_courier_ledger(
	id serial PRIMARY KEY,
	courier_id varchar(100) not null,
	courier_name varchar(100) not null,
	settlement_year integer not null,
	settlement_month int4 not null,
	orders_count integer not null check (orders_count >= 0) default (0),
	order_total_sum numeric(14,2) not null check (order_total_sum >= 0) default (0),
	rate_avg float not null check (rate_avg >=0) default (0),
	order_processing_fee numeric(14,2) not null check (order_processing_fee >= 0) default (0),
	courier_order_sum numeric(14,2) not null check (courier_order_sum >= 0) default (0),
	courier_tips_sum numeric(14,2) not null check (courier_tips_sum >= 0) default (0),
	courier_reward_sum numeric(14,2) not null check (courier_reward_sum >= 0) default (0)
);

ALTER TABLE cdm.dm_courier_ledger DROP CONSTRAINT IF EXISTS date_check;
ALTER TABLE cdm.dm_courier_ledger DROP CONSTRAINT IF EXISTS dm_courier_ledger_unique;

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT date_check 
CHECK (
	settlement_year >= 2020 
	and settlement_year <= 2500
	and settlement_month >= 1 
	and settlement_month <= 12
);

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT dm_courier_ledger_unique
UNIQUE (courier_id, settlement_year, settlement_month);

CREATE TABLE if not exists cdm.settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	workflow_key varchar,                          -- time_key
	workflow_settings text,                        -- table_name
	CONSTRAINT settings_pkey PRIMARY KEY (id)
);