/* 
--- DROP STG + tables ---
drop schema if exists stg;
drop table if exists stg.restaurants;
drop table if exists stg.couriers;
drop table if exists stg.deliveries;
drop table if exists stg.settings;
*/

create schema if not exists stg;

create table if not exists stg.restaurants(
id serial,
object_value text not null,
update_ts timestamp not null
);

create table if not exists stg.couriers(
id serial,
object_value text not null,
update_ts timestamp not null
);

create table if not exists stg.deliveries(
id serial,
object_value text not null,
update_ts timestamp not null
);

CREATE TABLE if not exists stg.settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	workflow_key varchar,                          -- time_key
	workflow_settings text,                        -- table_name
	CONSTRAINT settings_pkey PRIMARY KEY (id)
);

