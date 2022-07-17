-- drop tables
drop table if exists public.shipping_info;
drop table if exists public.shipping_country_rates;
drop table if exists public.shipping_agreement;
drop table if exists public.shipping_transfer;
drop table if exists public.shipping_status;


-- table shipping_country_rates
create table public.shipping_country_rates (
	shipping_country_id serial,
	shipping_country text,
	shipping_country_base_rate numeric(2,2),
	primary key (shipping_country_id)
);
CREATE INDEX shipping_country_index ON public.shipping_country_rates(shipping_country_id);

-- table shipping_agreement
create table public.shipping_agreement (
	agreementid int,
	agreement_number text,
	agreement_rate numeric(2,2),
	agreement_commission numeric(2,2),
	primary key (agreementid)
);

-- table shipping_transfer
create table public.shipping_transfer (
	transfer_type_id serial,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate numeric(4,3),
	primary key (transfer_type_id)
);

-- table shipping_info
create table public.shipping_info (
	shippingid int,
	vendorid int,
	payment_amount numeric (7,2),
	shipping_plan_datetime timestamp,
	transfer_type_id int,
	shipping_country_id int,
	agreementid int,
	primary key (shippingid),
	FOREIGN KEY  (transfer_type_id) REFERENCES shipping_transfer (transfer_type_id) ON UPDATE cascade,
	FOREIGN KEY (shipping_country_id) REFERENCES public.shipping_country_rates (shipping_country_id) ON UPDATE cascade,
	FOREIGN KEY (agreementid) REFERENCES public.shipping_agreement (agreementid) ON UPDATE cascade
);

-- table shipping_status
create table public.shipping_status (
	shippingid int UNIQUE,
	status text,
	state text,
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp,
	primary key (shippingid)
);

