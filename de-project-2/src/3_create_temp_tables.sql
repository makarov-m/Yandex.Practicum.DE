-- drop temp tables
DROP table IF EXISTS public.temp_is_delay;
DROP table IF EXISTS public.temp_full_day_at_shipping;
DROP table IF EXISTS public.temp_is_shipping_finished;
DROP table IF EXISTS public.temp_delay_day_at_shipping;
DROP table IF EXISTS public.temp_payment_info;

-- create temp tables

/* temp_full_day_at_shipping */
create table public.temp_full_day_at_shipping (
shippingid int,
full_day_at_shipping interval,
primary key (shippingid)
);

/* temp_is_delay */
create table public.temp_is_delay (
shippingid int,
is_delay int,
primary key (shippingid)
);

/* temp_is_shipping_finished */
create table public.temp_is_shipping_finished (
shippingid int,
is_shipping_finished int,
primary key (shippingid)
);

/* temp_delay_day_at_shipping */
create table public.temp_delay_day_at_shipping (
shippingid int,
delay_day_at_shipping int,
primary key (shippingid)
);

/* temp_payment_info */
create table public.temp_payment_info (
shippingid int,
payment_amount numeric(7,2),
vat numeric(14,7),
profit numeric(14,7),
primary key (shippingid)
);