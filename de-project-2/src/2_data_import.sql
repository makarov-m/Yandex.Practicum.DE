-- insert into shipping_country_rates 
insert into public.shipping_country_rates (shipping_country, shipping_country_base_rate )
select
	distinct 
		shipping_country, shipping_country_base_rate 
from 
	public.shipping s 
order by
	shipping_country_base_rate asc;

-- insert into shipping_agreement
insert into public.shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
select
distinct 
	(regexp_split_to_array(vendor_agreement_description , E'\\:+'))[1]::int as agreementid,
	(regexp_split_to_array(vendor_agreement_description , E'\\:+'))[2]::text as agreement_number,
	(regexp_split_to_array(vendor_agreement_description , E'\\:+'))[3]::numeric(2,2) as agreement_rate,
	(regexp_split_to_array(vendor_agreement_description , E'\\:+'))[4]::numeric(2,2) as agreement_commission
from 
	public.shipping s 
order by
	agreementid asc;

-- insert into shipping_transfer
insert into public.shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
select
distinct 
	(regexp_split_to_array(shipping_transfer_description , E'\\:+'))[1] as transfer_type,
	(regexp_split_to_array(shipping_transfer_description , E'\\:+'))[2] as transfer_model,
	shipping_transfer_rate
from 
	public.shipping s 
;

-- insert into shipping_info
insert into public.shipping_info (shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
with t1 as (
select
	transfer_type_id, 
	concat(transfer_type,':',transfer_model) as shipping_transfer_description,
	shipping_transfer_rate
from 
	public.shipping_transfer )
select
distinct 
	s.shippingid, 
	s.vendorid,
	s.payment_amount,
	s.shipping_plan_datetime,
	t1.transfer_type_id,
	scr.shipping_country_id,
	(regexp_split_to_array(s.vendor_agreement_description , E'\\:+'))[1]::int as agreementid
from 
	public.shipping s left join t1
	on (s.shipping_transfer_description = t1.shipping_transfer_description 
	and s.shipping_transfer_rate = t1.shipping_transfer_rate)
	left join public.shipping_country_rates scr on s.shipping_country_base_rate = scr.shipping_country_base_rate 
;

-- insert into shipping_status 
insert into public.shipping_status (shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
WITH max_order AS
  (SELECT shippingid,
          MAX(state_datetime) AS max_state_datetime
   FROM shipping
   GROUP BY shippingid
   ORDER BY shippingid),
     booked AS
  (SELECT shippingid,
          state,
          state_datetime AS shipping_start_fact_datetime
   FROM shipping s
   WHERE state = 'booked'
   ORDER BY shippingid ASC, state_datetime ASC),
     recieved AS
  (SELECT shippingid,
          state,
          state_datetime AS shipping_end_fact_datetime
   FROM shipping s
   WHERE state = 'recieved'
   ORDER BY shippingid ASC, state_datetime ASC)
SELECT s.shippingid,
       s.status,
       s.state,
       b.shipping_start_fact_datetime,
       r.shipping_end_fact_datetime
FROM shipping s
LEFT JOIN max_order mo ON s.shippingid = mo.shippingid
LEFT JOIN booked b ON s.shippingid = b.shippingid
LEFT JOIN recieved r ON s.shippingid = r.shippingid
WHERE s.state_datetime = mo.max_state_datetime
ORDER BY shippingid ;