-- create view
DROP VIEW IF EXISTS public.shipping_datamart;

create view public.shipping_datamart as
select
si.shippingid,
si.vendorid,
st.transfer_type,
tfdas.full_day_at_shipping ,
tid.is_delay ,
tisf.is_shipping_finished ,
tddas.delay_day_at_shipping,
si.payment_amount,
tpi.vat ,
tpi.profit 
from 
	public.shipping_info si 
		left join public.shipping_transfer st on si.transfer_type_id = st.transfer_type_id 
		left join public.temp_delay_day_at_shipping tddas on si.shippingid = tddas.shippingid 
		left join public.temp_full_day_at_shipping tfdas on si.shippingid = tfdas.shippingid 
		left join public.temp_is_delay tid on si.shippingid = tid.shippingid 
		left join public.temp_is_shipping_finished tisf on si.shippingid = tisf.shippingid 
		left join public.temp_payment_info tpi on si.shippingid = tpi.shippingid 
order by
	shippingid asc;