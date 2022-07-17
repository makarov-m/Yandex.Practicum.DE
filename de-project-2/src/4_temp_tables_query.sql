/* temp_full_day_at_shipping */
insert into public.temp_full_day_at_shipping (shippingid, full_day_at_shipping)
SELECT shippingid,
       date_trunc('day', shipping_end_fact_datetime - shipping_start_fact_datetime) AS full_day_at_shipping
FROM public.shipping_status ss;

/* temp_is_delay */
insert into public.temp_is_delay (shippingid, is_delay)
WITH t1 AS
  (SELECT DISTINCT s.shippingid,
                   s.shipping_plan_datetime,
                   ss.shipping_end_fact_datetime
   FROM public.shipping s
   LEFT JOIN public.shipping_status ss ON s.shippingid = ss.shippingid
   ORDER BY s.shippingid ASC)
SELECT shippingid,
       CASE
           WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN 1
           ELSE 0
       END AS is_delay
FROM t1;

/* temp_is_shipping_finished */
insert into public.temp_is_shipping_finished (shippingid, is_shipping_finished)
SELECT shippingid,
       CASE
           WHEN status = 'finished' THEN 1
           ELSE 0
       END AS is_shipping_finished
FROM public.shipping_status ss;

/* temp_delay_day_at_shipping */
insert into public.temp_delay_day_at_shipping (shippingid, delay_day_at_shipping)
SELECT si.shippingid,
       CASE
           WHEN (ss.shipping_end_fact_datetime > si.shipping_plan_datetime) 
           THEN EXTRACT (days FROM ss.shipping_end_fact_datetime - si.shipping_plan_datetime)
           ELSE 0
       END AS delay_day_at_shipping
FROM public.shipping_info si
LEFT JOIN public.shipping_status ss ON si.shippingid = ss.shippingid
ORDER BY shippingid ASC;

/* temp_payment_info */
insert into public.temp_payment_info (shippingid, payment_amount, vat, profit)
SELECT 
si.shippingid,
si.payment_amount,
si.payment_amount * ( scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) as vat,
si.payment_amount * sa.agreement_commission as profit
FROM public.shipping_info si 
	left join public.shipping_agreement sa on si.agreementid = sa.agreementid 
	left join public.shipping_country_rates scr on si.shipping_country_id = scr.shipping_country_id 
	left join public.shipping_transfer st on si.transfer_type_id = st.transfer_type_id 
order by
shippingid;
