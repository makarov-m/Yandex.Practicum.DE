insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select dc.date_id, item_id, customer_id, city_id, quantity, 
case
    when status = 'shipped' then payment_amount
    else payment_amount * (-1)
end as payment_amount,
status
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';