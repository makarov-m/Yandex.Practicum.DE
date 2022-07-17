SELECT id, "name", login
FROM production.users u;

select * 
from production.orderstatuslog osl ;

select *
from production.orderstatuses os; 

select *
from production.orders o ;

select * from orderitems oi ;

select * from products p ;

/** Таблица пользователей и количества заказов **/
select 
	o.user_id,
	count (
	case 
		when os.key = 'Closed' then 1
	end
	) as number_of_orders_closed
from 
	production.orders o left join production.orderstatuses os 
		on o.status  = os.id
group by 
	o.user_id 
order by 
	number_of_orders_closed desc ;

/** Таблица пользователей и суммы покупок **/
select 
	o.user_id,
	sum(payment) as money_amount
from 
	production.orders o left join production.orderstatuses os 
		on o.status  = os.id
where 
	os."key" = 'Closed'
group by 
	o.user_id 
order by 
	money_amount desc ;

/** Таблица пользователей, последнего заказа, предпоследнего заказа, разницы между последним и предпоследним **/
with cte1 as (
select 
	user_id, 
	LAG(osl.dttm,-1) OVER (
		PARTITION BY user_id
		ORDER BY osl.dttm desc
	) previous_close_time,
	osl.dttm as close_time
from orders o  
	inner join orderstatuslog osl on o.order_id = osl.order_id 
	inner join orderstatuses os on o.status = os.id
where 
	os."key" = 'Closed'
order by 
	user_id asc,  
	close_time desc
	)
select 
	*, (close_time - previous_close_time) as order_variance
from 
	cte1;

/** найдем максимальную дату заказа в датасете **/
select 
	MAX(osl.dttm) as max_close_order_time
from orders o  
	inner join orderstatuslog osl on o.order_id = osl.order_id 
	inner join orderstatuses os on o.status = os.id
where 
	os."key" = 'Closed'
;


/** максимальная и минимальная разница между заказами **/
select 
MAX(t1.order_variance),
MIN(t1.order_variance)
from
(with cte1 as (
select 
	user_id, 
	LAG(osl.dttm,-1) OVER (
		PARTITION BY user_id
		ORDER BY osl.dttm desc
	) previous_close_time,
	osl.dttm as close_time
from orders o  
	inner join orderstatuslog osl on o.order_id = osl.order_id 
	inner join orderstatuses os on o.status = os.id
where 
	os."key" = 'Closed'
order by 
	user_id asc,  
	close_time desc
	)
select 
	*, (close_time - previous_close_time) as order_variance
from 
	cte1) as t1
	
	
/** качество данных, типы данных **/
select table_name , column_name , is_nullable , data_type 
from information_schema."columns" c 
where table_schema = 'production'
order by table_name ;

/** качество данных, пропуски **/
select * from users;

/** качество данных, дубликаты **/
SELECT product_id, order_id, price, discount, quantity, COUNT(*)
FROM orderitems oi 
GROUP BY product_id, order_id, price, discount, quantity
HAVING COUNT(*) > 1

/** качество данных, дубликаты **/
SELECT order_id, COUNT(order_id)
FROM orders
GROUP BY order_id
HAVING COUNT(order_id) > 1	

/** качество данных, дубликаты **/
SELECT order_id, status_id, COUNT(*)
FROM orderstatuslog osl 
GROUP BY order_id, status_id
HAVING COUNT(*) > 1

/** качество данных, дубликаты **/
SELECT name, COUNT(name)
FROM products p 
GROUP BY name
HAVING COUNT(name) > 1	

/** поиск нулей **/
SELECT * FROM orderitems oi  WHERE NOT (oi IS NOT NULL);
SELECT * FROM orders o  WHERE NOT (o IS NOT NULL);
SELECT * FROM orderstatuses os  WHERE NOT (os IS NOT NULL);
SELECT * FROM orderstatuslog osl WHERE NOT (osl IS NOT NULL);
SELECT * FROM products p WHERE NOT (p IS NOT NULL);
