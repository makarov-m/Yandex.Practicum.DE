CREATE TABLE analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);

/** Таблица пользователей и количества заказов **/
select 
	o.user_id,
	count (
	case 
		when os.key = 'Closed' then 1
	end
	) as number_of_orders_closed
from 
	analysis.orders o left join analysis.orderstatuses os 
		on o.status  = os.id
group by 
	o.user_id 
order by 
	number_of_orders_closed desc ;
	
/** обернем пользователей и заказы в сte и разделим на корзины **/
WITH num_orders AS
  (SELECT o.user_id,
          COUNT (CASE
                     WHEN os.key = 'Closed' THEN 1
                 END) AS number_of_orders_closed
   FROM analysis.orders o
   LEFT JOIN analysis.orderstatuses os ON o.status = os.id
   GROUP BY o.user_id
   ORDER BY number_of_orders_closed DESC)
SELECT u.id, --nrds.number_of_orders_closed,
 NTILE(5) OVER(
               ORDER BY nrds.number_of_orders_closed) AS frequency
FROM analysis.users u
INNER JOIN num_orders nrds ON u.id = nrds.user_id

/** загрузим итоговые данные в таблицу **/
insert into analysis.tmp_rfm_frequency (user_id, frequency)
WITH num_orders AS
  (SELECT o.user_id,
          COUNT (CASE
                     WHEN os.key = 'Closed' THEN 1
                 END) AS number_of_orders_closed
   FROM analysis.orders o
   LEFT JOIN analysis.orderstatuses os ON o.status = os.id
   GROUP BY o.user_id
   ORDER BY number_of_orders_closed DESC)
SELECT u.id, --nrds.number_of_orders_closed,
 NTILE(5) OVER(
               ORDER BY nrds.number_of_orders_closed) AS frequency
FROM analysis.users u
INNER JOIN num_orders nrds ON u.id = nrds.user_id