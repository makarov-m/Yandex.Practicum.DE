CREATE TABLE analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);

/** Таблица пользователей и суммы покупок **/
--SELECT o.user_id,
       --sum(payment) AS money_amount
--FROM analysis.orders o
--LEFT JOIN analysis.orderstatuses os ON o.status = os.id
--WHERE os."key" = 'Closed'
--GROUP BY o.user_id
--ORDER BY money_amount DESC ;
	
/** обернем пользователей и заказы в сte и разделим на корзины **/
--WITH sum_orders AS
  --(SELECT o.user_id,
          --sum(payment) AS money_amount
   --FROM analysis.orders o
   --LEFT JOIN analysis.orderstatuses os ON o.status = os.id
   --WHERE os."key" = 'Closed'
   --GROUP BY o.user_id
   --ORDER BY money_amount DESC)
--SELECT u.id, --so.money_amount,
--NTILE(5) OVER(
              --ORDER BY so.money_amount) AS monetary_value
--FROM analysis.users u
--INNER JOIN sum_orders so ON u.id = so.user_id
--;

/** загрузим данные в таблицу **/
--insert into analysis.tmp_rfm_monetary_value (user_id, monetary_value)
--WITH sum_orders AS
  --(SELECT o.user_id,
          --sum(payment) AS money_amount
   --FROM analysis.orders o
   --LEFT JOIN analysis.orderstatuses os ON o.status = os.id
   --WHERE os."key" = 'Closed'
   --GROUP BY o.user_id
   --ORDER BY money_amount DESC)
--SELECT u.id, --so.money_amount,
--NTILE(5) OVER(
              --ORDER BY so.money_amount) AS monetary_value
--FROM analysis.users u
--INNER JOIN sum_orders so ON u.id = so.user_id
--;

----------------изменим логику запроса
/** изменим логику запроса
 * выведем id, status, monetary value, row number
 * выделим только row_number = 1
 * разделим на корзины 
 * загрузим данные в таблицу **/
insert into analysis.tmp_rfm_monetary_value (user_id, monetary_value)
with mvc as (
SELECT u.id,
       o.status,
       CASE
           WHEN o.status = 4 THEN SUM (o.payment)
           ELSE 0
       END AS monetary_value,
       row_number() OVER (PARTITION BY u.id
                          ORDER BY CASE
                                       WHEN o.status = 4 THEN SUM (o.payment)
                                       ELSE 0
                                   END DESC) AS row_number
FROM production.users u
LEFT JOIN production.orders o ON u.id = o.user_id
GROUP BY u.id,
         o.status
ORDER BY u.id,
         monetary_value desc)
select 
mvc.id, --mvc.monetary_value,
NTILE(5) OVER(
              ORDER BY mvc.monetary_value) AS monetary_value
from 
mvc
where 
mvc.row_number = 1;