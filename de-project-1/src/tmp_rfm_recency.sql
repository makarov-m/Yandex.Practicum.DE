CREATE TABLE analysis.tmp_rfm_recency (
user_id INT NOT NULL PRIMARY KEY, 
recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);

/** дата последнего CLOSED заказа в датасете **/
--SELECT MAX(o.order_ts) AS max_close_order_time
--FROM analysis.orders o
--INNER JOIN analysis.orderstatuses os ON o.status = os.id
--WHERE os."key" = 'Closed';

/** пользователь, дата последнего заказа по таблице, да последнего заказа пользователя **/
--SELECT u.id,
  --(SELECT MAX(o.order_ts) AS max_close_order_time
   --FROM analysis.orders o
   --INNER JOIN analysis.orderstatuses os ON o.status = os.id
   --WHERE os."key" = 'Closed' ), max(o.order_ts) AS max_order_by_user
--FROM analysis.users u
--LEFT JOIN analysis.orders o ON u.id = o.user_id
--WHERE o.status = 5
--GROUP BY u.id
--ORDER BY u.id ASC;

/** Находим разницу последнего заказа пользователя с последним заказом по таблице **/ 
--WITH t1 AS
  --(SELECT u.id,
     --(SELECT MAX(o.order_ts) AS max_close_order_time
      --FROM analysis.orders o
      --INNER JOIN analysis.orderstatuses os ON o.status = os.id
      --WHERE os."key" = 'Closed' ), max(o.order_ts) AS max_order_by_user
   --FROM analysis.users u
   --LEFT JOIN analysis.orders o ON u.id = o.user_id
   --WHERE o.status = 5
   --GROUP BY u.id
   --ORDER BY u.id ASC)
--SELECT t1.id,
       --t1.max_close_order_time - t1.max_order_by_user AS variation
--FROM t1;

/** Разобьем результат на 5 корзин**/ 
--WITH t1 AS
  --(SELECT u.id,
     --(SELECT MAX(o.order_ts) AS max_close_order_time
      --FROM analysis.orders o
      --INNER JOIN analysis.orderstatuses os ON o.status = os.id
      --WHERE os."key" = 'Closed' ), max(o.order_ts) AS max_order_by_user
   --FROM analysis.users u
   --LEFT JOIN analysis.orders o ON u.id = o.user_id
   --WHERE o.status = 5
   --GROUP BY u.id
   --ORDER BY u.id ASC)
--SELECT t1.id,
       --t1.max_close_order_time - t1.max_order_by_user AS variation,
       --NTILE(5) OVER(
                     --ORDER BY (t1.max_close_order_time - t1.max_order_by_user)) AS recency
--FROM t1;

/** загрузим данные в таблицу tmp_rfm_recency**/ 
--INSERT INTO analysis.tmp_rfm_recency (user_id, recency)
--WITH t1 AS
  --(SELECT u.id,
     --(SELECT MAX(o.order_ts) AS max_close_order_time
      --FROM analysis.orders o
      --INNER JOIN analysis.orderstatuses os ON o.status = os.id
      --WHERE os."key" = 'Closed' ), max(o.order_ts) AS max_order_by_user
   --FROM analysis.users u
   --LEFT JOIN analysis.orders o ON u.id = o.user_id
   --WHERE o.status = 5
   --GROUP BY u.id
   --ORDER BY u.id ASC)
--SELECT t1.id,
       --t1.max_close_order_time - t1.max_order_by_user AS variation,
       --NTILE(5) OVER(
                     --ORDER BY (t1.max_close_order_time - t1.max_order_by_user)) AS recency
--FROM t1;                     
                     
----------------изменим логику запроса 
/** изменим логику запроса
 * выведем id, status, timestamp заказа, timestamp если заказ закрыт и min_timestamp если заказ не закрыт, row number
 * выделим только row_number = 1 (таблица отсортирована по убыванию статуса заказа) 
 * таким образом, первая строчка по каждому клиенту имеет максимально близкую дату заказа
 * обернем в cte
 * разделим на корзины 
 * загрузим данные в таблицу **/                    
INSERT INTO analysis.tmp_rfm_recency (user_id, recency)   
WITH rcnc AS
  (SELECT u.id,
          o.status,
          o.order_ts,
          CASE
              WHEN o.status = 4 THEN o.order_ts
              ELSE
                     (SELECT MIN(o2.order_ts)
                      FROM production.orders o2)
          END AS order_time,
          row_number() OVER (PARTITION BY u.id
                             ORDER BY CASE
                                          WHEN o.status = 4 THEN o.order_ts
                                          ELSE
                                                 (SELECT MIN(o2.order_ts)
                                                  FROM production.orders o2)
                                      END DESC) AS row_number
   FROM production.users u
   LEFT JOIN production.orders o ON u.id = o.user_id
   ORDER BY u.id ASC)
SELECT rcnc.id,
       ntile(5) OVER (
                      ORDER BY rcnc.order_time) AS recency
FROM rcnc
WHERE rcnc.row_number = 1;
                     
