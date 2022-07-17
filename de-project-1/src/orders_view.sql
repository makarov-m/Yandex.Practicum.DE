DROP VIEW IF EXISTS analysis.orders CASCADE;

create view analysis.orders as
WITH last_order_status AS
  (SELECT order_id,
          status_id AS status,
          dttm,
          ROW_NUMBER() OVER(PARTITION BY order_id
                            ORDER BY status_id DESC)
   FROM production.orderstatuslog o
   ORDER BY order_id ASC, status DESC)
SELECT o.order_id, --o.order_ts,
 o.user_id,
 o.bonus_payment,
 o.payment,
 o."cost",
 o.bonus_grant,
 los.status AS status,
 los.dttm AS order_ts
FROM production.orders o
INNER JOIN last_order_status los ON o.order_id = los.order_id
WHERE los.row_number = 1;