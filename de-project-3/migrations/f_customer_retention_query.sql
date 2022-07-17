insert into mart.f_customer_retention (new_customers_count, returning_customers_count, refunded_customer_count, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
-- create base table for calculations
WITH mt AS
  (SELECT fs2.item_id,
          fs2.customer_id,
          fs2.payment_amount,
          fs2.status,
          dc.week_of_year AS period_id
   FROM mart.f_sales fs2
   LEFT JOIN mart.d_calendar dc ON fs2.date_id = dc.date_id),
-- create intermediate table for calculations
t1 AS
  (SELECT period_id,
          customer_id,
          item_id,
          count(customer_id) AS num_orders,
          SUM (payment_amount) AS sum_payment_amount
   FROM mt
   GROUP BY period_id,
            customer_id,
            item_id
   ORDER BY period_id,
            customer_id,
            item_id),
-- calculate: point 1 and point 2, point 6 and point 7           
t2 as (           
SELECT period_id,
       item_id,
       COUNT (CASE
                  WHEN num_orders = 1 THEN num_orders
              END) AS new_customers_count,
             SUM (CASE
                      WHEN num_orders = 1 THEN sum_payment_amount
                  END) AS new_customers_revenue,
                 COUNT (CASE
                            WHEN num_orders > 1 THEN num_orders
                        END) AS returning_customers_count,
                       SUM (CASE
                                WHEN num_orders > 1 THEN sum_payment_amount
                            END) AS returning_customers_revenue
FROM t1
GROUP BY period_id,
         item_id
ORDER BY period_id,
         item_id),
-- calculate: point 3 and point 8
t3 AS
  (SELECT period_id,
          item_id,
          COUNT (DISTINCT CASE
                              WHEN status = 'refunded' THEN customer_id
                          END) AS refunded_customer_count,
                COUNT (CASE
                           WHEN status = 'refunded' THEN customer_id
                       END) AS customers_refunded
   FROM mt
   GROUP BY period_id,
            item_id
   ORDER BY period_id,
            item_id),            
-- create table for aggregated metrics
main AS
  (SELECT DISTINCT period_id,
                   item_id
   FROM mt
   ORDER BY period_id,
            item_id)
-- combine            
SELECT t2.new_customers_count,
       t2.returning_customers_count,
       t3.refunded_customer_count,
       m.period_id,
       m.item_id,
       t2.new_customers_revenue,
       t2.returning_customers_revenue,
       t3.customers_refunded
FROM main m
LEFT JOIN t2 ON (m.period_id = t2.period_id AND m.item_id = t2.item_id)
LEFT JOIN t3 ON (m.period_id = t3.period_id AND m.item_id = t3.item_id)
order by m.period_id, m.item_id;