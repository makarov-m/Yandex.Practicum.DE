--2--
create view analysis.orders as 
select *
from production.orders o 
--where extract (year from o.order_ts ) >= 2021
;
--1--
create view analysis.orderitems as
select oi.* 
from production.orderitems oi 
inner join analysis.orders o on oi.order_id  = o.order_id 
;
--3--
create view analysis.orderstatuses as 
select *
from production.orderstatuses os
;
--4--
create view analysis.products as 
select * from production.products p 
;
--5--
create view analysis.users as 
SELECT *
FROM production.users u
;
--rename columns in user table
--ALTER TABLE analysis.users
  --RENAME COLUMN "name" TO new_name;

 --ALTER TABLE analysis.users
  --RENAME COLUMN "login" TO "name";
  
 --ALTER TABLE analysis.users
  --RENAME COLUMN "new_name" TO login;