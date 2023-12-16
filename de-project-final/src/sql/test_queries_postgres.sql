-- query to transactions
SELECT 
*
FROM public.transactions
where transaction_dt::date = '2022-10-01';

-- query to currencies
select 
*
from public.currencies c 
where date_update::date = '2022-10-01';
