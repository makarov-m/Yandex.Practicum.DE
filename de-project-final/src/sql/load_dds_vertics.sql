-------------------------
--- TRANSACTIONS deduplication
-------------------------

--- 2 STEP. Insert from STG to DDS
insert into SMARTFLIPYANDEXRU__DWH.operations
select operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt 
from (
    select * , row_number() over ( partition by operation_id, transaction_dt ) as rownum from SMARTFLIPYANDEXRU__STAGING.transactions
) a where a.rownum = 1 
		AND account_number_from > 0
		AND operation_id IS NOT NULL
		AND account_number_from IS NOT NULL
		AND account_number_to IS NOT NULL
		AND currency_code IS NOT NULL
		AND country IS NOT NULL
		AND status IS NOT NULL
		AND transaction_type IS NOT NULL
		AND amount IS NOT NULL
		AND transaction_dt IS NOT NULL;

-------------------------
--- CURRENCIES deduplication
-------------------------

--- 2 STEP. Insert from STG to DDS

insert into SMARTFLIPYANDEXRU__DWH.currencies
select id, date_update, currency_code, currency_code_with, currency_with_div
from (
    select * , row_number() over ( partition by currency_code, currency_code_with, date_update ) as rownum from SMARTFLIPYANDEXRU__STAGING.currencies
) a where a.rownum = 1 
	AND id IS NOT NULL
    AND date_update IS NOT NULL
    AND currency_code IS NOT NULL
    AND currency_code_with IS NOT NULL
    AND currency_with_div IS NOT NULL;