-- STAGING
DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.currencies_rej;
DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.transactions_rej;
DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.currencies CASCADE;
DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.transactions CASCADE;

-- Curresncies table & Projection
CREATE TABLE if not exists SMARTFLIPYANDEXRU__STAGING.currencies (
    id IDENTITY(1,1), 
    date_update timestamp(0) NULL,
    currency_code smallint NULL,
    currency_code_with smallint NULL,
    currency_with_div numeric(5, 3) NULL
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE PROJECTION SMARTFLIPYANDEXRU__STAGING.currencies /*+createtype(P)*/ 
(
 id,
 date_update,
 currency_code,
 currency_code_with,
 currency_with_div
)
AS
 SELECT currencies.id,
        currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_with_div
 FROM SMARTFLIPYANDEXRU__STAGING.currencies
 ORDER BY currencies.id
SEGMENTED BY hash(currencies.id) ALL NODES KSAFE 1;

-- Transactions table & Projection
CREATE TABLE if not exists SMARTFLIPYANDEXRU__STAGING.transactions (
    operation_id uuid NOT NULL,
    account_number_from int NULL,
    account_number_to int NULL,
    currency_code smallint NULL,
    country varchar(30) NULL,
    status varchar(30) NULL,
    transaction_type varchar(30) NULL,
    amount int NULL,
    transaction_dt timestamp(3) NULL
)
ORDER BY operation_id
SEGMENTED BY HASH(operation_id) ALL NODES
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);

CREATE PROJECTION SMARTFLIPYANDEXRU__STAGING.transactions 
(
operation_id,
account_number_from,
account_number_to,
currency_code,
country,
status,
transaction_type,
amount,
transaction_dt
)
AS
SELECT transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
FROM SMARTFLIPYANDEXRU__STAGING.transactions
ORDER BY transactions.operation_id
SEGMENTED BY hash(transactions.operation_id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);