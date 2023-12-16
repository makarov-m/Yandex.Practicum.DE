COPY SMARTFLIPYANDEXRU__STAGING.currencies (
	date_update, 
	currency_code, 
	currency_code_with, 
	currency_with_div)
FROM LOCAL '/Users/max/Documents/GitHub/de-project-final/data/currencies.csv'
DELIMITER ';'
REJECTED DATA AS TABLE SMARTFLIPYANDEXRU__STAGING.currencies_rej; 


COPY SMARTFLIPYANDEXRU__STAGING.transactions (
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
FROM LOCAL '/Users/max/Documents/GitHub/de-project-final/data/transactions.csv'
DELIMITER ';'
REJECTED DATA AS TABLE SMARTFLIPYANDEXRU__STAGING.transactions_rej; 