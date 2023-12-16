import pandas as pd
import vertica_python
import psycopg2
import ssl

# PostgreSQL connection details
pg_conn_info = {
    "host": "rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net",
    "port": 6432,
    "user": "student",
    "password": "de_student_112022",
    "database": "db1",
    "sslmode": "require",
    "sslcert": "/Users/max/Documents/GitHub/de-project-final/cert/CA.pem"
}

# Vertica connection details
vertica_conn_info = {
    "host": "vertica.tgcloudenv.ru",
    "port": 5433,
    "user": "smartflipyandexru",
    "password": "yI8pKMyWliMeKYY",
    "database": "dwh",
    'autocommit': True
}


selected_date = "2022-10-01"

# Postgres. Currencies table.
query_fetch_from_postgres_currencies = f"""
    select 
    *
    from public.currencies
    where date_update::date = '{selected_date}';"""

query_fetch_from_postgres_transactions = f"""
    select 
    *
    from public.transactions
    where transaction_dt::date = '{selected_date}';"""

# make a connection to Postgres
conn_1 = psycopg2.connect(
    f"""
    host='{pg_conn_info['host']}'
    port='{pg_conn_info['port']}'
    dbname='{pg_conn_info['database']}' 
    user='{pg_conn_info['user']}' 
    password='{pg_conn_info['password']}'
    """
    )

cur_1 = conn_1.cursor()       
cur_1.execute(query_fetch_from_postgres_currencies) 
currencies_df = pd.DataFrame(cur_1.fetchall())
cur_1.execute(query_fetch_from_postgres_transactions) 
transactions_df = pd.DataFrame(cur_1.fetchall())
cur_1.close()

# Save output from postgres to CSV
output_postgres_csv = "/Users/max/Documents/GitHub/de-project-final/data"
write_currencies_df = currencies_df.to_csv(
    path_or_buf=f'{output_postgres_csv}/currencies_{selected_date}.csv', 
    sep = ';', 
    header = False,
    encoding='utf-8',
    index=False
    )
print(f"currencies_{selected_date}.csv has been created")

write_transactions_df = transactions_df.to_csv(
    path_or_buf=f'{output_postgres_csv}/transactions_{selected_date}.csv', 
    sep = ';', 
    header = False,
    encoding='utf-8',
    index=False
    )
print(f"transactions_{selected_date}.csv has been created")

path_currencies = f"{output_postgres_csv}/currencies_{selected_date}.csv"
path_transactions = f"{output_postgres_csv}/transactions_{selected_date}.csv"

query_paste_to_vertica_currencies = f"""
COPY SMARTFLIPYANDEXRU__STAGING.currencies (
	date_update, 
	currency_code, 
	currency_code_with, 
	currency_with_div)
FROM LOCAL '{path_currencies}'
DELIMITER ';'
REJECTED DATA AS TABLE SMARTFLIPYANDEXRU__STAGING.currencies_rej; 
"""

query_paste_to_vertica_transactions = f"""
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
FROM LOCAL '{path_transactions}'
DELIMITER ';'
REJECTED DATA AS TABLE SMARTFLIPYANDEXRU__STAGING.transactions_rej; 
"""

# Write to Vertica
with vertica_python.connect(**vertica_conn_info) as conn:
    cur = conn.cursor()  
    cur.execute(query_paste_to_vertica_currencies)
    print(f"currencies for {selected_date} has been added to Vertica")
    cur.execute(query_paste_to_vertica_transactions)
    print(f"transactions for {selected_date} has been added to Vertica")
    conn.commit()
    
# delete temrorary files