# DAG which downloads from Poatgres and Uploading to STG Vertica

import os 
from datetime import datetime, timedelta
import pandas as pd
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.vertica.hooks.vertica import VerticaHook


### POSTGRESQL settings ###
pg_conn_1 = PostgresHook('postgres_conn').get_conn()

### Vertica settings ###
VERTICA_CONN_ID = 'vertica_conn'
vertica_hook = VerticaHook(vertica_conn_id=VERTICA_CONN_ID)

# load data from Postgres connection and save locally
def load_currencies(selected_date, output_postgres_csv, **kargs):
    #output_postgres_csv = "/lessons/data"
    path_currencies = f"{output_postgres_csv}/currencies_{selected_date}.csv"
    print(path_currencies)
    # Postgres. Currencies table.
    query_fetch_from_postgres_currencies = f"""
        select 
        *
        from public.currencies
        where date_update::date = '{selected_date}';"""
    test_connection_query = "select 100;"
    with pg_conn_1 as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                logging.info(f'Result = {result}')
                if result[0] == 100:
                    logging.info(f'No errors. Continuing ...')
                    cur.execute(query_fetch_from_postgres_currencies)
                    currencies_df = pd.DataFrame(cur.fetchall())
                    logging.info(currencies_df.values)
                    # Save output from postgres to CSV
                    write_currencies_df = currencies_df.to_csv(
                        path_or_buf=path_currencies, 
                        sep = ';', 
                        header = False,
                        encoding='utf-8',
                        index=False
                    )
                    logging.info(f"currencies_{selected_date}.csv has been created")
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()

def load_transactions(selected_date, output_postgres_csv, **kargs):
    path_transactions = f"{output_postgres_csv}/transactions_{selected_date}.csv"
    # Postgres. Transactions table.
    query_fetch_from_postgres_transactions = f"""
        select 
        *
        from public.transactions
        where transaction_dt::date = '{selected_date}';"""
    test_connection_query = "select 100;"
    with pg_conn_1 as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                logging.info(f'Result = {result}')
                if result[0] == 100:
                    logging.info(f'No errors. Continuing ...')      
                    cur.execute(query_fetch_from_postgres_transactions) 
                    transactions_df = pd.DataFrame(cur.fetchall())
                    write_transactions_df = transactions_df.to_csv(
                        path_or_buf=path_transactions, 
                        sep = ';', 
                        header = False,
                        encoding='utf-8',
                        index=False
                        )
                    logging.info(f"transactions_{selected_date}.csv has been created")
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()

def push_currencies(selected_date, output_postgres_csv, **kargs):
    path_currencies = f"{output_postgres_csv}/currencies_{selected_date}.csv"
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
    test_connection_query = "select 100;"
    # Write to Vertica
    with vertica_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                logging.info(f'Result = {result}')
                if result[0] == 100:
                    logging.info(f'No errors. Continuing ...')
                    cur.execute(query_paste_to_vertica_currencies)
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()

def push_transactions(selected_date, output_postgres_csv, **kargs):
    path_transactions = f"{output_postgres_csv}/transactions_{selected_date}.csv"
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
    test_connection_query = "select 100;"
    # Write to Vertica
    with vertica_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                logging.info(f'Result = {result}')
                if result[0] == 100:
                    logging.info(f'No errors. Continuing ...')
                    cur.execute(query_paste_to_vertica_transactions)
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()

def remove_temp_files(selected_date, output_postgres_csv, **kargs):
    path_currencies = f"{output_postgres_csv}/currencies_{selected_date}.csv"
    path_transactions = f"{output_postgres_csv}/transactions_{selected_date}.csv"
    os.remove(path_currencies)
    logging.info(f"currencies for {selected_date} has been removed")
    os.remove(path_transactions)
    logging.info(f"transactions file for {selected_date} has been removed")


default_args = {
    'owner': 'Airflow',
    'retries': 1,                          # the number of retries that should be performed before failing the task
    'retry_delay': timedelta(minutes=1),   # delay between retries
    'depends_on_past': False,
}

with DAG(
        '3_STG_vertica_load',               # name
        default_args=default_args,          # connect args
        schedule_interval='@daily',         # interval
        start_date=datetime(2023, 5, 11),   # start calc
        catchup=True,                       # used in  the first launch, from date in the past until now. Usually = off
        tags=['final', 'project'],
) as dag:

    # create DAG logic (sequence/order)
    start = DummyOperator(task_id="start")
    with TaskGroup("load_stg_tables") as load_tables:
        load_cur = PythonOperator(
            task_id="load_currencies", python_callable=load_currencies, 
            op_kwargs={'selected_date': '{{ ds }}', 'output_postgres_csv': '/lessons/data'}, 
            provide_context=True, dag=dag)
        load_trans = PythonOperator(
            task_id="load_transactions", python_callable=load_transactions, 
            op_kwargs={'selected_date': '{{ ds }}', 'output_postgres_csv': '/lessons/data'},
            dag=dag)
    with TaskGroup("push_data_stg") as push_data_vertica:
        push_cur = PythonOperator(
            task_id="push_currencies", python_callable=push_currencies, 
            op_kwargs={'selected_date': '{{ ds }}', 'output_postgres_csv': '/lessons/data'},
            dag=dag)
        push_trans = PythonOperator(
            task_id="push_transactions", python_callable=push_transactions, 
            op_kwargs={'selected_date': '{{ ds }}', 'output_postgres_csv': '/lessons/data'},
            dag=dag)
    remove_files = PythonOperator(
        task_id="remove_temp_files", python_callable=remove_temp_files, 
        op_kwargs={'selected_date': '{{ ds }}', 'output_postgres_csv': '/lessons/data'},
        dag=dag)
    end = DummyOperator(task_id="end")
    
    start >> load_tables >> push_data_vertica >> remove_files >> end



