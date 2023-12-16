# DAG which creates STAGING (as is) layer in Vertica database

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.vertica.hooks.vertica import VerticaHook

### Vertica settings ###
VERTICA_CONN_ID = 'vertica_conn'
vertica_hook = VerticaHook(vertica_conn_id=VERTICA_CONN_ID)


def create_stg_layer():
    query_create_stg_layer = """
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
                    cur.execute(query_create_stg_layer)
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()

default_args = {
    'owner': 'Airflow',
    'schedule_interval':'@once',           # sheduled or not
    'retries': 1,                          # the number of retries that should be performed before failing the task
    'retry_delay': timedelta(minutes=5),   # delay between retries
    'depends_on_past': False,
    'catchup':False
}

with DAG(
        '0_STG_vertica_create',            # name
        default_args=default_args,         # connect args
        schedule_interval='0/15 * * * *',  # interval
        start_date=datetime(2021, 1, 1),   # start calc
        catchup=False,                     # used in  the first launch, from date in the past until now. Usually = off
        tags=['final', 'project'],
) as dag:

    # create DAG logic (sequence/order)
    start = DummyOperator(task_id="start")
    with TaskGroup("load_stg_tables") as create_stg:
        t21 = PythonOperator(task_id="create_stg_layer", python_callable=create_stg_layer, dag=dag)
    end = DummyOperator(task_id="end")
    
    start >> create_stg >> end