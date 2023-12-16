# DAG which creates CDM (data mart) layer in Vertica database

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

def create_CDM_layer():
    query_create_CDM_layer = """
    CREATE SCHEMA IF NOT EXISTS dwh.SMARTFLIPYANDEXRU;
    DROP TABLE if exists SMARTFLIPYANDEXRU.global_metrics;
    CREATE TABLE if not exists SMARTFLIPYANDEXRU.global_metrics (
        date_update date NOT NULL,
        currency_from smallint NOT NULL,
        amount_total numeric (17,3) NOT NULL,
        cnt_transactions int NOT NULL,
        avg_transactions_per_account numeric (17,3) NOT NULL,
        cnt_accounts_make_transactions int NOT NULL,
        CONSTRAINT pk PRIMARY KEY (date_update, currency_from) ENABLED
    )
    ORDER BY date_update
    SEGMENTED BY HASH(date_update, currency_from) ALL NODES
    PARTITION BY date_update::date
    GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

    CREATE PROJECTION SMARTFLIPYANDEXRU.global_metrics 
    (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
    )
    AS
    SELECT global_metrics.date_update,
            global_metrics.currency_from,
            global_metrics.amount_total,
            global_metrics.cnt_transactions,
            global_metrics.avg_transactions_per_account,
            global_metrics.cnt_accounts_make_transactions
    FROM SMARTFLIPYANDEXRU.global_metrics
    ORDER BY global_metrics.date_update
    SEGMENTED BY hash(global_metrics.date_update, global_metrics.currency_from) ALL NODES KSAFE 1;

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
                    cur.execute(query_create_CDM_layer)
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
        '2_CDM_vertica_create',            # name
        default_args=default_args,         # connect args
        schedule_interval='0/15 * * * *',  # interval
        start_date=datetime(2021, 1, 1),   # start calc
        catchup=False,                     # used in  the first launch, from date in the past until now. Usually = off
        tags=['final', 'project'],
) as dag:

    # create DAG logic (sequence/order)
    start = DummyOperator(task_id="start")
    with TaskGroup("cread_CDM_tables") as create_CDM:
        t21 = PythonOperator(task_id="create_CDM_layer", python_callable=create_CDM_layer, dag=dag)
    end = DummyOperator(task_id="end")
    
    start >> create_CDM >>  end


