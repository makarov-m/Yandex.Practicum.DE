# DAG which insert in CDM from DDS

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook

### Vertica settings ###
VERTICA_CONN_ID = 'vertica_conn'
vertica_hook = VerticaHook(vertica_conn_id=VERTICA_CONN_ID)

def push_data_cdm(selected_date, **kargs):
    query_paste_to_vertica_cdm = f"""
    MERGE INTO SMARTFLIPYANDEXRU.global_metrics AS tgt USING
    (WITH t1 AS
        (-- main table with all data
    SELECT o.*,
            c.currency_with_div,
            (o.amount * c.currency_with_div) AS amount_usd
        FROM SMARTFLIPYANDEXRU__DWH.operations o
        LEFT JOIN SMARTFLIPYANDEXRU__DWH.currencies c ON o.currency_code = c.currency_code
        WHERE c.currency_code_with = 420
            AND o.transaction_dt::date = c.date_update::date
            AND o.status = 'done'
            AND o.transaction_dt::date = '{selected_date}' ) SELECT cast(transaction_dt AS date) AS date_update,
                                                            currency_code AS currency_from,
                                                            sum(amount_usd) AS amount_total,
                                                            count(amount_usd) AS cnt_transactions,
                                                            round((count(amount_usd)/count(DISTINCT (account_number_from))),3) AS avg_transactions_per_account,
                                                            count(DISTINCT (account_number_from)) AS cnt_accounts_make_transactions
    FROM t1
    GROUP BY date_update,
                currency_code
    ORDER BY date_update ASC) src ON tgt.date_update = src.date_update
    AND tgt.currency_from = src.currency_from WHEN MATCHED THEN
    UPDATE
    SET amount_total = src.amount_total,
        cnt_transactions = src.cnt_transactions,
        avg_transactions_per_account = src.avg_transactions_per_account,
        cnt_accounts_make_transactions = src.cnt_accounts_make_transactions WHEN NOT MATCHED THEN
    INSERT (date_update,
            currency_from,
            amount_total,
            cnt_transactions,
            avg_transactions_per_account,
            cnt_accounts_make_transactions)
    VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);
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
                    cur.execute(query_paste_to_vertica_cdm)
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()

default_args = {
    'owner': 'Airflow',
    'retries': 1,                          # the number of retries that should be performed before failing the task
    'retry_delay': timedelta(minutes=1),   # delay between retries
    'depends_on_past': False,
}

with DAG(
        '5_CDM_vertica_load',               # name
        default_args=default_args,          # connect args
        schedule_interval='@daily',         # interval
        start_date=datetime(2023, 5, 11),   # start calc
        catchup=True,                       # used in  the first launch, from date in the past until now. Usually = off
        tags=['final', 'project'],
) as dag:

    # create DAG logic (sequence/order)
    start = DummyOperator(task_id="start")
    insert_in_cdm = PythonOperator(
        task_id="push_data_cdm", python_callable=push_data_cdm, 
        op_kwargs={'selected_date': '{{ ds }}'}, 
        provide_context=True, dag=dag
        )
    end = DummyOperator(task_id="end")
    
    start >> insert_in_cdm >> end



