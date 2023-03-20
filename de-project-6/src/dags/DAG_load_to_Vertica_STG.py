#### Interact with Vertica DB:
#### drop and create tables
#### load data to vertica from docker container
#### in AIRFLOW create vertica connection first and instal libs in container


from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import logging
import vertica_python
from airflow.contrib.hooks.vertica_hook import VerticaHook

def execute_vertica():
    path = os.getcwd()
    logging.info(f"working path - {path}")
    cur = VerticaHook('vertica_conn_id').get_cursor()
    logging.info("Start connection - db Vertica")
    cur.execute("DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.group_log;")
    cur.execute("DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.group_log_rej;")
    cur.execute("""
                    CREATE TABLE SMARTFLIPYANDEXRU__STAGING.group_log
                    (
                        group_id INT NOT NULL,
                        user_id INT,
                        user_id_from INT,
                        event varchar(50),
                        datetime timestamp
                    );
        """)
    cur.execute("""
                    COPY SMARTFLIPYANDEXRU__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
                    FROM LOCAL '/data/group_log.csv'
                    DELIMITER ','
                    REJECTED DATA AS TABLE SMARTFLIPYANDEXRU__STAGING.group_log_rej;
    """)       

    sql = "select count(*) from SMARTFLIPYANDEXRU__STAGING.group_log"
    cur.execute(sql)
    result = cur.fetchall()[0][0]
    logging.info(result)
    logging.info(f'fetched result in DB - {result}')
    cur.execute('COMMIT;')
    cur.close()


default_args = {
    'owner': 'Airflow',
    'schedule_interval':'@once',           # sheduled or not
    'retries': 1,                          # the number of retries that should be performed before failing the task
    'retry_delay': timedelta(minutes=5),   # delay between retries
    'depends_on_past': False,
    'catchup':False
}

with DAG(
        'load_to_Vertica_STG',                     # name
        default_args=default_args,         # connect args
        schedule_interval='0/15 * * * *',  # interval
        start_date=datetime(2021, 1, 1),   # start calc
        catchup=False,                     # used in  the first launch, from date in the past until now. Usually = off
        tags=['sprint6', 'example'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    t2 = PythonOperator(task_id="create_tables_and_load_data", python_callable=execute_vertica, dag=dag)
    t4 = DummyOperator(task_id="end")
    
    t1 >> t2 >> t4
