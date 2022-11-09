# LOAD DATA TO DDS
# Забираем данные из STG
# инсертим в наш DWH

from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import psycopg2
import json
from wsgiref import headers
import requests

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup

### POSTGRESQL settings ###
# set postgresql connection from basehook
# all of these connections should be in Airflow as connectors
pg_conn_1 = PostgresHook.get_connection('PG_WAREHOUSE_CONNECTION')

# init connection
# Connect to your local postgres DB (Docker)
conn_1 = psycopg2.connect(
    f"""
    host='{pg_conn_1.host}'
    port='{pg_conn_1.port}'
    dbname='{pg_conn_1.schema}' 
    user='{pg_conn_1.login}' 
    password='{pg_conn_1.password}'
    """
    )   

# load data from DDS
# paste data to CDM local connection
# courier_ledger TABLE
def load_paste_data_dm_courier_ledger():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'courier_ledger'
    
    # load to local to DB (courier_ledger)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    WITH t1_courier_calculation AS
    (/* calculate tips depending on rating */ SELECT c.courier_id,
                                                    c.courier_name,
                                                    o.order_id,
                                                    o.sum,
                                                    t.year,
                                                    t.month,
                                                    avg(d.rate)::float AS rate_avg,
                                                    CASE
                                                        WHEN avg(d.rate) < 4 THEN (CASE
                                                                                        WHEN 0.05 * o.sum <= 100 THEN 100
                                                                                        ELSE 0.05 * o.sum
                                                                                    END)
                                                        WHEN avg(d.rate) >= 4
                                                                AND avg(d.rate) < 4.5 THEN (CASE
                                                                                                WHEN 0.07 * o.sum <= 150 THEN 150
                                                                                                ELSE 0.07 * o.sum
                                                                                            END)
                                                        WHEN avg(d.rate) >= 4.5
                                                                AND avg(d.rate) < 4.9 THEN (CASE
                                                                                                WHEN 0.08 * o.sum <= 175 THEN 175
                                                                                                ELSE 0.08 * o.sum
                                                                                            END)
                                                        WHEN avg(d.rate) >= 4.9 THEN (CASE
                                                                                            WHEN 0.1 * o.sum <= 200 THEN 200
                                                                                            ELSE 0.1 * o.sum
                                                                                        END)
                                                    END AS courier_order_sum
    FROM dds.deliveries d
    INNER JOIN dds.couriers c ON d.courier_id = c.id
    INNER JOIN dds.timestamps t ON d.delivery_ts = t.ts
    INNER JOIN dds.orders o ON d.order_id = o.id
    GROUP BY c.courier_id,
                c.courier_name,
                o.order_id,
                o.sum,
                t.year,
                t.month),
        t2_agg_tips AS
    (/* making aggregats from t1 */ SELECT courier_id,
                                            courier_name,
                                            "year",
                                            "month",
                                            SUM(courier_order_sum) AS courier_order_sum
    FROM t1_courier_calculation
    GROUP BY courier_id,
                courier_name,
                "year",
                "month")
    INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, order_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
    SELECT c.courier_id,
        c.courier_name,
        t.year,
        t.month,
        count(d.delivery_id) AS orders_count,
        sum(o."sum") AS orders_total_sum,
        avg(d.rate)::float AS rate_avg,
        sum(o."sum") * 0.25 AS order_processing_fee,
        t3.courier_order_sum AS courier_order_sum,
        sum(d.tip_sum) AS courier_tips_sum,
        (t3.courier_order_sum + 0.95 * sum(d.tip_sum)) AS courier_reward_sum
    FROM dds.deliveries d
    INNER JOIN dds.couriers c ON d.courier_id = c.id
    INNER JOIN dds.timestamps t ON d.delivery_ts = t.ts
    INNER JOIN dds.orders o ON d.order_id = o.id
    INNER JOIN t2_agg_tips AS t3 ON (c.courier_id = t3.courier_id
                                    AND t.year = t3.year
                                    AND t.month = t3.month)--WHERE c.courier_name = 'Эдуард Новиков'
    GROUP BY c.courier_id,
            c.courier_name,
            t.year,
            t.month,
            courier_order_sum ON CONFLICT (courier_id,
                                            settlement_year,
                                            settlement_month) DO
    UPDATE
    SET courier_name = excluded.courier_name,
        orders_count = excluded.orders_count,
        order_total_sum = excluded.order_total_sum,
        rate_avg = excluded.rate_avg,
        order_processing_fee = excluded.order_processing_fee,
        courier_order_sum = excluded.courier_order_sum,
        courier_tips_sum = excluded.courier_tips_sum,
        courier_reward_sum = excluded.courier_reward_sum;
    """
    
    # load to local to DB (settings)
    postgres_insert_query_settings = """ 
        INSERT INTO dds.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(fetching_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# init CDM in local DB
# create tables and layer if they are not exist
create_all_tables_CDM = PostgresOperator(
    task_id="create_CDM_tables",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="./create_tables_CDM.sql",
)

default_args = {
    'owner': 'Airflow',
    'schedule_interval':'@once',           # sheduled or not
    'retries': 1,                          # the number of retries that should be performed before failing the task
    'retry_delay': timedelta(minutes=5),   # delay between retries
    'depends_on_past': False,
    'catchup':False
}

with DAG(
        '3_CDM_postgres',                  # name
        default_args=default_args,         # connect args
        schedule_interval='0/15 * * * *',  # interval
        start_date=datetime(2021, 1, 1),   # start calc
        catchup=False,                     # used in  the first launch, from date in the past until now. Usually = off
        tags=['sprint5', 'example'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    with TaskGroup("load_cdm_tables") as load_tables:
        t21 = PythonOperator(task_id="courier_ledger", python_callable=load_paste_data_dm_courier_ledger, dag=dag)
    t4 = DummyOperator(task_id="end")
    
    t1 >> create_all_tables_CDM >> load_tables >> t4


