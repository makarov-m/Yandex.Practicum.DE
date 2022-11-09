# LOAD DATA TO STG 
# Забираем данные API
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

# delete STG in local DB
drop_all_tables_STG = PostgresOperator(
    task_id="delete_STG",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="./delete_tables_STG.sql",
)

# delete DDS in local DB
drop_all_tables_DDS = PostgresOperator(
    task_id="delete_DDS",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="./delete_tables_DDS.sql",
)

# delete CDM in local DB
drop_all_tables_CDM = PostgresOperator(
    task_id="delete_CDM",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="./delete_tables_CDM.sql",
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
        'DROP_STG_DDS_CDM',                # name
        default_args=default_args,         # connect args
        schedule_interval='0/15 * * * *',  # interval
        start_date=datetime(2021, 1, 1),   # start calc
        catchup=False,                     # used in  the first launch, from date in the past until now. Usually = off
        tags=['project4', 'example'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    t4 = DummyOperator(task_id="end")
    
    t1 >> drop_all_tables_STG >> drop_all_tables_DDS >> drop_all_tables_CDM >> t4


