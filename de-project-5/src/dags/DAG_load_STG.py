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

### API settings ###
# set params for API requests
sort_field = "id"
sort_direction = "asc"
limit = 50
offset = 0
nickname = "smartflip"
cohort = "3"
api_key = "25c27781-8fde-4b30-a22e-524044a7580f"
url_restaurants = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants" 
url_couriers = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers" 
url_deliveries = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries" 

class input_params:
    """Default params for API that will be changed in a loop"""
    headers = {
        "X-Nickname" : nickname,
        "X-Cohort" : cohort,
        "X-API-KEY" : api_key
        }
    params = {
        "sort_field" : sort_field, 
        "sort_direction" : sort_direction,
        "limit" : limit,
        "offset" : offset
        }

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

# load data from API connection
# paste data to local connection
# RESTAURANTS TABLE
def load_paste_data_restaurants():
    #RETRIEVE RESTAURANTS JSON
    r = requests.get(
        url = url_restaurants, 
        params = input_params.params, 
        headers = input_params.headers)
    json_record = json.loads(r.content)
    temp_list_restaurants = []
    while len(json_record) != 0 :
        r = requests.get(url = url_restaurants, params = input_params.params, headers = input_params.headers)
        json_record = json.loads(r.content)
        for obj in json_record:
            temp_list_restaurants.append(obj)
        input_params.params['offset'] += 50
    temp_list_restaurants = json.dumps(temp_list_restaurants)

    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'restaurants'
    
    # load to local to DB (restaurants)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        INSERT INTO stg.restaurants (object_value,update_ts) 
        VALUES ('{}', '{}');""".format(temp_list_restaurants,fetching_time)
    
    # load to local to DB (settings)
    postgres_insert_query_settings = """ 
        INSERT INTO stg.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(fetching_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# load data from API connection
# paste data to local connection
# COURIERS TABLE
def load_paste_data_couriers():
    #RETRIEVE COURIERS JSON
    r = requests.get(
        url = url_couriers, 
        params = input_params.params, 
        headers = input_params.headers)
    json_record = json.loads(r.content)
    temp_list_couriers = []
    while len(json_record) != 0 :
        r = requests.get(url = url_couriers, params = input_params.params, headers = input_params.headers)
        json_record = json.loads(r.content)
        for obj in json_record:
            temp_list_couriers.append(obj)
        input_params.params['offset'] += 50
    temp_list_couriers = json.dumps(temp_list_couriers)

    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'couriers'
    
    # load to local to DB (couriers)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        INSERT INTO stg.couriers (object_value,update_ts) 
        VALUES ('{}', '{}');""".format(temp_list_couriers,fetching_time)
    
    # load to local to DB (settings)
    postgres_insert_query_settings = """ 
        INSERT INTO stg.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(fetching_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# load data from API connection
# paste data to local connection
# DELIVERIES TABLE
def load_paste_data_deliveries():
    #RETRIEVE COURIERS JSON
    r = requests.get(
        url = url_deliveries, 
        params = input_params.params, 
        headers = input_params.headers)
    json_record = json.loads(r.content)
    temp_list_deliveries = []
    while len(json_record) != 0 :
        r = requests.get(url = url_deliveries, params = input_params.params, headers = input_params.headers)
        json_record = json.loads(r.content)
        for obj in json_record:
            temp_list_deliveries.append(obj)
        input_params.params['offset'] += 50
    temp_list_deliveries = json.dumps(temp_list_deliveries)

    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'deliveries'
    
    # load to local to DB (deliveries)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        INSERT INTO stg.deliveries (object_value,update_ts) 
        VALUES ('{}', '{}');""".format(temp_list_deliveries,fetching_time)
    
    # load to local to DB (settings)
    postgres_insert_query_settings = """ 
        INSERT INTO stg.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(fetching_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# init STG in local DB
# create tables and layer if they are not exist
create_all_tables_STG = PostgresOperator(
    task_id="create_STG_tables",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="./create_tables_STG.sql",
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
        '1_STG_postgres',                  # name
        default_args=default_args,         # connect args
        schedule_interval='0/15 * * * *',  # interval
        start_date=datetime(2021, 1, 1),   # start calc
        catchup=False,                     # used in  the first launch, from date in the past until now. Usually = off
        tags=['sprint5', 'example'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    with TaskGroup("load_stg_tables") as load_tables:
        t21 = PythonOperator(task_id="restaurants", python_callable=load_paste_data_restaurants, dag=dag)
        t22 = PythonOperator(task_id="couriers", python_callable=load_paste_data_couriers, dag=dag)
        t23 = PythonOperator(task_id="deliveries", python_callable=load_paste_data_deliveries, dag=dag)
    t4 = DummyOperator(task_id="end")
    
    t1 >> create_all_tables_STG >> load_tables >> t4


