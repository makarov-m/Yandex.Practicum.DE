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

# load data from STG
# paste data to DDS local connection
# RESTAURANTS TABLE
def load_paste_data_restaurants():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'restaurants'
    
    # load to local to DB (restaurants)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    insert into dds.restaurants (restaurant_id, restaurant_name)
    SELECT distinct 
        json_array_elements(r.object_value::JSON)->>'_id'  AS restaurant_id,
        json_array_elements(r.object_value::JSON)->>'name' AS restaurant_name
    FROM stg.restaurants r
    INNER JOIN stg.settings s ON r.update_ts = s.workflow_key::timestamp
    WHERE s.workflow_key::timestamp =
        (SELECT MAX (workflow_key::timestamp)
        FROM stg.settings
        WHERE workflow_settings = 'restaurants' )
    on conflict (restaurant_id) do update 
        set 
            restaurant_id = excluded.restaurant_id,
            restaurant_name = excluded.restaurant_name;
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

# load data from STG
# paste data to DDS local connection
# COURIERS TABLE
def load_paste_data_couriers():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'couriers'
    
    # load to local to DB (couriers)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    insert into dds.couriers (courier_id, courier_name)
    SELECT distinct
        json_array_elements(c.object_value::JSON)->>'_id'  AS courier_id,
        json_array_elements(c.object_value::JSON)->>'name' AS courier_name
    FROM stg.couriers c
    INNER JOIN stg.settings s ON c.update_ts = s.workflow_key::timestamp
    WHERE s.workflow_key::timestamp =
        (SELECT MAX (workflow_key::timestamp)
        FROM stg.settings
        WHERE workflow_settings = 'couriers' )
    on conflict (courier_id) do update 
        set 
            courier_id = excluded.courier_id,
            courier_name = excluded.courier_name;
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

# load data from STG
# paste data to DDS local connection
# TIMESTAMPS TABLE
def load_paste_data_timestamps():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'timestamps'
    
    # load to local to DB (timestamps)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    with t1_deliveries_ts as (
    SELECT distinct
        (json_array_elements(d.object_value::JSON) ->> 'delivery_ts')::timestamp  as delivery_ts
    FROM stg.deliveries d
    INNER JOIN stg.settings s ON d.update_ts = s.workflow_key::timestamp
    WHERE s.workflow_key::timestamp =
        (SELECT MAX (workflow_key::timestamp)
        FROM stg.settings
        WHERE workflow_settings = 'deliveries' )     
    ),
    t2_orders_ts as (
    SELECT distinct
        (json_array_elements(d.object_value::JSON) ->> 'order_ts')::timestamp  as order_ts
    FROM stg.deliveries d
    INNER JOIN stg.settings s ON d.update_ts = s.workflow_key::timestamp
    WHERE s.workflow_key::timestamp =
        (SELECT MAX (workflow_key::timestamp)
        FROM stg.settings
        WHERE workflow_settings = 'deliveries' )    
    ),
    t3_union as (
    select 
        delivery_ts as ts
    from
        t1_deliveries_ts
    union all
    select 
        order_ts as ts
    from
        t2_orders_ts   
    )
    insert into dds.timestamps (ts, year, month, day, time, date)
    select distinct
        ts,
        extract ("year" from ts) as "year",
        extract ("month" from ts) as "month",
        extract ("day" from ts) as "day",
        ts::time as "time",
        ts::date as "date"
    from
        t3_union
    on conflict (ts) do update 
        set 
            ts = excluded.ts,
            "year" = excluded."year",
            "month" = excluded."month",
            "day" = excluded."day",
            "time" = excluded."time",
            "date" = excluded."date";
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

# load data from STG
# paste data to DDS local connection
# ORDERS TABLE
def load_paste_data_orders():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'orders'
    
    # load to local to DB (orders)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    insert into dds.orders (order_id, order_ts, "sum")
    select distinct 
        json_array_elements(d.object_value::JSON) ->> 'order_id'              as order_id,
        (json_array_elements(d.object_value::JSON) ->> 'order_ts')::timestamp as order_ts,
        (json_array_elements(d.object_value::JSON) ->> 'sum')::numeric(14,2)  as sum
    FROM stg.deliveries d
    INNER JOIN stg.settings s ON d.update_ts = s.workflow_key::timestamp
    WHERE s.workflow_key::timestamp =
        (SELECT MAX (workflow_key::timestamp)
        FROM stg.settings
        WHERE workflow_settings = 'deliveries' )
    on conflict (order_id) do update 
        set 
            order_id = excluded.order_id,
            order_ts = excluded.order_ts,
            "sum" = excluded."sum";
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

# load data from STG
# paste data to DDS local connection
# DELIVERIES TABLE
def load_paste_data_deliveries():
    # fetching time UTC and table
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'deliveries'
    
    # load to local to DB (deliveries)
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    with t1_deliveries_part as (
    select
        json_array_elements(d.object_value::JSON) ->> 'delivery_id'               as delivery_id,
        json_array_elements(d.object_value::JSON) ->> 'order_id'                  as order_id,
        json_array_elements(d.object_value::JSON) ->> 'courier_id'                as courier_id,
        json_array_elements(d.object_value::JSON) ->> 'address'                   as address,
        (json_array_elements(d.object_value::JSON) ->> 'delivery_ts')::timestamp  as delivery_ts,
        (json_array_elements(d.object_value::JSON) ->> 'rate')::int               as rate,
        (json_array_elements(d.object_value::JSON) ->> 'tip_sum')::numeric(14,2)  as tip_sum
    FROM stg.deliveries d
    INNER JOIN stg.settings s ON d.update_ts = s.workflow_key::timestamp
    WHERE s.workflow_key::timestamp =
        (SELECT MAX (workflow_key::timestamp)
        FROM stg.settings
        WHERE workflow_settings = 'deliveries' )
    )     
    insert into dds.deliveries (delivery_id, courier_id, order_id, address, delivery_ts, rate, tip_sum)
    select distinct
        t1.delivery_id, 
        c.id as courier_id_one, 
        o.id as order_id_one, 
        t1.address, 
        t1.delivery_ts, 
        t1.rate, 
        t1.tip_sum 
    from
        t1_deliveries_part t1
            inner join dds.orders o on t1.order_id = o.order_id 
            inner join dds.couriers c on t1.courier_id = c.courier_id
    on conflict (delivery_id) do update 
        set 
            delivery_id = excluded.delivery_id,
            courier_id = excluded.courier_id,
            order_id = excluded.order_id,
            address = excluded.address,
            delivery_ts = excluded.delivery_ts,
            rate = excluded.rate,
            tip_sum = excluded.tip_sum;
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

# init STG in local DB
# create tables and layer if they are not exist
create_all_tables_DDS = PostgresOperator(
    task_id="create_DDS_tables",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="./create_tables_DDS.sql",
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
        '2_DDS_postgres',                  # name
        default_args=default_args,         # connect args
        schedule_interval='0/15 * * * *',  # interval
        start_date=datetime(2021, 1, 1),   # start calc
        catchup=False,                     # used in  the first launch, from date in the past until now. Usually = off
        tags=['sprint5', 'example'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    with TaskGroup("load_dds_tables") as load_tables:
        t21 = PythonOperator(task_id="restaurants", python_callable=load_paste_data_restaurants, dag=dag)
        t22 = PythonOperator(task_id="couriers", python_callable=load_paste_data_couriers, dag=dag)
        t23 = PythonOperator(task_id="timestamps", python_callable=load_paste_data_timestamps, dag=dag)
        t24 = PythonOperator(task_id="orders", python_callable=load_paste_data_orders, dag=dag)
        t25 = PythonOperator(task_id="deliveries", python_callable=load_paste_data_deliveries, dag=dag)
    t4 = DummyOperator(task_id="end")
    
    t1 >> create_all_tables_DDS >> load_tables >> t4


