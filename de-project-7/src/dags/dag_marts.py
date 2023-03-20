import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id = "datalake_marts_creation",
    default_args=default_args,
    schedule_interval=None,
)

mart_users = SparkSubmitOperator(
    task_id='mart_users',
    dag=dag_spark,
    application ='/lessons/mart_users.py' ,
    conn_id= 'yarn_spark',
    application_args = [ 
        "/user/mmakarov/analytics/proj7_repartition/", 
        "/user/mmakarov/analytics/proj7/cities/geo.csv", 
        "/user/mmakarov/prod/user_mart/"
        ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

mart_zones = SparkSubmitOperator(
    task_id='mart_zones',
    dag=dag_spark,
    application ='/lessons/mart_zones.py' ,
    conn_id= 'yarn_spark',
    application_args = [
        "/user/mmakarov/analytics/proj7_repartition/", 
        "/user/mmakarov/analytics/proj7/cities/geo.csv",
        "/user/mmakarov/prod/zones_mart/"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

mart_geo_activity = SparkSubmitOperator(
    task_id='mart_geo_activity',
    dag=dag_spark,
    application ='/lessons/mart_geo_activity.py' ,
    conn_id= 'yarn_spark',
    application_args = [
        "/user/mmakarov/analytics/proj7_repartition/", 
        "/user/mmakarov/analytics/proj7/cities/geo.csv", 
        "/user/mmakarov/prod/geo_activity_mart/"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)



