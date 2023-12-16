# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.vertica.operators.vertica import VerticaOperator
# from airflow.utils.task_group import TaskGroup

# ### Vertica settings ###
# VERTICA_CONN_ID = 'vertica_conn'

# default_args = {
#     'owner': 'Airflow',
#     'schedule_interval':'@once',           # sheduled or not
#     'retries': 1,                          # the number of retries that should be performed before failing the task
#     'retry_delay': timedelta(minutes=5),   # delay between retries
#     'depends_on_past': False,
#     'catchup':False
# }

# with DAG(
#         '0_STG_vertica_create',            # name
#         default_args=default_args,         # connect args
#         schedule_interval='0/15 * * * *',  # interval
#         start_date=datetime(2021, 1, 1),   # start calc
#         catchup=False,                     # used in  the first launch, from date in the past until now. Usually = off
#         tags=['final', 'project'],
# ) as dag:

#     # create DAG logic (sequence/order)
#     start = DummyOperator(task_id="start")
#     with TaskGroup("0_dag__test") as create_stg:
#         t21 = VerticaOperator(task_id="src/sql/ddl_stg.sql", vertica_conn_id=VERTICA_CONN_ID)
#     end = DummyOperator(task_id="end")
    
#     start >> create_stg >> end


