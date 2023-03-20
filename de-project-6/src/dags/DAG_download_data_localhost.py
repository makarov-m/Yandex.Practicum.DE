import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import boto3
from airflow.models import Variable

# AIRFLOW VARIABLES
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'                                                     # path to container
        #Filename=os.path(f'/Users/max/Documents/practicum/sprint-6/s6-lessons/data/{key}')    # local
    )

# print first 10 rows from each file
bash_command_tmpl = """
echo {{ params.files }} ;
head -10 {{ params.files }}
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def download_file_to_localhost():
    bucket_files = ['group_log.csv']

    task1 = PythonOperator(
        task_id=f'fetch_groups.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': bucket_files[0]},
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': ' '.join([f'/data/{f}' for f in bucket_files])}
    )

    task1>>print_10_lines_of_each

dag = download_file_to_localhost()