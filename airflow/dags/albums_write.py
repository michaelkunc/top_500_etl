from etl_classes import albums
import boto3
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 14),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2017, 5, 20),
}


def write_local():
    a = albums.Albums()
    a.df.to_csv('airflow/dags/csvs/album_frame.csv')
    a.genres_by_years().to_csv('airflow/dags/csvs/genre.csv')
    return "The album & genre frame have been written locally"


def write_s3():
    s3 = boto3.client('s3')
    files = ['airflow/dags/csvs/album_frame.csv',
             'airflow/dags/csvs/genre.csv']
    bucket = os.environ['BUCKET']
    for f in files:
        s3.upload_file(f, bucket, f)
    return "The album & genre frame have been written to s3"


dag = DAG('albums', default_args=default_args, schedule_interval='@once')

t1 = PythonOperator(
    task_id='write_local',
    python_callable=write_local,
    dag=dag)

t2 = PythonOperator(
    task_id='write_s3',
    python_callable=write_s3,
    dag=dag)

t2.set_upstream(t1)
