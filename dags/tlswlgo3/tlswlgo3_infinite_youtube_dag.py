import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# script 파일 경로 추가
sys.path.append(os.path.dirname(__file__))

# Import run_my_crawler from tlswlgo3_infinite_youtube_script.py
from tlswlgo3_infinite_youtube_script import run_my_crawler

default_args = {
    'owner': 'inyoung',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='infinite_youtube_dag',
    default_args=default_args,
    description='유튜브 데이터를 30분마다 자동 수집',
    schedule='*/30 * * * *', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    collect_task = PythonOperator(
        task_id='collect_youtube_data',
        python_callable=run_my_crawler,
        op_kwargs={
            'api_key': '{{ var.value.tlswlgo3_youtube_apikey }}'
        }
    )
