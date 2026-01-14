from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# youtube_script.py가 있는 경로를 추가 (필요시)
# script_dir = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(script_dir)
sys.path.append(os.path.dirname(__file__))

# Try to import, but catch error if it's run in an environment where youtube_script isn't available
try:
    from youtube_script import run_my_crawler
except ImportError:
    def run_my_crawler():
        print("youtube_script not found")

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
        python_callable=run_my_crawler
    )
