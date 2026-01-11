from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import datetime

def print_hello():
    print("=" * 60)
    print("축하합니다! GitHub에서 자동으로 배포된 DAG가 정상 작동 중입니다.")
    print("Hello! This DAG was deployed automatically via GitHub Actions!")
    print("=" * 60)

default_args = {
    'owner': 'datapopcorn',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='11_deployment_test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test', 'deployment'],
) as dag:

    hello_task = PythonOperator(
        task_id='print_hello_message',
        python_callable=print_hello,
    )
