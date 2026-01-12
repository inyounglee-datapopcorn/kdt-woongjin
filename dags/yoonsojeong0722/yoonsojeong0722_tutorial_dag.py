import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

default_args = dict(
    owner = 'yoonsojeong0722', # 개별 DAG 관리자
    email = ['yoon.sojeong0722@gmail.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="yoonsojeong0722_01_tutorial_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    default_args = default_args,
    catchup=False
):
    
    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3")
    task4 = EmptyOperator(task_id="task4")
    task5 = EmptyOperator(task_id="task5")

# task1 >> task2 >> task3 >> task4 >> task5
task1 >> [task2, task3] >> task4 >> task5
 
 