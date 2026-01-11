import pendulum
from pprint import pprint
from airflow.sdk import DAG, task, get_current_context

"""
🔸 Airflow의 컨텍스트(Context)
    - 태스크가 실행될 때 DAG, Task, 실행 날짜, 매크로, 파라미터 등 실행 환경 관련 메타데이터를 담고 있는 딕셔너리
    - 시간 관련 데이터도 포함하고 있기 때문에 실행별로 값이 달라짐
    - 'ti' 객체를 활용하여 XCOM 저장소 데이터를 저장/조회 가능
"""

default_args = dict(
    owner = 'bda',
    email = ['bda@airflow.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="05_context_check_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = ['20250824','BASIC'],
    default_args = default_args,
    catchup=False
):

    @task(task_id='context_check')
    def context_check():
        ctx = get_current_context()
        pprint(ctx)
    
    context_check()