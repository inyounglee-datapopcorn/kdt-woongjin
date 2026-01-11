import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
import pandas as pd
from pprint import pprint
from airflow.providers.http.operators.http import HttpOperator
"""
🔸 HttpOperator
    - 지정된 HTTP 엔드포인트로 요청을 보내고 그 응답을 가져오는 작업을 수행하는 오퍼레이터
    - Airflow UI에서 'HTTP' connection 설정 필요!

🔸 Connection 설정 정보
    - Conn Id : http_connection
    - Host : https://fakerapi.it/api/v2
    
https://airflow.apache.org/docs/apache-airflow-providers-http/stable/_api/airflow/providers/http/operators/http/index.html
"""

default_args = dict(
    owner = 'popcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="06_http_operator_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = ['20250824','INTERMEDIATE'],
    default_args = default_args,
    catchup=False
):
    get_api_data = HttpOperator(
        task_id='get_api_data',
        http_conn_id='http_connection',
        method='GET',
        endpoint='users',
        response_filter=lambda response: response.json()['data']
    )
    
    @task(task_id='api_to_dataframe')
    def api_to_dataframe():
        
        # XCOM 저장소에 저장된 API 데이터를 가져오는 코드
        context = get_current_context()
        ti = context['ti']
        api_data = ti.xcom_pull(task_ids='get_api_data')
        
        # 가져온 데이터를 DataFrame으로 변환
        df = pd.json_normalize(api_data)
        
        for row in df.head().to_dict(orient='records'):
            pprint(row)
        print("df.shape : ", df.shape)
        
get_api_data >> api_to_dataframe()