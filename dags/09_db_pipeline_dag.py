import pendulum
import requests
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook 
"""
API 호출을 통해 데이터를 가져와 Supabase(Postgres) DB에 저장하는 파이프라인

API → Pandas DataFrame → Supabase Table
"""

default_args = dict(
    owner = 'popcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="09_db_pipeline_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = ['20250824','ADVANCED'],
    default_args = default_args,
    catchup=False
):

    @task(task_id='get_api_data')
    def get_api_data():
        URL = 'https://fakerapi.it/api/v2/users'
        response = requests.get(URL)
        res = response.json()['data']
        return res

    @task(task_id='api_to_dataframe')
    def api_to_dataframe(api_data):
        
        # 실행 시점의 날짜값을 추출
        ctx = get_current_context()
        batch_date = ctx['ds_nodash']
        
        df = pd.json_normalize(api_data)
        # uuid 컬럼이 있다면 제거 (Supabase 자동생성 등 고려)
        # df.drop('id', axis=1, inplace=True) 
        df['created_at'] = batch_date

        return df
    
    @task(task_id='dataframe_to_postgres')
    def dataframe_to_postgres(df):
        # Supabase(Postgres) 연결 사용
        hook = PostgresHook(postgres_conn_id='supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        df.to_sql(
            'users', 
            con=conn,
            if_exists='append', 
            index=False
            )
    
    api_data = get_api_data()
    df = api_to_dataframe(api_data)
    dataframe_to_postgres(df)


