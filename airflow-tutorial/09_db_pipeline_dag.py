import pendulum
import requests
import pandas as pd
from airflow.sdk import DAG, task, get_current_context
from airflow.providers.mysql.hooks.mysql import MySqlHook 
"""
API 호출을 통해 데이터를 가져와 MYSQL DB에 저장하는 파이프라인

API → Pandas DataFrame → MYSQL Table
"""

default_args = dict(
    owner = 'bda',
    email = ['bda@airflow.com'],
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
        df.drop('id', axis=1, inplace=True)
        df['created_at'] = batch_date

        return df
    
    @task(task_id='dataframe_to_mysql')
    def dataframe_to_mysql(df):
        hook = MySqlHook(mysql_conn_id='mysql_connection')
        conn = hook.get_sqlalchemy_engine()
        df.to_sql(
            'users', 
            con=conn,
            if_exists='append', 
            index=False
            )
    
    api_data = get_api_data()
    df = api_to_dataframe(api_data)
    dataframe_to_mysql(df)


