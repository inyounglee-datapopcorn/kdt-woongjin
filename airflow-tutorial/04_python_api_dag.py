import pendulum
import pandas as pd
from pprint import pprint
from airflow.sdk import DAG, task
import requests


default_args = dict(
    owner = 'bda',
    email = ['bda@airflow.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="04_python_api_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = ['20250824','BASIC'],
    default_args = default_args,
    catchup=False
):

    @task(task_id='get_brewery_api')
    def get_brewery_api():
        URL = 'https://fakerapi.it/api/v2/users'
        response = requests.get(URL)
        
        res = response.json()['data']

        return res

    @task(task_id='api_to_dataframe')
    def api_to_dataframe(api_data):
        df = pd.json_normalize(api_data)
        
        for row in df.head().to_dict(orient='records'):
            pprint(row)
        print("df.shape : ", df.shape)
    
    api_data = get_brewery_api()
    api_to_dataframe(api_data)