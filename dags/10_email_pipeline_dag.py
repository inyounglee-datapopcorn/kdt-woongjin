import pendulum, requests
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.smtp.operators.smtp import EmailOperator
"""
등록 유저 명단을 이메일로 전송해주는 파이프라인
"""

default_args = dict(
    owner = 'popcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="10_email_pipeline_dag",
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
    
    @task(task_id='extract_registered_user')
    def extract_registered_user(api_data):
        
        df = pd.json_normalize(api_data)
        df['fullname'] = df['firstname'] + ' ' + df['lastname']
        # 금일 등록자 수
        count = len(df)

        # 이름 리스트
        names = df['fullname'].tolist()

        # HTML 포맷의 이메일 본문 생성
        html_body = f"""
        <html>
        <body>
            <h2>금일 등록자 수: {count}</h2>
            <h3>등록자 명단</h3>
            <ul>
            {''.join(f'<li>{name}</li>' for name in names)}
            </ul>
        </body>
        </html>
        """
        return html_body
    
    send_email = EmailOperator(
        task_id='send_email',
        conn_id='gmail_connection',
        to='hyunsooyein@gmail.com',
        subject='User Registered - {{ ds_nodash }}',
        html_content="{{ ti.xcom_pull(task_ids='extract_registered_user') }}",
    )
    
    api_data = get_api_data()
    html_body = extract_registered_user(api_data)
    
    html_body >> send_email