import logging
import os
import shutil
import pendulum
import pandas as pd
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook 

default_args = dict(
    owner = 'datapopcorn',
    email = ['datapopcorn@gmail.com'],
    email_on_failure = False,
    retries = 0
)

# 데이터 파일이 위치할 경로 (dags/data)
DATA_DIR = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags/data')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')

with DAG(
    dag_id="popcorn_17_static_file_loader",
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    schedule='0 9 * * *', # 수동 실행 (Trigger Only)
    catchup=False,
    default_args=default_args,
    tags=['utility', 'file_upload', 'supabase'],
    description="dags/data 폴더에 있는 CSV/Excel 파일을 Supabase로 업로드합니다."
) as dag:

    @task(task_id='upload_files_to_supabase')
    def upload_files():
        hook = PostgresHook(postgres_conn_id='supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        
        # 처리할 파일 목록 조회
        files = [f for f in os.listdir(DATA_DIR) if f.endswith(('.csv', '.xlsx', '.xls'))]
        
        if not files:
            logging.info(f"No files found in {DATA_DIR}")
            return

        logging.info(f"Found {len(files)} files: {files}")

        for file_name in files:
            file_path = os.path.join(DATA_DIR, file_name)
            table_name = os.path.splitext(file_name)[0] # 파일명을 테이블명으로 사용
            
            try:
                logging.info(f"Reading {file_name}...")
                
                # 파일 확장자에 따른 읽기
                if file_name.endswith('.csv'):
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)
                
                # 컬럼명 정리 (특수문자 제거, 공백 -> 언더바)
                df.columns = df.columns.astype(str).str.strip().str.replace(r'[^\w]', '_', regex=True)
                
                logging.info(f"Uploading to table '{table_name}'...")
                
                # DB 적재 (if_exists='replace'로 설정하여 테이블 새로 생성, 필요 시 'append'로 변경)
                df.to_sql(table_name, con=conn, if_exists='replace', index=False, method='multi')
                
                logging.info(f"Successfully uploaded {table_name}.")
                
                # 완료된 파일 이동
                if not os.path.exists(PROCESSED_DIR):
                    os.makedirs(PROCESSED_DIR)
                shutil.move(file_path, os.path.join(PROCESSED_DIR, file_name))
                logging.info(f"Moved {file_name} to processed folder.")
                
            except Exception as e:
                logging.error(f"Failed to process {file_name}: {e}")
                # 실패해도 다음 파일 계속 진행

    upload_files()
