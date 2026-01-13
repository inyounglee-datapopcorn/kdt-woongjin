import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 

# Configuration
SEOUL_API_KEY = "716a654d46716f7835365268626a78"

default_args = dict(
    owner = 'qoxjf135',
    email = ['qoxjf135@gmail.com'],
    email_on_failure = False,
    retries = 1
)

with DAG(
    dag_id="qoxjf135_seoul_subway_stats",
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'), # 데이터가 있는 과거 시점 설정
    schedule="0 10 * * *",  # 매일 오전 10시 실행 (전일 데이터 집계)
    catchup=False,
    default_args=default_args,
    tags=['subway', 'stats', 'daily'],
) as dag:

    # 1. 테이블 생성
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='qoxjf135_supabase_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS daily_subway_stats (
                id SERIAL PRIMARY KEY,
                use_dt VARCHAR(8),
                line_num VARCHAR(50),
                sub_sta_nm VARCHAR(50),
                ride_pasgr_num INT,
                alight_pasgr_num INT,
                work_dt VARCHAR(14),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. 일별 승하차 인원 수집 (최근 데이터)
    @task(task_id='fetch_daily_stats')
    def fetch_daily_stats():
        hook = PostgresHook(postgres_conn_id='qoxjf135_supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        
        # 일반적으로 3일 전 데이터가 최신일 수 있음. 안전하게 2024년 12월 1일로 하드코딩 테스트하거나, 동적 날짜 사용.
        # 여기서는 execution_date (논리적 날짜) 기준 전날 데이터를 시도.
        # 하지만 공공데이터 갱신 주기를 고려해 특정 과거 날짜(예: 20241201)를 우선 샘플로 수집하거나,
        # 최근 1주일치를 Loop 도는 방식이 안전함.
        
        # 심플하게: 실행일 기준 3일 전 데이터 호출
        target_date = pendulum.now('Asia/Seoul').subtract(days=3).format('YYYYMMDD')
        
        url = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/CardSubwayStatsNew/1/1000/{target_date}"
        logging.info(f"Fetching statistics for {target_date}")
        
        response = requests.get(url)
        data = response.json()
        
        if 'CardSubwayStatsNew' in data:
            items = data['CardSubwayStatsNew']['row']
            rows = []
            for item in items:
                rows.append({
                    "use_dt": item.get("USE_DT"),
                    "line_num": item.get("LINE_NUM"),
                    "sub_sta_nm": item.get("SUB_STA_NM"),
                    "ride_pasgr_num": int(item.get("RIDE_PASGR_NUM")),
                    "alight_pasgr_num": int(item.get("ALIGHT_PASGR_NUM")),
                    "work_dt": item.get("WORK_DT"),
                })
            
            if rows:
                import pandas as pd
                df = pd.DataFrame(rows)
                df.to_sql('daily_subway_stats', con=conn, if_exists='append', index=False, method='multi')
                logging.info(f"Inserted {len(df)} stats records for {target_date}.")
        else:
            logging.warning(f"No statistics data found for {target_date}. Response: {data}")

    create_table >> fetch_daily_stats()
